"""원본 그래프 바깥의 실행 한도·재시도·구조화 로그. 진단 판단에는 관여하지 않는다."""
import asyncio
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
import json
import logging
import uuid
import hashlib
import inspect
import math
from pathlib import Path
from langgraph.errors import GraphRecursionError
from .observability import context, log_event
from time import monotonic

import httpx
import httpx2
from openai import APIConnectionError

from . import config as C

log = logging.getLogger("worker.harness")
current = ContextVar("execution_budget", default=None)


LIMIT_CODES = frozenset(("EXECUTION_LIMIT", "TOOL_CALL_LIMIT", "TOOL_REPEAT_LIMIT",
                         "NODE_EXECUTION_LIMIT", "RUN_TIMEOUT"))


class LimitExceeded(RuntimeError):
    def __init__(self, message="Execution limit exceeded", code="EXECUTION_LIMIT"):
        self.code = code
        super().__init__(message)


class ToolCallTimeout(TimeoutError):
    def __init__(self, tool):
        self.tool = tool
        super().__init__("Tool call timed out")


def validate_limits():
    for value in (C.MAX_TOOL_CALLS, C.RECURSION_LIMIT, C.MAX_NODE_EXECUTIONS, C.MAX_SAME_INPUT_ATTEMPTS):
        if type(value) is not int or value < 1:
            raise ValueError("Execution count limits must be positive integers")
    for value in (C.RUN_TIMEOUT, C.TOOL_TIMEOUT):
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Execution time limits must be finite and positive")


@dataclass
class Budget:
    event_id: str
    call_limit: int
    context: dict = field(default_factory=dict)
    prompt_bundle: dict | None = None
    prompt_versions: dict = field(default_factory=dict)
    calls: int = 0
    retries: int = 0
    nodes: int = 0
    attempts_by_input: dict = field(default_factory=dict, repr=False)
    attempts_by_tool: dict = field(default_factory=dict)
    node_counts: dict = field(default_factory=dict)
    limits_hit: list = field(default_factory=list)
    run_timeout: float = field(default_factory=lambda: C.RUN_TIMEOUT)
    node_limit: int = field(default_factory=lambda: C.MAX_NODE_EXECUTIONS)
    repeat_limit: int = field(default_factory=lambda: C.MAX_SAME_INPUT_ATTEMPTS)
    started: float = field(default_factory=monotonic)

    def stop(self, code, **details):
        hit = {"code": code, **details}
        self.limits_hit.append(hit)
        record("run.limit_exceeded", **hit)
        raise LimitExceeded(code, code=code)

    def check_time(self):
        if monotonic() - self.started >= self.run_timeout:
            self.stop("RUN_TIMEOUT", limit=self.run_timeout)

    def consume(self, count, tool=None, fingerprint=None):
        self.check_time()
        if self.calls + count > self.call_limit:
            self.stop("TOOL_CALL_LIMIT", used=self.calls, requested=count, limit=self.call_limit)
        if fingerprint is not None:
            used = self.attempts_by_input.get(fingerprint, 0)
            if used >= self.repeat_limit:
                self.stop("TOOL_REPEAT_LIMIT", tool=tool, used=used, limit=self.repeat_limit)
            self.attempts_by_input[fingerprint] = used + 1
            self.attempts_by_tool[tool] = self.attempts_by_tool.get(tool, 0) + 1
        self.calls += count

    def enter_node(self, name):
        self.check_time()
        if self.nodes >= self.node_limit:
            self.stop("NODE_EXECUTION_LIMIT", node=name, used=self.nodes, limit=self.node_limit)
        self.nodes += 1
        self.node_counts[name] = self.node_counts.get(name, 0) + 1


def call_identity(fn, args, kwargs, name_arg):
    bound = inspect.signature(fn).bind(*args, **kwargs)
    bound.apply_defaults()
    name = bound.arguments[name_arg] if name_arg else fn.__name__
    # 입력 원문·주소를 로그에 남기지 않는다. 해시는 실행 중 메모리의 반복 판별에만 사용한다.
    def encode_path(value):
        if isinstance(value, Path):
            return str(value)
        raise TypeError("Unsupported tool argument")
    values = json.dumps(bound.arguments, sort_keys=True, ensure_ascii=False, allow_nan=False,
                        default=encode_path)
    identity = f"{fn.__module__}.{getattr(fn, '__qualname__', fn.__name__)}:{name}:{values}"
    return name, hashlib.sha256(identity.encode()).hexdigest()

def record(event, **fields):
    budget = current.get()
    log_event(event, **(budget.context if budget else {}), **fields)


def transient(exc, seen=frozenset()):
    if id(exc) in seen:
        return False
    seen = seen | {id(exc)}
    if isinstance(exc, BaseExceptionGroup):
        return bool(exc.exceptions) and all(transient(e, seen) for e in exc.exceptions)
    status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
    if status in (429, 503):
        return True
    if isinstance(exc, (TimeoutError, ConnectionError, httpx.TransportError,
                        httpx2.TransportError, APIConnectionError)):
        return True
    # FastMCP는 접속 오류를 RuntimeError로 감싼다. 명시적 원인이 일시 오류일 때만 재시도한다.
    return (isinstance(exc, RuntimeError) and exc.__cause__ is not None
            and transient(exc.__cause__, seen))


def guarded(weight=1, name_arg=None):
    def decorate(fn):
        @wraps(fn)
        async def wrapped(*args, **kwargs):
            call_id = str(uuid.uuid4())
            budget = current.get()
            tool, fingerprint = call_identity(fn, args, kwargs, name_arg) if budget else (fn.__name__, None)
            for attempt in range(C.MAX_RETRY + 1):
                if budget:
                    budget.consume(weight, tool, fingerprint)
                    if attempt:
                        budget.retries += 1
                started = monotonic()
                record("tool.started", tool=tool, call_id=call_id, attempt=attempt + 1)
                try:
                    async with asyncio.timeout(C.TOOL_TIMEOUT):
                        value = await fn(*args, **kwargs)
                    record("tool.succeeded", tool=tool, call_id=call_id, duration_ms=round((monotonic()-started)*1000))
                    return value
                except Exception as exc:
                    record("tool.failed", tool=tool, call_id=call_id, error_type=type(exc).__name__, attempt=attempt + 1)
                    if isinstance(exc, LimitExceeded) or attempt == C.MAX_RETRY or not transient(exc):
                        if isinstance(exc, TimeoutError):
                            raise ToolCallTimeout(tool) from exc
                        raise
                    record("tool.retry", tool=tool, call_id=call_id, next_attempt=attempt+2)
                    await asyncio.sleep(min(2 ** attempt, 2))
        return wrapped
    return decorate


def observed(fn):
    @wraps(fn)
    async def node(state, config):
        budget = current.get()
        if budget:
            budget.enter_node(fn.__name__)
        record("node.started", node=fn.__name__)
        started = monotonic()
        try:
            return await fn(state, config)
        except Exception as exc:
            record("node.failed", node=fn.__name__, error_type=type(exc).__name__)
            raise
        finally:
            record("node.finished", node=fn.__name__, duration_ms=round((monotonic()-started)*1000))
    return node


async def invoke(graph, command, emit, metrics=None):
    validate_limits()
    round_no = command["payload"]["round"]
    if type(round_no) is not int or not 0 <= round_no <= C.MAX_PHOTO_ROUNDS:
        raise ValueError("Additional input round must be between 0 and 3")
    # 사진 장수는 재요청 횟수와 독립. 사진별 분류·이미지 분석·해석 호출을 계산한다.
    photos = len(command["payload"].get("attachments", []))
    budget = Budget(command["eventId"], min(C.MAX_TOOL_CALLS, max(30, (photos * 5 + 10) * 3)))
    budget.context = context(command)
    token = current.set(budget)
    deadline = None
    try:
        from .prompts import load_bundle
        budget.prompt_bundle = load_bundle()
        async with asyncio.timeout(budget.run_timeout) as deadline:
            return await graph.ainvoke({"command": command},
                                      {"recursion_limit": C.RECURSION_LIMIT,
                                       "configurable": {"emit": emit}})
    except TimeoutError:
        # 전체 deadline과 개별 도구 timeout은 구분한다.
        if deadline is not None and deadline.expired():
            budget.stop("RUN_TIMEOUT", limit=budget.run_timeout)
        raise
    except GraphRecursionError:
        hit = {"code": "GRAPH_STEP_LIMIT", "limit": C.RECURSION_LIMIT}
        budget.limits_hit.append(hit)
        record("run.limit_exceeded", **hit)
        raise
    finally:
        values = {"calls": budget.calls, "retries": budget.retries,
                  "duration_ms": round((monotonic()-budget.started)*1000), "call_limit": budget.call_limit,
                  "prompts": budget.prompt_versions, "node_executions": budget.nodes,
                  "nodes": budget.node_counts, "tool_attempts": budget.attempts_by_tool,
                  "max_same_input_attempts_used": max(budget.attempts_by_input.values(), default=0),
                  "limits_hit": budget.limits_hit,
                  "limits": {"same_input_attempts": budget.repeat_limit, "node_executions": budget.node_limit,
                             "graph_super_steps": C.RECURSION_LIMIT, "run_seconds": budget.run_timeout,
                             "tool_seconds": C.TOOL_TIMEOUT, "weighted_calls": budget.call_limit}}
        if metrics is not None:
            metrics.update(values)
        record("run.finished", **values)
        current.reset(token)
