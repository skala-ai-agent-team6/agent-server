"""원본 그래프 바깥의 실행 한도·재시도·구조화 로그. 진단 판단에는 관여하지 않는다."""
import asyncio
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
import json
import logging
import uuid
from .observability import context, log_event
from time import monotonic

import httpx
from openai import APIConnectionError

from . import config as C

log = logging.getLogger("worker.harness")
current = ContextVar("execution_budget", default=None)


class LimitExceeded(RuntimeError):
    pass


@dataclass
class Budget:
    event_id: str
    call_limit: int
    context: dict = field(default_factory=dict)
    calls: int = 0
    retries: int = 0
    started: float = field(default_factory=monotonic)

    def consume(self, count):
        if self.calls + count > self.call_limit:
            raise LimitExceeded("Tool call budget exhausted")
        if monotonic() - self.started >= C.RUN_TIMEOUT:
            raise LimitExceeded("Execution deadline exceeded")
        self.calls += count


def record(event, **fields):
    budget = current.get()
    log_event(event, **(budget.context if budget else {}), **fields)


def transient(exc):
    if isinstance(exc, BaseExceptionGroup):
        return bool(exc.exceptions) and all(transient(e) for e in exc.exceptions)
    status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
    if status in (429, 503):
        return True
    return isinstance(exc, (TimeoutError, ConnectionError, httpx.TransportError, APIConnectionError))


def guarded(weight=1):
    def decorate(fn):
        @wraps(fn)
        async def wrapped(*args, **kwargs):
            call_id = str(uuid.uuid4())
            for attempt in range(C.MAX_RETRY + 1):
                budget = current.get()
                if budget:
                    budget.consume(weight)
                started = monotonic()
                record("tool.started", tool=fn.__name__, call_id=call_id, attempt=attempt + 1)
                try:
                    async with asyncio.timeout(C.TOOL_TIMEOUT):
                        value = await fn(*args, **kwargs)
                    record("tool.succeeded", tool=fn.__name__, call_id=call_id, duration_ms=round((monotonic()-started)*1000))
                    return value
                except Exception as exc:
                    record("tool.failed", tool=fn.__name__, call_id=call_id, error_type=type(exc).__name__, attempt=attempt + 1)
                    if attempt == C.MAX_RETRY or not transient(exc):
                        raise
                    if budget:
                        budget.retries += 1
                    record("tool.retry", tool=fn.__name__, call_id=call_id, next_attempt=attempt+2)
                    await asyncio.sleep(min(2 ** attempt, 2))
        return wrapped
    return decorate


def observed(fn):
    @wraps(fn)
    async def node(state, config):
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
    round_no = command["payload"]["round"]
    if type(round_no) is not int or not 0 <= round_no <= C.MAX_PHOTO_ROUNDS:
        raise ValueError("Additional input round must be between 0 and 3")
    # 사진 장수는 재요청 횟수와 독립. 사진별 분류·이미지 분석·해석 호출을 계산한다.
    photos = len(command["payload"].get("attachments", []))
    budget = Budget(command["eventId"], min(C.MAX_TOOL_CALLS, max(30, (photos * 5 + 10) * 3)))
    budget.context = context(command)
    token = current.set(budget)
    try:
        async with asyncio.timeout(C.RUN_TIMEOUT):
            return await graph.ainvoke({"command": command},
                                      {"recursion_limit": C.RECURSION_LIMIT,
                                       "configurable": {"emit": emit}})
    finally:
        values = {"calls": budget.calls, "retries": budget.retries,
                  "duration_ms": round((monotonic()-budget.started)*1000), "call_limit": budget.call_limit}
        if metrics is not None:
            metrics.update(values)
        record("run.finished", **values)
        current.reset(token)
