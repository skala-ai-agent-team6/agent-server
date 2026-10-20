"""이벤트 처리와 로컬 실행 상태 저장. Spring 업무 DB·Kafka 발행기와 분리한다."""
import hashlib
import json
import sqlite3
from pathlib import Path
from threading import RLock
from app.models import DiagnosisEvent
from app.observability import context, log_event
from time import monotonic
from langgraph.errors import GraphRecursionError
from app.limits import DEFAULT_GRAPH_STEP_LIMIT, MAX_ADDITIONAL_REQUESTS
from app.workflow import failure_update


class EventConflict(Exception):
    pass


class EventProcessor:
    def __init__(self, graph, store_path=":memory:", graph_step_limit=DEFAULT_GRAPH_STEP_LIMIT, mode="mock"):
        if type(graph_step_limit) is not int or graph_step_limit < 1:
            raise ValueError("graph_step_limit must be a positive integer")
        self.graph_step_limit = graph_step_limit
        self.mode = mode
        self.graph = graph
        if store_path != ":memory:":
            Path(store_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(store_path, check_same_thread=False)
        self.lock = RLock()
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, case_id TEXT NOT NULL, state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY, digest TEXT NOT NULL, result TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS backend_events (
                event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outgoing_events (
                event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, body TEXT NOT NULL
            );
        """)

    def process(self, message: DiagnosisEvent, prepare_result=None) -> dict:
        started = monotonic()
        data = message.model_dump()
        data["_mode"] = self.mode
        log_event("event.received", **context(data), event_type=message.event_type,
                  photo_count=len(message.attachments), transport="backend-v2-local" if message.source_context else "local")
        canonical = json.dumps(data, sort_keys=True, ensure_ascii=False)
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        with self.lock:
            # 로컬에서는 작업 저장·입력 중복 기록·출력 이벤트를 한 트랜잭션에 묶는다.
            # 네트워크 도구를 연결할 때는 DB 잠금 안에서 장시간 실행하지 않도록 claim/lease로 교체한다.
            self.db.execute("BEGIN IMMEDIATE")
            try:
                seen = self.db.execute("SELECT digest,result FROM processed_events WHERE event_id=?", (message.event_id,)).fetchone()
                if seen:
                    if seen[0] != digest:
                        raise EventConflict("event_id reused with different input")
                    self.db.commit()
                    log_event("event.duplicate", **context(data), reexecuted=False)
                    return json.loads(seen[1])
                row = self.db.execute("SELECT state FROM tasks WHERE task_id=?", (message.task_id,)).fetchone()
                previous = json.loads(row[0]) if row else None
                self._validate(message, previous)
                log_event("event.accepted", **context(data), existing_task=previous is not None,
                          previous_photo_request_count=previous["photo_request_count"] if previous else 0)
                initial = {"request": data, "status": "RUNNING", "photo_request_count": 0,
                           "tool_call_count": 0, "retry_count": 0, "executions": [],
                           "error_code": None, "outgoing_event": None, "report": None}
                if previous:
                    for key in ("photo_request_count", "tool_call_count", "retry_count", "executions"):
                        initial[key] = previous[key]
                result = dict(initial)
                try:
                    # 완료된 노드 갱신을 보관해 한도 초과 시 호출 횟수·결과를 잃지 않는다.
                    for updates in self.graph.stream(initial, {"recursion_limit": self.graph_step_limit},
                                                     stream_mode="updates"):
                        for update in updates.values():
                            result.update(update)
                except GraphRecursionError:
                    log_event("graph.limit_reached", **context(data), graph_step_limit=self.graph_step_limit,
                              photo_request_count=result["photo_request_count"],
                              tool_call_count=result["tool_call_count"])
                    result.update(failure_update(result, "GRAPH_STEP_LIMIT"))
                self.db.execute("INSERT INTO tasks VALUES(?,?,?) ON CONFLICT(task_id) DO UPDATE SET state=excluded.state",
                                (message.task_id, message.case_id, json.dumps(result, ensure_ascii=False)))
                response = {"mode": self.mode, "task_id": message.task_id, **result}
                if prepare_result is not None:
                    # 변환/계약 검증 실패도 상태·입력·출력 저장과 함께 rollback한다.
                    response["backend_events"] = prepare_result(result)
                    for outgoing in response["backend_events"]:
                        self.db.execute("INSERT INTO backend_events VALUES(?,?,?)", (
                            outgoing["eventId"], message.task_id, json.dumps(outgoing, ensure_ascii=False)))
                self.db.execute("INSERT INTO processed_events VALUES(?,?,?)",
                                (message.event_id, digest, json.dumps(response, ensure_ascii=False)))
                out = result["outgoing_event"]
                self.db.execute("INSERT INTO outgoing_events VALUES(?,?,?)",
                                (out["event_id"], message.task_id, json.dumps(out, ensure_ascii=False)))
                self.db.commit()
                log_event("event.committed", **context(data), status=result["status"],
                          outgoing_type=result["outgoing_event"]["event_type"],
                          photo_request_count=result["photo_request_count"],
                          tool_call_count=result["tool_call_count"], retry_count=result["retry_count"],
                          backend_event_count=len(response.get("backend_events", [])),
                          duration_ms=round((monotonic()-started)*1000, 2))
                return response
            except Exception as exc:
                self.db.rollback()
                log_event("event.rolled_back", **context(data), error_type=type(exc).__name__,
                          reason_code="CONTRACT_PENDING" if type(exc).__name__ == "ContractPending" else
                                      "EVENT_CONFLICT" if isinstance(exc, EventConflict) else "PROCESSING_ERROR",
                          contract_field=getattr(exc, "field", None),
                          duration_ms=round((monotonic()-started)*1000, 2))
                raise

    def _validate(self, msg, previous):
        if msg.event_type == "DIAGNOSIS_REQUESTED":
            if previous:
                raise EventConflict("Task already exists; duplicate delivery must keep event_id")
            for row in self.db.execute("SELECT state FROM tasks WHERE case_id=?", (msg.case_id,)):
                if json.loads(row[0])["status"] in ("RUNNING", "WAITING_INPUT"):
                    raise EventConflict("Case already has an active diagnosis")
            return
        if previous is None:
            raise EventConflict("Unknown task for additional input")
        if msg.case_id != previous["request"]["case_id"]:
            raise EventConflict("Task belongs to a different case")
        if previous["status"] != "WAITING_INPUT" or msg.round_no != previous["photo_request_count"]:
            raise EventConflict("Not waiting for this additional-input round")
        if not 0 <= previous["photo_request_count"] <= MAX_ADDITIONAL_REQUESTS:
            raise EventConflict("Stored additional-input count is out of range")
        old_context = previous["request"].get("source_context")
        context = msg.source_context
        if bool(old_context) != bool(context):
            raise EventConflict("Cannot mix local and backend v2 events for one task")
        if context and old_context:
            if context["inputRevision"] != old_context["inputRevision"] + 1:
                raise EventConflict("Unexpected inputRevision")
            if context["executionId"] == old_context["executionId"]:
                raise EventConflict("A new input revision needs a new executionId")
            if context["correlationId"] != old_context["correlationId"]:
                raise EventConflict("correlationId changed")
        old_photos = {a["photo_id"]: a for a in previous["request"]["attachments"]}
        new_photos = {a.photo_id: a.model_dump() for a in msg.attachments}
        if any(new_photos.get(key) != value for key, value in old_photos.items()):
            raise EventConflict("Additional event must preserve previous photo metadata")
        if old_photos == new_photos and msg.symptom == previous["request"]["symptom"]:
            raise EventConflict("Additional event must include new photos or updated text")

    def get(self, task_id):
        with self.lock:
            row = self.db.execute("SELECT state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(task_id)
            state = json.loads(row[0])
            return {"mode": state["request"].get("_mode", "mock"), "task_id": task_id, **state}

    def outgoing(self):
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute("SELECT body FROM outgoing_events ORDER BY rowid")]

    def backend_outgoing(self):
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute("SELECT body FROM backend_events ORDER BY rowid")]

    def close(self):
        self.db.close()
