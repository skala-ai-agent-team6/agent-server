"""이벤트 처리와 로컬 실행 상태 저장. Spring 업무 DB·Kafka 발행기와 분리한다."""
import hashlib
import json
import sqlite3
from pathlib import Path
from threading import RLock
from app.models import DiagnosisEvent


class EventConflict(Exception):
    pass


class EventProcessor:
    def __init__(self, graph, store_path=":memory:"):
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
        data = message.model_dump()
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
                    return json.loads(seen[1])
                row = self.db.execute("SELECT state FROM tasks WHERE task_id=?", (message.task_id,)).fetchone()
                previous = json.loads(row[0]) if row else None
                self._validate(message, previous)
                initial = {"request": data, "status": "RUNNING", "photo_request_count": 0,
                           "tool_call_count": 0, "retry_count": 0, "executions": [],
                           "error_code": None, "outgoing_event": None, "report": None}
                if previous:
                    for key in ("photo_request_count", "tool_call_count", "retry_count", "executions"):
                        initial[key] = previous[key]
                result = dict(self.graph.invoke(initial, {"recursion_limit": 20}))
                self.db.execute("INSERT INTO tasks VALUES(?,?,?) ON CONFLICT(task_id) DO UPDATE SET state=excluded.state",
                                (message.task_id, message.case_id, json.dumps(result, ensure_ascii=False)))
                response = {"mode": "mock", "task_id": message.task_id, **result}
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
                return response
            except Exception:
                self.db.rollback()
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
            return {"mode": "mock", "task_id": task_id, **json.loads(row[0])}

    def outgoing(self):
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute("SELECT body FROM outgoing_events ORDER BY rowid")]

    def backend_outgoing(self):
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute("SELECT body FROM backend_events ORDER BY rowid")]

    def close(self):
        self.db.close()
