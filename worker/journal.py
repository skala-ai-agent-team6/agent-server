"""단일 워커용 영속 inbox/outbox. 완료 결과가 있으면 모델 대신 저장된 이벤트를 재전송."""
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from . import config as C

VALIDATOR = Draft202012Validator(
    json.loads((C.ROOT / "contract/kafka-event.schema.json").read_text()),
    format_checker=FormatChecker())


class CommandConflict(ValueError):
    pass


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


class Journal:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, command TEXT NOT NULL, result TEXT);
        CREATE TABLE IF NOT EXISTS events(
          job TEXT NOT NULL, seq INTEGER NOT NULL, body TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(job,seq));
        """)

    def begin(self, command):
        VALIDATOR.validate(command)
        if command["eventType"] not in ("DIAGNOSIS_REQUESTED", "DIAGNOSIS_RESUMED"):
            raise CommandConflict("Unsupported command")
        round_no = command["payload"]["round"]
        if type(round_no) is not int or not 0 <= round_no <= C.MAX_PHOTO_ROUNDS:
            raise CommandConflict("Invalid additional-input round")
        payload = command["payload"]
        ids = [a["id"] for a in payload["attachments"]]
        added = payload["addedAttachmentIds"]
        if (len(set(ids)) != len(ids) or ids != payload["attachmentIds"]
                or len(set(added)) != len(added) or not set(added) <= set(ids)):
            raise CommandConflict("Attachment IDs do not match metadata")
        if (str(payload["repair"]["id"]) != command["repairId"]
                or str(payload["task"]["id"]) != command["taskId"]):
            raise CommandConflict("Nested task/repair IDs do not match")
        if command["inputRevision"] != round_no + 1:
            raise CommandConflict("Revision must match input round")
        if command["eventType"] == "DIAGNOSIS_REQUESTED":
            if round_no or payload.get("requestId") or payload.get("requestKey") or added:
                raise CommandConflict("Invalid initial request")
        elif not round_no or not payload.get("requestId") or not payload.get("requestKey"):
            raise CommandConflict("Resume requires additional-input request")
        raw = encode(command)
        row = self.db.execute("SELECT command,result FROM jobs WHERE id=?", (command["eventId"],)).fetchone()
        if row:
            if row[0] != raw:
                raise CommandConflict("Same event ID with different body")
            return json.loads(row[1]) if row[1] else None
        # 이미 처리한 작업의 이전 차수 재실행 및 위조 resume를 막는다.
        previous = self.db.execute("SELECT command,result FROM jobs ORDER BY rowid DESC").fetchall()
        latest = {}
        for saved_command, saved_result in previous:
            old = json.loads(saved_command)
            latest.setdefault(old["taskId"], (old, json.loads(saved_result) if saved_result else {}))
        for task, (old, result) in latest.items():
            if (task != command["taskId"] and old["repairId"] == command["repairId"]
                    and result.get("eventType") not in ("DIAGNOSIS_COMPLETED", "DIAGNOSIS_FAILED")):
                raise CommandConflict("Case already has an active diagnosis")
        for saved_command, saved_result in previous:
            old = json.loads(saved_command)
            if old["taskId"] != command["taskId"]:
                continue
            if (old["repairId"] != command["repairId"]
                    or command["inputRevision"] != old["inputRevision"] + 1):
                raise CommandConflict("Stale or conflicting task revision")
            result = json.loads(saved_result) if saved_result else {}
            expected = result.get("payload", {})
            if (command["eventType"] != "DIAGNOSIS_RESUMED"
                    or result.get("eventType") != "PHOTO_REQUESTED"
                    or any(command["payload"].get(k) != expected.get(k)
                           for k in ("round", "requestKey", "checkpoint"))):
                raise CommandConflict("Resume does not match stored photo request")
            if (command["executionId"] == old["executionId"]
                    or command["correlationId"] != old["correlationId"]):
                raise CommandConflict("Invalid execution or correlation ID")
            old_photos = {a["id"]: a for a in old["payload"]["attachments"]}
            new_photos = {a["id"]: a for a in payload["attachments"]}
            if any(new_photos.get(k) != v for k, v in old_photos.items()):
                raise CommandConflict("Resume changed previous attachment metadata")
            if set(added) != set(new_photos) - set(old_photos):
                raise CommandConflict("addedAttachmentIds does not match new photos")
            def text(p):
                r = p["repair"]
                return (r.get("symptom"), r.get("user_input", {}).get("customerDescription"))
            if old_photos == new_photos and text(payload) == text(old["payload"]):
                raise CommandConflict("Resume requires new photos or changed text")
            break
        else:
            if command["eventType"] != "DIAGNOSIS_REQUESTED" or round_no != 0:
                raise CommandConflict("Initial request must start at round zero")
        with self.db:
            self.db.execute("INSERT INTO jobs VALUES(?,?,NULL)", (command["eventId"], raw))
        return None

    def append(self, command, kind, payload):
        row = self.db.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE job=?", (command["eventId"],)).fetchone()
        sequence = row[0] + 1
        event = {k: command[k] for k in ("schemaVersion", "repairId", "taskId", "correlationId",
                                       "inputRevision", "executionId")}
        event.update(eventId=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{command['eventId']}:{sequence}")),
                     eventType=kind, occurredAt=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                     sequence=sequence, payload=payload)
        VALIDATOR.validate(event)
        self.db.execute("INSERT INTO events(job,seq,body) VALUES(?,?,?)",
                        (command["eventId"], sequence, encode(event)))
        return event

    def finish(self, command, result):
        with self.db:
            event = self.append(command, result["eventType"], result["payload"])
            self.db.execute("UPDATE jobs SET result=? WHERE id=?", (encode(result), command["eventId"]))
        return event

    def close(self):
        self.db.close()
