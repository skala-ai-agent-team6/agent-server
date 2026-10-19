"""로컬 단일 프로세스 모의 API. 운영 서버나 Spring API 대체물이 아니다."""
from threading import RLock
from fastapi import FastAPI, HTTPException
from langgraph.types import Command
from app.adapters import MockTools
from app.models import DiagnosisRequest, PhotoSubmission, Approval
from app.workflow import build_graph


def create_app(tools=None, **limits):
    app = FastAPI(title="Repair Agent — local mock", version="0.1.0")
    graph = build_graph(tools or MockTools(), **limits)
    lock = RLock()
    requests = {}

    def config(task_id):
        return {"configurable": {"thread_id": task_id}, "recursion_limit": 50}

    def snapshot(task_id):
        state = graph.get_state(config(task_id))
        if not state.values:
            raise HTTPException(404, "Unknown task")
        pending = [i.value for t in state.tasks for i in t.interrupts]
        return {"mode": "mock", "task_id": task_id, **state.values,
                "checkpoint_ref": state.config["configurable"].get("checkpoint_id"), "pending": pending}

    @app.get("/health")
    def health():
        return {"status": "ok", "mode": "mock", "persistence": "memory"}

    @app.post("/internal/diagnoses")
    def start(body: DiagnosisRequest):
        data = body.model_dump()
        with lock:
            if body.task_id in requests:
                if requests[body.task_id] != data:
                    raise HTTPException(409, "Task ID already used with different input")
                return snapshot(body.task_id)
            for task_id in requests:
                old = snapshot(task_id)
                if old["request"]["case_id"] == body.case_id and old["status"] in ("RUNNING", "WAITING_INPUT"):
                    raise HTTPException(409, "Case already has an active diagnosis")
            requests[body.task_id] = data
            graph.invoke({"request": data, "status": "RUNNING", "workflow_status": "RUNNING",
                          "photo_request_count": 0, "tool_call_count": 0, "retry_count": 0,
                          "executions": [], "error_code": None, "order": None}, config(body.task_id))
            return snapshot(body.task_id)

    @app.get("/internal/diagnoses/{task_id}")
    def get(task_id: str):
        with lock:
            return snapshot(task_id)

    @app.post("/internal/diagnoses/{task_id}/photos")
    def photos(task_id: str, body: PhotoSubmission):
        with lock:
            current = snapshot(task_id)
            if current["status"] != "WAITING_INPUT" or body.round_no != current["photo_request_count"]:
                raise HTTPException(409, "Not waiting for this photo round")
            ids = {a["id"] for a in current["request"]["attachments"]}
            new_ids = [a.id for a in body.attachments]
            if len(set(new_ids)) != len(new_ids) or ids.intersection(new_ids):
                raise HTTPException(409, "Attachment IDs must be new and unique")
            graph.invoke(Command(resume=body.model_dump()), config(task_id))
            return snapshot(task_id)

    @app.post("/internal/diagnoses/{task_id}/mock-approval")
    def approval(task_id: str, body: Approval):
        with lock:
            current = snapshot(task_id)
            if current["workflow_status"] != "WAITING_APPROVAL" or body.order_id != current["order"]["id"]:
                raise HTTPException(409, "Not waiting for this order approval")
            graph.invoke(Command(resume=body.model_dump()), config(task_id))
            return snapshot(task_id)

    return app

app = create_app()
