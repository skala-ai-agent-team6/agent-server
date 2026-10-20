"""Kafka 연결 전 동일 이벤트 계약을 시험하는 로컬 API."""
import os
from fastapi import FastAPI, HTTPException
from app.backend_v2 import BackendCommand, process_backend_command
from app.adapters import MockTools
from app.models import DiagnosisEvent
from app.service import EventConflict, EventProcessor
from app.workflow import build_graph


def create_app(tools=None, store_path=":memory:", **limits):
    app = FastAPI(title="Agent Server — local event mock", version="0.2.0")
    processor = EventProcessor(build_graph(tools or MockTools(), **limits), store_path)
    app.state.processor = processor

    @app.get("/health")
    def health():
        return {"status": "ok", "mode": "mock", "persistence": "sqlite" if store_path != ":memory:" else "memory"}

    @app.post("/internal/events")
    def process_event(body: DiagnosisEvent):
        try:
            return processor.process(body)
        except EventConflict as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/internal/backend-v2/events")
    def backend_event(body: BackendCommand):
        try:
            return process_backend_command(processor, body)
        except EventConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/internal/backend-v2/outgoing-events")
    def backend_outgoing():
        return processor.backend_outgoing()

    @app.get("/internal/diagnoses/{task_id}")
    def get(task_id: str):
        try:
            return processor.get(task_id)
        except KeyError as exc:
            raise HTTPException(404, "Unknown task") from exc

    @app.get("/internal/outgoing-events")
    def outgoing():
        # 로컬 확인용. Kafka/Spring에 실제 전송하지는 않는다.
        return processor.outgoing()

    return app


app = create_app(store_path=os.getenv("AGENT_STATE_PATH", ".local/agent-state.sqlite3"))
