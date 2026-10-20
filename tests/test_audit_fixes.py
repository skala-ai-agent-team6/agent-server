import asyncio
import copy
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from openai import APIStatusError

from worker import config as C, graph, harness, tools, main
from worker.send_test import COMMAND
from worker.journal import Journal


@pytest.mark.parametrize("status,attempts", [(429, 3), (503, 3), (401, 1)])
def test_status_retry_policy(monkeypatch, status, attempts):
    monkeypatch.setattr(harness.asyncio, "sleep", AsyncMock())
    error = APIStatusError("test", response=httpx.Response(
        status, request=httpx.Request("POST", "https://example.invalid")), body={})
    async def run():
        fn = AsyncMock(side_effect=error)
        fn.__name__ = "model_call"
        with pytest.raises(APIStatusError):
            await harness.guarded()(fn)()
        assert fn.await_count == attempts
    asyncio.run(run())


def test_references_reach_final_report(monkeypatch):
    class Client:
        def __init__(self, *args, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def call_tool(self, *args, **kwargs):
            return SimpleNamespace(structured_content={
                "repair_cases": [], "label_definitions": [], "case_search_status": "no_candidates",
                "pcb_references": [{"document_id": "tech-1", "text": "Measure voltage",
                                    "source_url": "https://example.org/technical"}]})
    monkeypatch.setattr(tools, "Client", Client)
    writer = AsyncMock(return_value=tools.Report(summary="test", repairMethod="inspect",
        candidates=[tools.Candidate(cause="test", confidence=.5, explanation="technical reference")]))
    monkeypatch.setattr(tools, "write_report", writer)
    async def run():
        rag = await tools.call_rag_mcp("power failure", [])
        state = {"command": copy.deepcopy(COMMAND), "text": "power failure",
                 "findings": [{"source": "RAG", "found": False, "output": rag}]}
        result = await graph.report(state, {})
        assert "Measure voltage" in str(writer.call_args)
        assert result["result"]["payload"]["evidence"] == [{
            "type": "MANUAL", "citation": "tech-1", "excerpt": "Measure voltage",
            "sourceUri": "https://example.org/technical"}]
    asyncio.run(run())


def test_100_dent_photos_produce_one_interpretation_label(monkeypatch):
    from worker import enrichment
    interpreter = AsyncMock(return_value={"detections": []})
    monkeypatch.setattr(enrichment, "interpret_damage", interpreter)
    model = AsyncMock(return_value={"is_defective": True, "defects": [{"type": "roof-dent"}]})
    state = {"photos": [{"kind": "MECH", "path": "fixture.jpg", "attachmentId": str(i+1)}
                        for i in range(100)]}
    asyncio.run(graph.run_photo_mcp(state, {"configurable": {"emit": AsyncMock()}},
                                   "MECH", model, "detect_dents", "MOTOR", "mechanical"))
    assert model.await_count == 100
    interpreter.assert_awaited_once_with([{"damage_type": "dent", "part": "roof"}])


def test_download_failure_produces_failed_not_sample_report(monkeypatch, tmp_path):
    from worker import report
    client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda req: httpx.Response(404))
    monkeypatch.setattr(tools.httpx, "AsyncClient", lambda **kw: client(transport=transport, **kw))
    model = AsyncMock()
    monkeypatch.setattr(tools, "call_pcb_mcp", model)
    monkeypatch.setattr(report, "OUT", tmp_path/"reports")
    async def run():
        journal = Journal(tmp_path/"state.db")
        try:
            result = await main.handle(copy.deepcopy(COMMAND), SimpleNamespace(send_and_wait=AsyncMock()), journal)
            assert result["eventType"] == "DIAGNOSIS_FAILED"
            assert model.await_count == 0
        finally:
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("corrupt", [False, True])
def test_download_integrity_and_target(monkeypatch, corrupt):
    content = b"test-image-bytes"
    client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda req: httpx.Response(200, content=content))
    monkeypatch.setattr(tools.httpx, "AsyncClient", lambda **kw: client(transport=transport, **kw))
    cmd = copy.deepcopy(COMMAND)
    cmd["payload"]["attachments"][0].update(
        byte_size=len(content), sha256="wrong" if corrupt else hashlib.sha256(content).hexdigest(), target="MOTOR")
    if corrupt:
        with pytest.raises(ValueError, match="checksum"):
            asyncio.run(tools.load_photos(cmd))
    else:
        photos = asyncio.run(tools.load_photos(cmd))
        assert photos[0]["target"] == "MOTOR"
        assert photos[0]["attachmentId"] == "1"


def test_kafka_consumer_uses_long_job_limit(monkeypatch):
    class Consumer:
        def __init__(self, *args, **kwargs):
            assert kwargs["max_poll_interval_ms"] == C.KAFKA_MAX_POLL_INTERVAL_MS
            assert kwargs["max_poll_interval_ms"] >= (C.RUN_TIMEOUT+180)*1000
            assert kwargs["max_poll_records"] == 1
        async def start(self):
            pass
        def assignment(self):
            return {0}
        async def stop(self):
            pass
        def __aiter__(self):
            return self
        async def __anext__(self):
            raise StopAsyncIteration
    monkeypatch.setattr(main, "AIOKafkaConsumer", Consumer)
    monkeypatch.setattr(main, "AIOKafkaProducer", lambda **kwargs: SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock()))
    asyncio.run(main.run())
