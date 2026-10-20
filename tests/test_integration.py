import asyncio
import copy
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from worker import config as C, enrichment, graph, harness, main, report, tools
from worker.journal import Journal, CommandConflict, VALIDATOR
from worker.send_test import COMMAND


def command():
    value = copy.deepcopy(COMMAND)
    value["eventId"] = str(uuid.uuid4())
    value["payload"]["attachments"][0].update(byte_size=1, sha256="fixture")
    return value


class Producer:
    def __init__(self):
        self.events = []
        self.fail_final = False

    async def send_and_wait(self, topic, body, **kwargs):
        event = json.loads(body)
        if self.fail_final and event.get("eventType") == "DIAGNOSIS_COMPLETED":
            raise ConnectionError("temporary broker failure")
        self.events.append(event)


@pytest.fixture
def fixture_tools(monkeypatch, tmp_path):
    image = str(tmp_path / "boxed.jpg")
    monkeypatch.setattr(tools, "load_photos", AsyncMock(side_effect=lambda cmd: [
        {"attachmentId": a["id"], "path": image, "target": a.get("target", "PCB")}
        for a in cmd["payload"]["attachments"]]))
    monkeypatch.setattr(tools, "split_text", AsyncMock(return_value=tools.Clues(pcb=[], mech=[], sw=[])))
    monkeypatch.setattr(enrichment, "normalize_observation", AsyncMock(return_value={"observations": []}))
    monkeypatch.setattr(enrichment, "interpret_pcb", AsyncMock(return_value={"detections": []}))
    monkeypatch.setattr(enrichment, "interpret_damage", AsyncMock(return_value={"detections": []}))
    out = {"is_defective": True, "defects": [{"type": "short", "confidence": .3}],
           "annotated_image": image}
    monkeypatch.setattr(tools, "call_pcb_mcp", AsyncMock(return_value=out))
    monkeypatch.setattr(tools, "call_mech_mcp", AsyncMock(return_value={**out, "is_defective": False, "defects": []}))
    monkeypatch.setattr(tools, "call_rag_mcp", AsyncMock(return_value={"hits": [], "status": "no_candidates"}))
    monkeypatch.setattr(tools, "write_report", AsyncMock(return_value=tools.Report(
        summary="테스트", repairMethod="점검", candidates=[tools.Candidate(cause="short", confidence=.3, explanation="관찰")])))
    monkeypatch.setattr(report, "OUT", tmp_path / "reports")
    return out


def test_mixed_preserves_any_found_and_low_confidence_rule(fixture_tools):
    cmd = command()
    cmd["payload"]["attachments"] += [{"id": "2", "target": "MOTOR"}]
    result = asyncio.run(graph.build_graph().ainvoke({"command": cmd},
                         {"configurable": {"emit": AsyncMock()}}))
    assert result["result"]["eventType"] == "DIAGNOSIS_COMPLETED"
    assert tools.call_pcb_mcp.await_count == tools.call_mech_mcp.await_count == 1
    assert tools.call_rag_mcp.await_count == 1
    assert tools.write_report.await_count == 1
    assert enrichment.interpret_damage.await_count == 0
    assert len(result["result"]["payload"]["evidence"]) == 1


def test_live_runner_preserves_command_ids_and_saves_report(fixture_tools, monkeypatch, tmp_path):
    from worker import verify_live
    monkeypatch.setattr(C, "USE_GPT", True)
    monkeypatch.setattr(C, "ROOT", tmp_path)
    rows = asyncio.run(verify_live.run("mixed"))
    assert rows[0]["status"] == "DIAGNOSIS_COMPLETED"
    saved = json.loads(Path(rows[0]["json"]).read_text())
    cmd = saved["command"]
    assert cmd["taskId"] == cmd["payload"]["task"]["id"]
    assert cmd["repairId"] == cmd["payload"]["repair"]["id"]
    assert Path(rows[0]["report"]).is_file()


def test_all_100_photos_without_incrementing_round(fixture_tools):
    cmd = command()
    cmd["payload"]["attachments"] = [{"id": str(i+1), "target": "PCB"} for i in range(100)]
    result = asyncio.run(harness.invoke(graph.build_graph(), cmd, AsyncMock()))
    assert tools.call_pcb_mcp.await_count == 100
    assert cmd["payload"]["round"] == 0
    assert len(result["result"]["payload"]["evidence"]) == 100


def test_dent_labels_preserve_six_and_never_invent_part():
    for part in enrichment.DENT_PARTS:
        assert enrichment.dent_labels({"defects": [{"type": part+"-dent"}]}) == [
            {"damage_type": "dent", "part": part}]
    assert enrichment.dent_labels({"defects": [{"type": "dent"}, {"type": "breakage"}]}) == []


def test_typed_dent_reaches_interpreter(fixture_tools):
    tools.call_mech_mcp.return_value = {**fixture_tools, "defects": [{"type": "roof-dent", "confidence": .7}]}
    cmd = command()
    cmd["payload"]["attachments"][0]["target"] = "MOTOR"
    result = asyncio.run(graph.build_graph().ainvoke({"command": cmd},
                         {"configurable": {"emit": AsyncMock()}}))
    enrichment.interpret_damage.assert_awaited_once_with([{"damage_type": "dent", "part": "roof"}])
    assert result["result"]["eventType"] == "DIAGNOSIS_COMPLETED"


def test_durable_delivery_does_not_repeat_models(fixture_tools, tmp_path):
    async def run():
        cmd, producer = command(), Producer()
        producer.fail_final = True
        journal = Journal(tmp_path / "state.db")
        with pytest.raises(ConnectionError):
            await main.handle(cmd, producer, journal)
        journal.close()
        assert tools.call_pcb_mcp.await_count == 1
        journal = Journal(tmp_path / "state.db")
        producer.fail_final = False
        result = await main.handle(cmd, producer, journal)
        await main.handle(cmd, producer, journal)
        assert tools.call_pcb_mcp.await_count == 1
        assert result["eventType"] == "DIAGNOSIS_COMPLETED"
        assert len([e for e in producer.events if e["eventType"] == "DIAGNOSIS_COMPLETED"]) == 1
        for e in producer.events:
            VALIDATOR.validate(e)
        assert list(report.OUT.glob("*.json")) and list(report.OUT.glob("*.md"))
        changed = copy.deepcopy(cmd)
        changed["payload"]["repair"]["symptom"] = "다른 입력"
        with pytest.raises(CommandConflict):
            await main.handle(changed, producer, journal)
        journal.close()
    asyncio.run(run())


def test_three_requests_then_failure(fixture_tools, tmp_path):
    tools.call_pcb_mcp.return_value = {**fixture_tools, "is_defective": False, "defects": []}
    async def run():
        journal, producer, cmd = Journal(tmp_path/"state.db"), Producer(), command()
        for n in range(4):
            result = await main.handle(cmd, producer, journal)
            if n == 3:
                assert result["eventType"] == "DIAGNOSIS_FAILED"
                break
            assert result["eventType"] == "PHOTO_REQUESTED"
            assert result["payload"]["round"] == n+1
            cmd = copy.deepcopy(cmd)
            cmd.update(eventId=str(uuid.uuid4()), eventType="DIAGNOSIS_RESUMED", inputRevision=n+2, executionId=str(uuid.uuid4()))
            photo = {**cmd["payload"]["attachments"][0], "id": str(n+2), "downloadPath": f"/internal/attachments/{n+2}"}
            cmd["payload"]["attachments"].append(photo)
            cmd["payload"]["attachmentIds"].append(photo["id"])
            cmd["payload"]["addedAttachmentIds"] = [photo["id"]]
            for k in ("round", "requestKey", "checkpoint"):
                cmd["payload"][k] = result["payload"][k]
            cmd["payload"]["requestId"] = str(n+1)
        journal.close()
    asyncio.run(run())


def test_limit_is_not_silenced_by_optional_mcp(fixture_tools, monkeypatch):
    monkeypatch.setattr(enrichment, "normalize_observation", AsyncMock(side_effect=harness.LimitExceeded()))
    with pytest.raises(harness.LimitExceeded):
        asyncio.run(harness.invoke(graph.build_graph(), command(), AsyncMock()))


def test_recursion_limit_is_effective(fixture_tools, monkeypatch):
    from langgraph.errors import GraphRecursionError
    monkeypatch.setattr(C, "RECURSION_LIMIT", 1)
    with pytest.raises(GraphRecursionError):
        asyncio.run(harness.invoke(graph.build_graph(), command(), AsyncMock()))


def test_tool_retries_are_bounded(monkeypatch):
    monkeypatch.setattr(harness.asyncio, "sleep", AsyncMock())
    async def run():
        fn = AsyncMock(side_effect=ConnectionError())
        fn.__name__ = "test_tool"
        token = harness.current.set(harness.Budget("test", 10))
        try:
            with pytest.raises(ConnectionError):
                await harness.guarded()(fn)()
            assert fn.await_count == 3
            assert harness.current.get().calls == 3
        finally:
            harness.current.reset(token)
    asyncio.run(run())


def test_unknown_recommendation_rejected(monkeypatch):
    monkeypatch.setattr(C, "USE_GPT", True)
    response = tools.Report(summary="결과", repairMethod="점검",
        candidates=[tools.Candidate(cause="PCB", confidence=.6, explanation="관찰")],
        recommendedParts=[tools.RecommendedPart(partId="999", quantity=1)])
    model = type("Model", (), {"ainvoke": AsyncMock(return_value=response)})()
    monkeypatch.setattr(tools, "gpt", lambda schema: model)
    with pytest.raises(ValueError, match="Unknown"):
        asyncio.run(tools.write_report("증상", [], [{"id": "1"}]))


def test_business_gateway_auth_mapping_and_write_rejection(monkeypatch):
    from worker import business_gateway as business
    from starlette.testclient import TestClient
    monkeypatch.setenv("BUSINESS_GATEWAY_TOKEN", "test-only-token")
    rpc = AsyncMock(return_value={"part": {"id": 1},
        "warehouses": [{"warehouse_id": 2, "on_hand": 10, "reserved": 2, "available": 8}],
        "suppliers": [{"factory_id": 3, "region": "서울", "phone": "fixture", "lead_days": 3}]})
    monkeypatch.setattr(business, "call", rpc)
    body = {"jsonrpc": "2.0", "id": "test", "method": "tools/call",
            "params": {"name": "inventory.lookup", "arguments": {"partIds": ["1", "1"]}}}
    with TestClient(business.app) as client:
        assert client.post("/rpc", json=body).status_code == 401
        headers = {"Authorization": "Bearer test-only-token"}
        out = client.post("/rpc", json=body, headers=headers).json()
        assert out["result"]["structuredContent"]["stocks"][0]["available"] == 8
        assert rpc.await_count == 1
        body["params"]["name"] = "create_purchase_order_draft"
        assert client.post("/rpc", json=body, headers=headers).status_code == 400
