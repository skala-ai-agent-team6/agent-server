"""실제 MCP·LLM 검증. 사진 입력과 Kafka 출력만 로컬로 연결해 Spring과 분리한다."""
import argparse
import asyncio
import copy
import json
import hashlib
import logging
import uuid
from contextlib import ExitStack
from unittest.mock import patch

from . import config as C, enrichment, main, report, tools
from .journal import Journal, VALIDATOR
from .send_test import COMMAND


class LocalOutput:
    """브로커에 발행하지 않고 같은 이벤트를 파일 검증용으로 수집."""
    def __init__(self):
        self.events = []

    async def send_and_wait(self, topic, body, **kwargs):
        event = json.loads(body)
        VALIDATOR.validate(event)
        self.events.append(event)
        print(event["eventType"], event["payload"].get("stage") or event["payload"].get("tool") or "", flush=True)


async def run(scenario, fault=None):
    if not C.USE_GPT:
        raise RuntimeError("실제 모델 테스트에는 OPENAI_API_KEY가 필요합니다")
    if fault and scenario != "mixed":
        raise ValueError("장애 검증은 mixed 시나리오에서만 실행합니다")
    cases = {
        "multi-pcb": [("PCB", C.HERE/"samples/pcb_sample.jpg"),
                      ("PCB", C.ROOT.parent/"MCP_Error_check/pcb_agent/test_samples/l_light_01_missing_hole_07_2_600.jpg")],
        "mixed": [("PCB", C.HERE/"samples/pcb_sample.jpg"), ("MOTOR", C.HERE/"samples/mech_sample.jpg")],
        "mechanical": [("MOTOR", C.HERE/"samples/mech_sample.jpg")],
    }
    selected = cases if scenario == "all" else {scenario: cases[scenario]}
    summaries = []
    for name, photos in selected.items():
        cmd = copy.deepcopy(COMMAND)
        task = str(uuid.uuid4().int % 100000000 + 1)
        cmd.update(eventId=str(uuid.uuid4()), taskId=task, repairId=task)
        cmd["payload"]["repair"]["id"] = task
        cmd["payload"]["task"]["id"] = task
        cmd["payload"]["repair"]["symptom"] = "충전이 안 되고 바퀴 모터에서 소음이 납니다."
        cmd["payload"]["repair"]["user_input"] = {"customerDescription": "외관과 기판을 점검해주세요."}
        cmd["payload"]["repair"]["error_codes"] = []
        cmd["payload"]["attachments"] = [
            {"id": str(i+1), "request_id": None, "mime_type": "image/jpeg",
             "downloadPath": f"/internal/attachments/{i+1}", "target": target,
             "byte_size": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for i,(target,path) in enumerate(photos)]
        cmd["payload"]["attachmentIds"] = [a["id"] for a in cmd["payload"]["attachments"]]
        local = [{"attachmentId": str(i+1), "path": str(path.resolve()), "target": target}
                 for i,(target,path) in enumerate(photos)]
        output = LocalOutput()
        journal = Journal(C.ROOT/".local/live-check.sqlite3")
        try:
            # 모델/해석/RAG/LLM 함수는 대체하지 않는다.
            with ExitStack() as stack:
                stack.enter_context(patch.object(tools, "load_photos", return_value=local))
                # 서버를 끄지 않는다. 이 검증 프로세스의 주소만 연결 불가 주소로 바꾼다.
                if fault:
                    setting = {"mechanical-unavailable": "MECH_MCP_URL",
                               "rag-unavailable": "RAG_MCP_URL"}[fault]
                    stack.enter_context(patch.object(C, setting, "http://127.0.0.1:1/mcp"))
                result = await main.handle(cmd, output, journal)
            metrics = json.loads(journal.db.execute(
                "SELECT metrics FROM executions WHERE event_id=?", (cmd["eventId"],)).fetchone()[0])
        finally:
            journal.close()
        errors = [e["payload"] for e in output.events
                  if e["eventType"] == "TOOL_EXECUTED" and e["payload"]["output"].get("error")]
        reference_ids = {hit["citation"] for e in output.events if e["eventType"] == "TOOL_EXECUTED"
                         for hit in e["payload"]["output"].get("hits", []) if hit["kind"] == "REFERENCE"}
        saved_reference_ids = {e.get("citation") for e in result["payload"].get("evidence", [])
                               if e["type"] == "MANUAL"}
        if not reference_ids <= saved_reference_ids:
            raise RuntimeError("Technical references missing from final report")
        path = report.OUT/f"task-{task}_revision-1_고장리포트.md"
        event_path = path.with_name(path.stem+"_events.json")
        event_path.write_text(json.dumps(output.events, ensure_ascii=False, indent=2))
        row = {"scenario": name, "mode": "real-models-local-input-output",
               "fault": fault,
               "metrics": metrics,
               "photo_count": len(photos), "status": result["eventType"], "errors": errors,
               "technical_references": len(reference_ids),
               "report": str(path), "json": str(path.with_suffix(".json")), "events": str(event_path)}
        summaries.append(row)
        summary_name = f"live-{fault}-summary.json" if fault else "live-summary.json"
        (report.OUT/summary_name).write_text(json.dumps(summaries, ensure_ascii=False, indent=2))
        print(json.dumps(row, ensure_ascii=False), flush=True)
        if fault == "mechanical-unavailable":
            valid = (result["eventType"] == "DIAGNOSIS_FAILED"
                     and result["payload"]["errorCode"] == "REQUIRED_TOOL_FAILED"
                     and any(e["output"].get("failurePolicy") == "required" for e in errors)
                     and metrics["retries"] >= 2
                     and not any(e["eventType"] == "PHOTO_REQUESTED" for e in output.events)
                     and "정상 판정이 아닙니다" in path.read_text())
        elif fault == "rag-unavailable":
            valid = (result["eventType"] == "DIAGNOSIS_COMPLETED" and len(errors) == 1
                     and errors[0].get("tool") == "retrieve_repair_context"
                     and errors[0]["output"].get("failurePolicy") == "optional"
                     and metrics["retries"] >= 2
                     and "근거 제한:" in result["payload"]["summary"]
                     and "근거 제한:" in path.read_text()
                     and not any(e["type"] in ("CASE", "MANUAL")
                                 for e in result["payload"]["evidence"]))
        else:
            valid = not errors and result["eventType"] == "DIAGNOSIS_COMPLETED"
        if not valid:
            raise RuntimeError(f"{name}: 실제 호출/리포트 결과 확인 필요. {summary_name} 참조")
    return summaries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["all", "multi-pcb", "mixed", "mechanical"], default="all")
    parser.add_argument("--fault", choices=["mechanical-unavailable", "rag-unavailable"],
                        help="mixed에서만 사용. 해당 MCP 주소를 이 프로세스 안에서만 연결 불가 주소로 변경")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run(args.scenario, args.fault))
