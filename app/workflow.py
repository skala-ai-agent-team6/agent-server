from time import monotonic
from typing import TypedDict
from uuid import uuid4
from langgraph.graph import StateGraph, START, END
from app.adapters import AnalysisTools
from app.models import DiagnosisReport


class State(TypedDict, total=False):
    request: dict
    status: str
    selected: list[str]
    results: list[dict]
    report: dict | None
    executions: list[dict]
    photo_request_count: int
    tool_call_count: int
    retry_count: int
    missing: list[str]
    error_code: str | None
    outgoing_event: dict | None


class BudgetExceeded(Exception):
    pass


def build_graph(tools: AnalysisTools, max_calls=24, max_retries=2,
                active_seconds=120, confidence_threshold=0.5):
    if max_calls < 1 or max_retries < 0 or active_seconds <= 0 or not 0 <= confidence_threshold <= 1:
        raise ValueError("Invalid execution limits")

    def route(state):
        req = state["request"]
        # 입력 부위는 기사에게 받는다. 사진 종류를 LLM으로 재분류하지 않는다.
        # 자연어 단서 추출은 임시 키워드 규칙이며 이후 구조화 출력으로 교체한다.
        selected = {a["target"] for a in req["attachments"]}
        if any(w in req["symptom"].lower() for w in ("sw", "펌웨어", "업데이트", "소프트웨어")):
            selected.add("SW")
        return {"selected": sorted(selected), "status": "RUNNING"}

    def analyze(state):
        calls, retries = state["tool_call_count"], state["retry_count"]
        executions = list(state["executions"])
        results = []
        started = monotonic()

        def invoke(name, fn):
            nonlocal calls, retries
            call_key = str(uuid4())
            for attempt in range(1, max_retries + 2):
                if calls >= max_calls or monotonic() - started >= active_seconds:
                    raise BudgetExceeded()
                calls += 1
                record = {"call_key": call_key, "tool_name": name, "attempt": attempt,
                          "task_id": state["request"]["task_id"], "event_id": state["request"]["event_id"]}
                try:
                    value = fn()
                    if monotonic() - started >= active_seconds:
                        raise BudgetExceeded()
                    executions.append({**record, "status": "SUCCEEDED", "output": value})
                    return value
                except (TimeoutError, ConnectionError) as exc:
                    executions.append({**record, "status": "FAILED", "error": type(exc).__name__})
                    if attempt > max_retries:
                        raise
                    retries += 1

        try:
            for domain in state["selected"]:
                results.append(invoke(f"analyze_{domain.lower()}", lambda d=domain: tools.analyze(d, state["request"])))
            missing = [r["domain"] for r in results
                       if not r["sufficient"] or r["confidence"] < confidence_threshold]
            if not results:
                missing = ["GENERAL"]
            report = None
            if not missing:
                sources = invoke("search_manual", lambda: tools.search(state["request"]["symptom"]))
                # 실제 LLM with_structured_output 연결 전 Pydantic 출력 검증.
                report = DiagnosisReport.model_validate({
                    "task_id": state["request"]["task_id"], "case_id": state["request"]["case_id"],
                    "candidates": [{"rank": i+1, "cause_code": f"MOCK_{r['domain']}",
                                    "cause_description": r["finding"], "confidence": r["confidence"],
                                    "evidence": {"photo_ids": r["evidence_ids"], "manuals": sources,
                                                 "finding": r["finding"]},
                                    "repair_guide": "[MOCK] 담당 기사 확인 후 점검·수리",
                                    "annotated_image": r.get("annotated_image")}
                                   for i, r in enumerate(results)]
                }).model_dump()
            return {"results": results, "missing": missing, "report": report,
                    "executions": executions, "tool_call_count": calls, "retry_count": retries}
        except (TimeoutError, ConnectionError, BudgetExceeded) as exc:
            return {"status": "FAILED", "error_code": type(exc).__name__, "report": None,
                    "executions": executions, "tool_call_count": calls, "retry_count": retries}

    def next_step(state):
        if state["status"] == "FAILED":
            return "failed"
        if state["missing"]:
            return "failed" if state["photo_request_count"] >= 3 else "request_input"
        return "complete"

    def event(state, kind, payload):
        return {"event_id": f"{state['request']['event_id']}:{kind}", "event_type": kind,
                "task_id": state["request"]["task_id"], "case_id": state["request"]["case_id"],
                "payload": payload}

    def request_input(state):
        count = state["photo_request_count"] + 1
        instructions = {
            "PCB": "PCB 기판 전체와 결함 의심 부위를 선명하게 촬영해주세요.",
            "MECHANICAL": "찌그러짐 등 의심 부위의 전체와 근접 사진을 제출해주세요.",
            "SW": "증상 발생 시점·업데이트 여부·재현 절차를 추가로 설명해주세요.",
            "GENERAL": "분석할 부위의 사진과 구체적인 증상을 추가해주세요.",
        }
        return {"photo_request_count": count, "status": "WAITING_INPUT",
                "outgoing_event": event(state, "ADDITIONAL_INPUT_REQUESTED", {
                    "round_no": count, "targets": state["missing"],
                    "instructions": [instructions[d] for d in state["missing"]]})}

    def complete(state):
        return {"status": "SUCCEEDED", "outgoing_event": event(state, "DIAGNOSIS_COMPLETED", {"report": state["report"]})}

    def failed(state):
        code = state["error_code"] or "PHOTO_LIMIT"
        return {"status": "FAILED", "error_code": code,
                "outgoing_event": event(state, "DIAGNOSIS_FAILED", {
                    "error_code": code, "manual_review_required": True})}

    graph = StateGraph(State)
    for name, fn in [("route", route), ("analyze", analyze), ("request_input", request_input),
                     ("complete", complete), ("failed", failed)]:
        graph.add_node(name, fn)
    graph.add_edge(START, "route")
    graph.add_edge("route", "analyze")
    graph.add_conditional_edges("analyze", next_step)
    for name in ("request_input", "complete", "failed"):
        graph.add_edge(name, END)
    # 이벤트 하나당 끝까지 실행하고 종료한다. interrupt/체크포인트 재개 없음.
    return graph.compile()
