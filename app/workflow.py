from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from time import monotonic
from typing import TypedDict
from uuid import uuid4
from langgraph.graph import StateGraph, START, END
from app.adapters import AnalysisTools
from app.models import DiagnosisReport
from app.observability import context, log_event
from app.limits import MAX_ADDITIONAL_REQUESTS


class State(TypedDict, total=False):
    request: dict
    status: str
    selected: list[str]
    results: list[dict]
    sources: list[dict]
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


def failure_update(state, code):
    """그래프/실행 한도를 넘겨도 예외 대신 저장 가능한 실패 결과를 만든다."""
    req = state["request"]
    return {"status": "FAILED", "error_code": code, "report": None,
            "outgoing_event": {"event_id": f"{req['event_id']}:DIAGNOSIS_FAILED",
                               "event_type": "DIAGNOSIS_FAILED", "task_id": req["task_id"],
                               "case_id": req["case_id"],
                               "payload": {"error_code": code, "manual_review_required": True}}}


def build_graph(tools: AnalysisTools, max_calls=24, max_retries=2,
                active_seconds=120, confidence_threshold=0.5):
    if max_calls < 1 or max_retries < 0 or active_seconds <= 0 or not 0 <= confidence_threshold <= 1:
        raise ValueError("Invalid execution limits")

    def route(state):
        req = state["request"]
        # 입력 부위는 기사에게 받는다. 사진 종류를 LLM으로 재분류하지 않는다.
        # 기사 소견은 분석 도구와 RAG 검색에 전달한다. 별도 SW 분석 분기는 없다.
        selected = {a["target"] for a in req["attachments"]}
        log_event("route.selected", **context(req), selected=sorted(selected),
                  photo_count=len(req["attachments"]))
        return {"selected": sorted(selected), "status": "RUNNING"}

    def analyze(state):
        calls, retries = state["tool_call_count"], state["retry_count"]
        executions = list(state["executions"])
        results = []
        started = monotonic()
        lock = Lock()

        def invoke(name, fn):
            nonlocal calls, retries
            call_key = str(uuid4())
            for attempt in range(1, max_retries + 2):
                with lock:
                    if calls >= max_calls or monotonic() - started >= active_seconds:
                        raise BudgetExceeded()
                    calls += 1
                    call_count = calls
                attempt_started = monotonic()
                log_event("tool.started", **context(state["request"]), tool=name, call_key=call_key,
                          attempt=attempt, tool_call_count=call_count)
                record = {"call_key": call_key, "tool_name": name, "attempt": attempt,
                          "task_id": state["request"]["task_id"], "event_id": state["request"]["event_id"]}
                try:
                    value = fn()
                    if monotonic() - started >= active_seconds:
                        raise BudgetExceeded()
                    executions.append({**record, "status": "SUCCEEDED", "output": value})
                    summary = ({"domain": value["domain"], "sufficient": value["sufficient"],
                                "confidence": value["confidence"], "evidence_count": len(value["evidence_ids"])}
                               if name.startswith("analyze_") else {"source_count": len(value)})
                    log_event("tool.succeeded", **context(state["request"]), tool=name, call_key=call_key,
                              attempt=attempt, duration_ms=round((monotonic()-attempt_started)*1000, 2),
                              output_summary=summary)
                    return value
                except (TimeoutError, ConnectionError, ValueError) as exc:
                    executions.append({**record, "status": "FAILED", "error": type(exc).__name__})
                    log_event("tool.failed", **context(state["request"]), tool=name, call_key=call_key,
                              attempt=attempt, error_type=type(exc).__name__,
                              duration_ms=round((monotonic()-attempt_started)*1000, 2))
                    if isinstance(exc, ValueError) or attempt > max_retries:
                        raise
                    with lock:
                        retries += 1
                    log_event("tool.retry", **context(state["request"]), tool=name, next_attempt=attempt+1,
                              retry_count=retries, photo_request_count=state["photo_request_count"])

        sources = []
        try:
            # PCB·기계·RAG는 같은 분석 단계에서 시작한다. 모든 결과 수집 후 충분성을 판단한다.
            errors = []
            with ThreadPoolExecutor(max_workers=3) as pool:
                jobs = {pool.submit(invoke, f"analyze_{d.lower()}",
                                    lambda domain=d: tools.analyze(domain, state["request"])): d
                        for d in state["selected"]}
                jobs[pool.submit(invoke, "search_manual", lambda: tools.search(state["request"]["symptom"]))] = "RAG"
                for job in as_completed(jobs):
                    try:
                        value = job.result()
                        if jobs[job] == "RAG":
                            sources = value
                        else:
                            results.append(value)
                    except (TimeoutError, ConnectionError, BudgetExceeded, ValueError) as exc:
                        errors.append(exc)
            if errors:
                raise errors[0]
            results.sort(key=lambda r: r["domain"])
            missing = [r["domain"] for r in results
                       if not r["sufficient"] or r["confidence"] < confidence_threshold]
            if not results:
                missing = ["GENERAL"]
            log_event("analysis.evaluated", **context(state["request"]), sufficient=not missing,
                      missing=missing, confidence_threshold=confidence_threshold,
                      photo_request_count=state["photo_request_count"])
            report = None
            if not missing:
                # 실제 LLM with_structured_output 연결 전 Pydantic 출력 검증.
                report = DiagnosisReport.model_validate({
                    "task_id": state["request"]["task_id"], "case_id": state["request"]["case_id"],
                    "mode": getattr(tools, "mode", "mock"),
                    "candidates": [{"rank": i+1, "cause_code": f"{'DETECTED' if r.get('mode') == 'live' else 'MOCK'}_{r['domain']}",
                                    "cause_description": r["finding"], "confidence": r["confidence"],
                                    "evidence": {"photo_ids": r["evidence_ids"], "manuals": sources,
                                                 "finding": r["finding"]},
                                    "repair_guide": "담당 기사 확인 필요. 수리 방법 자동 생성은 아직 미연결",
                                    "annotated_image": r.get("annotated_image")}
                                   for i, r in enumerate(results)]
                }).model_dump()
            return {"results": results, "sources": sources, "missing": missing, "report": report,
                    "executions": executions, "tool_call_count": calls, "retry_count": retries}
        except (TimeoutError, ConnectionError, BudgetExceeded, ValueError) as exc:
            log_event("analysis.failed", **context(state["request"]), error_type=type(exc).__name__,
                      tool_call_count=calls, retry_count=retries)
            return {"status": "FAILED", "error_code": type(exc).__name__, "report": None,
                    "results": results, "sources": sources,
                    "executions": executions, "tool_call_count": calls, "retry_count": retries}

    def next_step(state):
        if state["status"] == "FAILED":
            destination = "failed"
        elif state["missing"]:
            destination = "failed" if state["photo_request_count"] >= MAX_ADDITIONAL_REQUESTS else "request_input"
        else:
            destination = "complete"
        log_event("graph.branch", **context(state["request"]), next_node=destination,
                  photo_request_count=state["photo_request_count"])
        return destination

    def event(state, kind, payload):
        return {"event_id": f"{state['request']['event_id']}:{kind}", "event_type": kind,
                "task_id": state["request"]["task_id"], "case_id": state["request"]["case_id"],
                "payload": payload}

    def request_input(state):
        # 분기 코드가 바뀌어도 이 노드 자체에서 4차 요청을 막는다.
        if state["photo_request_count"] >= MAX_ADDITIONAL_REQUESTS:
            log_event("input.limit_reached", **context(state["request"]),
                      photo_request_count=state["photo_request_count"])
            return failure_update(state, "PHOTO_LIMIT")
        count = state["photo_request_count"] + 1
        instructions = {
            "PCB": "PCB 기판 전체와 결함 의심 부위를 선명하게 촬영해주세요.",
            "MECHANICAL": "찌그러짐 등 의심 부위의 전체와 근접 사진을 제출해주세요.",
            "GENERAL": "분석할 부위의 사진과 구체적인 증상을 추가해주세요.",
        }
        log_event("input.requested", **context(state["request"]), request_round=count,
                  targets=state["missing"], graph_ends=True)
        return {"photo_request_count": count, "status": "WAITING_INPUT",
                "outgoing_event": event(state, "ADDITIONAL_INPUT_REQUESTED", {
                    "round_no": count, "targets": state["missing"],
                    "instructions": [instructions[d] for d in state["missing"]]})}

    def complete(state):
        return {"status": "SUCCEEDED", "outgoing_event": event(state, "DIAGNOSIS_COMPLETED", {"report": state["report"]})}

    def failed(state):
        code = state["error_code"] or "PHOTO_LIMIT"
        return failure_update(state, code)

    graph = StateGraph(State)
    def observed(name, fn):
        def run(state):
            started = monotonic()
            log_event("node.started", **context(state["request"]), node=name)
            try:
                update = fn(state)
            except Exception as exc:
                log_event("node.failed", **context(state["request"]), node=name,
                          error_type=type(exc).__name__, duration_ms=round((monotonic()-started)*1000, 2))
                raise
            log_event("node.finished", **context(state["request"]), node=name,
                      duration_ms=round((monotonic()-started)*1000, 2),
                      status=update.get("status", state["status"]))
            return update
        return run

    for name, fn in [("route", route), ("analyze", analyze), ("request_input", request_input),
                     ("complete", complete), ("failed", failed)]:
        graph.add_node(name, observed(name, fn))
    graph.add_edge(START, "route")
    graph.add_edge("route", "analyze")
    graph.add_conditional_edges("analyze", next_step)
    for name in ("request_input", "complete", "failed"):
        graph.add_edge(name, END)
    # 이벤트 하나당 끝까지 실행하고 종료한다. interrupt/체크포인트 재개 없음.
    return graph.compile()
