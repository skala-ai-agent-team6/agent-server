from time import monotonic
from typing import TypedDict
from uuid import uuid4
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import interrupt
from app.adapters import AnalysisTools

class State(TypedDict, total=False):
    request: dict
    status: str
    workflow_status: str
    selected: list[str]
    results: list[dict]
    candidates: list[dict]
    executions: list[dict]
    photo_request_count: int
    tool_call_count: int
    retry_count: int
    missing: list[str]
    error_code: str | None
    order: dict | None

class BudgetExceeded(Exception):
    pass


def build_graph(tools: AnalysisTools, max_calls=24, max_retries=2, active_seconds=120):
    def route(state):
        req = state["request"]
        # 첫 버전의 결정적 라우터. 이후 LLM structured output으로 교체한다.
        selected = {a["target"] for a in req["attachments"]}
        if req["error_codes"] or any(w in req["symptom"].lower() for w in ("sw", "펌웨어", "업데이트")):
            selected.add("SW")
        if any(w in req["symptom"].lower() for w in ("모터", "바퀴", "motor")):
            selected.add("MOTOR")
        if any(w in req["symptom"].lower() for w in ("pcb", "보드", "기판")):
            selected.add("PCB")
        return {"selected": sorted(selected or {"PCB", "MOTOR", "SW"}), "status": "RUNNING"}

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
                          "task_id": state["request"]["task_id"]}
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
            missing = [r["domain"] for r in results if not r["sufficient"]]
            candidates = []
            if not missing:
                sources = invoke("search_manual", lambda: tools.search(state["request"]["symptom"]))
                candidates = [{"rank": i+1, "cause_code": f"MOCK_{r['domain']}",
                               "cause_description": r["finding"], "evidence": {"analysis": r, "manuals": sources},
                               "repair_guide": "[MOCK] 담당 기사 확인 후 점검·수리", "part_id": None}
                              for i, r in enumerate(results)]
            return {"results": results, "missing": missing, "candidates": candidates,
                    "executions": executions, "tool_call_count": calls, "retry_count": retries}
        except (TimeoutError, ConnectionError, BudgetExceeded) as exc:
            return {"status": "FAILED", "workflow_status": "DONE", "error_code": type(exc).__name__,
                    "executions": executions, "tool_call_count": calls, "retry_count": retries}

    def next_step(state):
        if state["status"] == "FAILED":
            return END
        if state["missing"]:
            return "failed" if state["photo_request_count"] >= 3 else "request_photo"
        return "diagnosed"

    def request_photo(state):
        return {"photo_request_count": state["photo_request_count"] + 1, "status": "WAITING_INPUT"}

    def wait_photo(state):
        # interrupt 앞에 외부 쓰기나 횟수 증가를 두지 않는다. 재개 시 노드가 재실행된다.
        submission = interrupt({"type": "PHOTO_REQUESTED", "round_no": state["photo_request_count"],
                                "targets": state["missing"], "instruction": "해당 부위 전체와 커넥터를 선명하게 촬영해주세요."})
        request = {**state["request"], "attachments": state["request"]["attachments"] + submission["attachments"]}
        return {"request": request, "status": "RUNNING"}

    def diagnosed(state):
        return {"status": "SUCCEEDED", "workflow_status": "DONE"}

    def propose(state):
        return {"workflow_status": "WAITING_APPROVAL", "order": {
            "id": f"mock-order-{state['request']['task_id']}", "status": "PENDING",
            "description": "[MOCK] 부품 발주안. 실제 부품·수량 조회 미연결"}}

    def approval(state):
        decision = interrupt({"type": "APPROVAL_REQUIRED", "order": state["order"]})
        return {"order": {**state["order"], "status": "ORDERED" if decision["approved"] else "REJECTED"},
                "workflow_status": "DONE"}

    graph = StateGraph(State)
    for name, fn in [("route", route), ("analyze", analyze), ("request_photo", request_photo),
                     ("wait_photo", wait_photo), ("diagnosed", diagnosed), ("propose", propose), ("approval", approval)]:
        graph.add_node(name, fn)
    graph.add_node("failed", lambda s: {"status": "FAILED", "workflow_status": "DONE", "error_code": "PHOTO_LIMIT"})
    graph.add_edge(START, "route")
    graph.add_edge("route", "analyze")
    graph.add_conditional_edges("analyze", next_step)
    graph.add_edge("request_photo", "wait_photo")
    graph.add_edge("wait_photo", "route")
    graph.add_conditional_edges("diagnosed", lambda s: "propose" if s["request"]["mock_order_required"] else END)
    graph.add_edge("propose", "approval")
    graph.add_edge("approval", END)
    graph.add_edge("failed", END)
    return graph.compile(checkpointer=InMemorySaver())
