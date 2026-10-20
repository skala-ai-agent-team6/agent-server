"""진단 그래프. 위에서 아래로 실행 순서대로 적었다.

  classify_photos → split_text → [pcb_mcp, mech_mcp 중 필요한 것 동시에] → rag_mcp → judge
      judge → report(완료) | request_photo(사진 요청) | fail(실패) → 끝

노드는 state(바구니)를 읽고, 바꿀 값만 dict로 돌려준다.
Spring에 진행 상황을 알릴 땐 emit(config, 이벤트종류, 내용)을 부른다.
"""

import operator
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph

from . import config as C
from . import tools
from . import enrichment
from .harness import LimitExceeded, observed


class State(TypedDict, total=False):
    command: dict  # Kafka로 받은 명령 원문
    text: str  # 증상 + 기사 설명
    photos: list[dict]  # [{attachmentId, path, kind: PCB|MECH|OTHER}]
    clues: dict  # {pcb: [], mech: [], sw: [], errorCodes: []}
    findings: Annotated[list[dict], operator.add]  # MCP 결과. 동시에 돌아도 덮어쓰지 않고 이어 붙인다
    decision: str  # COMPLETE | PHOTO | FAIL
    result: dict  # Spring에 보낼 마지막 이벤트 {eventType, payload}


async def emit(config, event_type: str, payload: dict):
    await config["configurable"]["emit"](event_type, payload)


# ── 1. 사진 분류 ─────────────────────────────────────────


async def classify_photos(state: State, config) -> dict:
    await emit(config, "DIAGNOSIS_PROGRESS", {"stage": "CLASSIFYING_PHOTOS"})
    photos = await tools.load_photos(state["command"])
    for p in photos:
        # 이미 구분해 접수한 사진은 그 구분을 사용. GENERAL/미지정은 원본 GPT 분류 유지.
        p["kind"] = {"PCB": "PCB", "MOTOR": "MECH", "MECHANICAL": "MECH"}.get(p.get("target"))
        if p["kind"] is None:
            p["kind"] = await tools.classify_photo(p["path"])
    return {"photos": photos}


# ── 2. 글 분리 ───────────────────────────────────────────


async def split_text(state: State, config) -> dict:
    await emit(config, "DIAGNOSIS_PROGRESS", {"stage": "SPLITTING_TEXT"})
    repair = state["command"]["payload"]["repair"]
    text = f"{repair.get('symptom') or ''} {repair.get('user_input', {}).get('customerDescription', '')}".strip()
    clues = (await tools.split_text(text)).model_dump()
    clues["errorCodes"] = repair.get("error_codes") or []
    observation = await supplemental(config, "normalize_observation", "observation-mcp",
                                     lambda: enrichment.normalize_observation(text))
    return {"text": text, "clues": clues, "findings": [observation]}


async def supplemental(config, name, server, call):
    """추가 해석은 근거만 보강한다. 성공/실패로 원본의 found 판단을 바꾸지 않는다."""
    try:
        out = await call()
    except LimitExceeded:
        raise
    except Exception as exc:
        out = {"error": type(exc).__name__}
    await emit(config, "TOOL_EXECUTED", {"tool": name, "toolVersion": "1", "agentType": "RAG",
                                        "mcpServer": server, "input": {}, "output": out})
    return {"source": "INTERPRETATION", "attachmentId": None, "found": False,
            "output": {"tool": name, **out}}


# ── 3. MCP 고르기 + 호출 (동시에) ────────────────────────


def pick_mcps(state: State) -> list[str]:
    """부를 사진 MCP 노드 목록. 여러 개면 LangGraph가 동시에 실행한다. 사진 MCP가 없으면 바로 rag로."""
    kinds = {p["kind"] for p in state["photos"]}
    picked = []
    if "PCB" in kinds:
        picked.append("pcb_mcp")
    if "MECH" in kinds:
        picked.append("mech_mcp")
    return picked or ["rag_mcp"]


async def run_photo_mcp(state, config, kind: str, call, tool: str, agent_type: str, server: str) -> dict:
    """kind 사진마다 MCP를 부르고, Spring에 TOOL_EXECUTED를 알린다. pcb_mcp·mech_mcp가 같이 쓴다."""
    findings = []
    for p in [p for p in state["photos"] if p["kind"] == kind]:
        try:
            out = await call(p["path"])
        except LimitExceeded:
            raise
        except Exception as e:  # MCP 꺼짐 등. 멈추지 않고 '못 찾음'으로 넘긴다
            out = {"error": type(e).__name__}
        await emit(config, "TOOL_EXECUTED", {"tool": tool, "toolVersion": "1", "agentType": agent_type,
                                             "mcpServer": server, "input": {"path": p["path"]}, "output": out})
        findings.append({"source": kind, "attachmentId": p["attachmentId"], "found": bool(out.get("is_defective")), "output": out})
    if kind == "PCB":
        labels = sorted({d["type"] for f in findings for d in f["output"].get("defects", [])})
        if labels:
            findings.append(await supplemental(config, "interpret_pcb", "pcb-mcp",
                                                lambda: enrichment.interpret_pcb(labels)))
    else:
        # 일반 결함명은 유지하고, 모델이 반환한 원래 부위 라벨로 추가 해석한다.
        labels = [label for f in findings for label in enrichment.dent_labels(f["output"])]
        labels = [{"damage_type": kind, "part": part}
                  for kind, part in dict.fromkeys((v["damage_type"], v["part"]) for v in labels)]
        if labels:
            findings.append(await supplemental(config, "interpret_damage", "damage-mcp",
                                                lambda: enrichment.interpret_damage(labels)))
    return {"findings": findings}


async def pcb_mcp(state: State, config) -> dict:
    return await run_photo_mcp(state, config, "PCB", tools.call_pcb_mcp, "analyze_pcb_image", "PCB", "pcb-detector")


async def mech_mcp(state: State, config) -> dict:
    return await run_photo_mcp(state, config, "MECH", tools.call_mech_mcp, "detect_surface_defects", "MOTOR", "mechanical-mcp")


async def rag_mcp(state: State, config) -> dict:
    """사진 MCP가 끝난 뒤 실행: 증상 글 + PCB가 찾은 라벨로 과거 사례 검색."""
    codes = state["clues"]["errorCodes"]
    text = state["text"] + (f" 에러코드 {', '.join(codes)}" if codes else "")
    pcb_labels = sorted({d["type"] for f in state.get("findings", []) if f["source"] == "PCB"
                         for d in f["output"].get("defects", [])})
    try:
        out = await tools.call_rag_mcp(text, pcb_labels)
    except LimitExceeded:
        raise
    except Exception as e:  # RAG 서버 꺼짐 등. 검색 없이 진행
        out = {"hits": [], "error": type(e).__name__}
    await emit(config, "TOOL_EXECUTED", {"tool": "retrieve_repair_context", "toolVersion": "1", "agentType": "RAG",
                                         "mcpServer": "rag-mcp", "input": {"text": text, "pcbLabels": pcb_labels}, "output": out})
    return {"findings": [{"source": "RAG", "attachmentId": None, "found": False, "output": out}]}


# ── 4. 판단 ──────────────────────────────────────────────


def decide(findings: list[dict], photo_round: int) -> str:
    """근거가 충분한가? TODO(오케스트레이션): GPT 판단으로 바꿀 수 있다."""
    if any(f["found"] for f in findings):
        return "COMPLETE"  # 사진에서 결함을 하나라도 찾음
    if photo_round < C.MAX_PHOTO_ROUNDS:
        return "PHOTO"  # 못 찾음 + 아직 요청 가능
    return "FAIL"  # 3번 요청했는데도 못 찾음


async def judge(state: State, config) -> dict:
    await emit(config, "DIAGNOSIS_PROGRESS", {"stage": "JUDGING"})
    return {"decision": decide(state["findings"], state["command"]["payload"]["round"])}


# ── 5. 끝: 리포트 / 사진 요청 / 실패 ─────────────────────


async def report(state: State, config) -> dict:
    parts = state["command"]["payload"]["repair"].get("product_snapshot", {}).get("compatibleParts", [])
    r = await tools.write_report(state["text"], state["findings"], parts)
    candidates = [{"rank": i + 1, **c.model_dump()} for i, c in enumerate(r.candidates)]
    evidence = []  # 근거는 코드가 MCP 결과에서 직접 만든다 (GPT가 사진 번호를 지어내지 않게)
    for f in state["findings"]:
        if f["found"]:
            e = {"type": "PHOTO", "annotation": {"defects": f["output"]["defects"], "image": f["output"]["annotated_image"]}}
            if f["output"].get("dent_image"):  # 기계 사진은 찌그러짐 박스 사진이 하나 더 있다
                e["annotation"]["dentImage"] = f["output"]["dent_image"]
            if f["attachmentId"]:  # 예시 사진은 Spring 첨부가 아니라 번호가 없다
                e["attachmentId"] = f["attachmentId"]
            evidence.append(e)
        if f["source"] == "RAG":
            evidence += [{"type": "MANUAL" if h["kind"] in ("LABEL", "REFERENCE") else "CASE",
                          **{k: v for k, v in h.items() if k != "kind"}} for h in f["output"]["hits"]]
    payload = {"summary": r.summary, "modelVersion": C.MODEL_VERSION, "repairMethod": r.repairMethod,
               "candidates": candidates, "recommendedParts": [p.model_dump() for p in r.recommendedParts],
               "evidence": evidence}
    return {"result": {"eventType": "DIAGNOSIS_COMPLETED", "payload": payload}}


async def request_photo(state: State, config) -> dict:
    cmd = state["command"]
    payload = {"round": cmd["payload"]["round"] + 1, "bodyPart": "wheel",  # TODO: GPT가 부위·설명을 고른다
               "description": "결함 부위를 가까이에서 밝게 다시 찍어 주세요", "checkpoint": f"task-{cmd['taskId']}"}
    return {"result": {"eventType": "PHOTO_REQUESTED", "payload": payload}}  # requestKey는 main.py가 붙인다


async def fail(state: State, config) -> dict:
    return {"result": {"eventType": "DIAGNOSIS_FAILED", "payload": {"errorCode": "INCONCLUSIVE_AFTER_THIRD_PHOTO"}}}


# ── 조립 ────────────────────────────────────────────────


def build_graph():
    g = StateGraph(State)
    for fn in (classify_photos, split_text, pcb_mcp, mech_mcp, rag_mcp, judge, report, request_photo, fail):
        g.add_node(fn.__name__, observed(fn))

    g.add_edge(START, "classify_photos")
    g.add_edge("classify_photos", "split_text")
    g.add_conditional_edges("split_text", pick_mcps, ["pcb_mcp", "mech_mcp", "rag_mcp"])  # 여기서 갈라져서 동시 실행
    for mcp in ("pcb_mcp", "mech_mcp"):
        g.add_edge(mcp, "rag_mcp")  # 사진 MCP가 다 끝나면 rag에서 합류 (PCB 라벨을 넘기려고)
    g.add_edge("rag_mcp", "judge")
    g.add_conditional_edges("judge", lambda s: s["decision"], {"COMPLETE": "report", "PHOTO": "request_photo", "FAIL": "fail"})
    for end in ("report", "request_photo", "fail"):
        g.add_edge(end, END)
    return g.compile()
