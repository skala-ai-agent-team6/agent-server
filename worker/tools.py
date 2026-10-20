"""그래프가 쓰는 '팔다리': 사진 받기, MCP 호출, GPT 호출. graph.py는 이 함수들만 부른다."""

import base64
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Literal

import httpx
from fastmcp import Client
from fastmcp.client.transports import StdioTransport
from pydantic import BaseModel, Field, field_validator

from . import config as C
from .harness import guarded
from .tool_policy import check_image_result, check_object, check_response

# ── 1. 사진 ──────────────────────────────────────────────


async def load_photos(command: dict) -> list[dict]:
    """접수 사진만 내려받는다. 누락·다운로드 오류를 예시 사진으로 대체하지 않는다."""
    folder = Path(tempfile.mkdtemp(prefix=f"task{command['taskId']}_"))
    photos = []
    async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
        for a in command["payload"]["attachments"]:
            if a["downloadPath"] != f"/internal/attachments/{a['id']}":
                raise ValueError("Invalid attachment download path")
            content = bytearray()
            async with client.stream("GET", C.SPRING_URL.rstrip("/") + a["downloadPath"],
                                     headers={"Authorization": f"Bearer {C.SPRING_TOKEN}"}) as r:
                r.raise_for_status()
                async for chunk in r.aiter_bytes():
                    content.extend(chunk)
                    if len(content) > min(a["byte_size"], 20_000_000):
                        raise ValueError("Attachment exceeds expected size")
            if len(content) != a["byte_size"] or hashlib.sha256(content).hexdigest() != a["sha256"]:
                raise ValueError("Attachment size or checksum mismatch")
            path = folder / f"{a['id']}.jpg"
            path.write_bytes(content)
            photos.append({"attachmentId": a["id"], "path": str(path), "target": a.get("target")})
    if not photos:
        raise ValueError("No intake photos supplied")
    return photos


# ── 2. MCP ───────────────────────────────────────────────


@guarded(weight=2)
async def call_mech_mcp(path: str) -> dict:
    """mechanical-mcp: 표면 결함(scratches, patches 등) + 찌그러짐(dent, breakage)을 둘 다 보고 합친다."""
    async with Client(C.MECH_MCP_URL) as c:
        surface = check_image_result(check_response(
            await c.call_tool("detect_surface_defects", {"photo_id": path}), "detect_surface_defects").data,
            "detect_surface_defects")
        dent = check_image_result(check_response(
            await c.call_tool("detect_dents", {"photo_id": path}), "detect_dents").data, "detect_dents")
    return {
        "is_defective": surface["is_defective"] or dent["is_defective"],
        "defects": surface["defects"] + dent["defects"],
        "defect_counts": {**surface.get("defect_counts", {}), **dent.get("defect_counts", {})},
        "annotated_image": surface["annotated_image"],
        "dent_image": dent["annotated_image"],
        "damage_labels": surface.get("damage_labels", []) + dent.get("damage_labels", []),
    }


@guarded()
async def call_pcb_mcp(path: str) -> dict:
    """pcb-detector (MCP_Error_check/pcb_agent): PCB 결함 (short, missing_hole 등)."""
    transport = StdioTransport(command=C.PCB_MCP_PYTHON, args=[C.PCB_MCP_SERVER], log_file=C.HERE / "pcb_mcp.log")
    # 옛 서버가 내는 경고(server/discover 모름)는 터미널 대신 로그 파일로. 동작엔 문제없음
    async with Client(transport) as c:
        r = await c.call_tool("analyze_pcb_image", {"image_path": path})
        check_response(r, "analyze_pcb_image")
        return check_image_result(json.loads(r.content[0].text), "analyze_pcb_image")
        # 옛 mcp 1.x 서버라 결과가 글(JSON 문자열)로 온다


@guarded()
async def call_rag_mcp(text: str, pcb_labels: list[str]) -> dict:
    """Rag_mcp(retrieve_repair_context): 증상 글 + PCB 라벨로 과거 수리 사례·라벨 정의를 찾는다.

    찌그러짐 라벨은 보내지 않는다. RAG는 원본 자동차 라벨을 받아 "자동차라 대응 안 됨" 경고를 붙이는데,
    그 경고가 GPT에 들어가면 결과를 무시해 버린다. 찌그러짐은 기계 MCP 결과로만 리포트에 넣는다.
    """
    request = {"raw_text": text, "origin": "technician", "technician_confirmed": True,
               "pcb_labels": pcb_labels, "damage_labels": [], "top_k": 3}
    async with Client(C.RAG_MCP_URL, timeout=60) as c:
        d = check_object(check_response(
            await c.call_tool("retrieve_repair_context", {"request": request}),
            "retrieve_repair_context").structured_content, "retrieve_repair_context")
    hits = [{"kind": "CASE", "citation": x["source_id"], "sourceUri": x["source_url"],
             "excerpt": (x.get("korean_summary") or x.get("chunk_text") or "")[:300]} for x in d["repair_cases"]]
    hits += [{"kind": "LABEL", "citation": x["document_id"], "excerpt": x["title"]} for x in d["label_definitions"]]
    hits += [{"kind": "REFERENCE", "citation": x["document_id"],
              "excerpt": (x.get("text") or x.get("chunk_text") or x.get("title") or "")[:2000],
              **({"sourceUri": x["source_url"]} if x.get("source_url") else {})}
             for x in d.get("pcb_references", [])]
    return {"hits": hits, "status": d["case_search_status"], "warnings": d.get("warnings", [])}


# ── 3. GPT ───────────────────────────────────────────────


class PhotoKind(BaseModel):
    kind: Literal["PCB", "MECH", "OTHER"] = Field(description="PCB: 회로기판·구리 배선·납땜·전자부품. MECH: 금속 표면 클로즈업·모터·기어·축·바퀴. OTHER: 그 외")


class Clues(BaseModel):
    pcb: list[str] = Field(description="전원·충전·회로 관련 증상")
    mech: list[str] = Field(description="바퀴·모터·브러시·소음 등 기계 증상")
    sw: list[str] = Field(description="펌웨어·앱·연결 등 소프트웨어 증상")


class Candidate(BaseModel):
    cause: str = Field(description="고장 원인 한 줄")
    confidence: float = Field(ge=0, le=1)
    explanation: str = Field(description="근거가 된 MCP 결과를 들어 설명")

    @field_validator("cause", "explanation")
    @classmethod
    def nonblank(cls, value):
        if not value.strip():
            raise ValueError("Candidate text must not be blank")
        return value


class RecommendedPart(BaseModel):
    partId: str = Field(pattern=r"^[1-9][0-9]*$")
    quantity: int = Field(ge=1, le=100)


class Report(BaseModel):
    summary: str
    repairMethod: str
    candidates: list[Candidate] = Field(min_length=1)
    recommendedParts: list[RecommendedPart] = Field(default_factory=list, max_length=20)

    @field_validator("summary", "repairMethod")
    @classmethod
    def nonblank(cls, value):
        if not value.strip():
            raise ValueError("Report text must not be blank")
        return value


def gpt(schema):
    from langchain_openai import ChatOpenAI

    return ChatOpenAI(model=C.GPT_MODEL, temperature=0, timeout=C.TOOL_TIMEOUT, max_retries=0).with_structured_output(schema)


@guarded()
async def classify_photo(path: str) -> str:
    """사진 한 장이 PCB / MECH / OTHER 중 무엇인지."""
    if not C.USE_GPT:
        return "PCB" if "pcb" in Path(path).name else "MECH"  # 키 없을 때: 파일 이름으로
    b64 = base64.b64encode(Path(path).read_bytes()).decode()
    msg = [{"role": "user", "content": [
        {"type": "text", "text": "로봇청소기 수리용 부품 사진이다. 흑백·확대 사진일 수 있다. PCB / MECH / OTHER 중 하나로 분류해라."},
        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
    ]}]
    return (await gpt(PhotoKind).ainvoke(msg)).kind


@guarded()
async def split_text(text: str) -> Clues:
    """증상 문장을 PCB / 기계 / SW 단서로 나눈다."""
    if not C.USE_GPT:
        return Clues(pcb=[w for w in ("전원", "충전", "회로") if w in text],
                     mech=[w for w in ("바퀴", "모터", "브러시", "소음") if w in text],
                     sw=[w for w in ("펌웨어", "앱", "업데이트") if w in text])
    return await gpt(Clues).ainvoke(f"로봇청소기 고장 증상을 단서별로 나눠라. 없으면 빈 목록.\n증상: {text}")


@guarded()
async def write_report(text: str, findings: list[dict], compatible_parts=None) -> Report:
    """MCP 결과를 근거로 진단 리포트를 쓴다."""
    defects = [d for f in findings for d in f["output"].get("defects", [])]
    if not C.USE_GPT:
        top = max(defects, key=lambda d: d["confidence"])
        return Report(summary=f"{top['type']} 결함 추정", repairMethod="해당 부품 점검 후 교체",
                      candidates=[Candidate(cause=top["type"], confidence=top["confidence"], explanation="MCP 검출 결과")])
    result = await gpt(Report).ainvoke(
        "로봇청소기 수리 기사에게 줄 진단 리포트를 한국어로 써라. 후보는 확신도 높은 순.\n"
        "MCP 결과는 전부 이 로봇청소기 부품 사진을 검사한 결과다. 무시하지 말고 근거로 써라.\n"
        "단, 해석 MCP가 도메인 불일치로 표시한 자동차 부위는 청소기의 부위·고장 원인으로 제시하지 마라. "
        "모델 검출 점수나 RAG 유사도는 실제 고장 확률이 아니다. 원인 확정에는 추가 점검이 필요하다고 명시하라.\n"
        "추천 부품은 제공된 호환 부품의 id만 사용하라. 목록이 없거나 교체 근거가 없으면 빈 배열.\n"
        f"호환 부품: {compatible_parts or []}\n증상: {text}\nMCP 결과: {findings}")
    allowed = {str(p["id"]) for p in compatible_parts or []}
    identifiers = [p.partId for p in result.recommendedParts]
    if not set(identifiers) <= allowed or len(set(identifiers)) != len(identifiers):
        raise ValueError("Unknown or duplicate recommended part")
    return result
