"""Rag_mcp의 해석 도구를 덧붙인다. 이미지 분석 결과·판단 규칙은 바꾸지 않는다."""
import json
from fastmcp import Client

from . import config as C
from .harness import guarded
from .tool_policy import check_object, check_response

DENT_PARTS = frozenset(("boot", "doorouter", "fender", "front-bumper", "quaterpanel", "roof"))


def dent_labels(output):
    """명시된 6종 라벨만 전달. 일반 dent를 특정 부위로 추측하지 않는다."""
    parts = []
    for label in output.get("damage_labels", []):
        if label.get("damage_type") == "dent":
            parts.append(label.get("part"))
    for defect in output.get("defects", []):
        raw = defect.get("raw_label") or defect.get("type", "")
        parts.append(raw.removesuffix("-dent"))
    return [{"damage_type": "dent", "part": p} for p in dict.fromkeys(parts) if p in DENT_PARTS]


@guarded(name_arg="name")
async def call(url, name, arguments):
    async with Client(url, timeout=C.TOOL_TIMEOUT) as client:
        response = await client.call_tool(name, arguments)
    check_response(response, name)
    data = response.structured_content
    if data is None:
        data = json.loads("".join(block.text for block in response.content if hasattr(block, "text")))
    return check_object(data, name)


async def normalize_observation(text):
    data = await call(C.OBSERVATION_MCP_URL, "normalize_observation",
                      {"raw_text": text, "origin": "technician", "technician_confirmed": True})
    if data.get("raw_text") != text or not isinstance(data.get("observations"), list):
        raise ValueError("Invalid observation response")
    for item in data["observations"]:
        evidence = item.get("evidence_text")
        if not evidence or evidence not in text:
            raise ValueError("Observation evidence is absent from original text")
    return data


async def interpret_pcb(labels):
    data = await call(C.PCB_INTERPRET_MCP_URL, "interpret_pcb", {"labels": labels})
    if not isinstance(data.get("detections"), list):
        raise ValueError("Invalid PCB interpretation")
    return data


async def interpret_damage(labels):
    # dent/breakage만으로 bonnet/roof 등의 부위를 만들어 보내지 않는다.
    data = await call(C.DAMAGE_MCP_URL, "interpret_damage", {"labels": labels})
    if not isinstance(data.get("detections"), list):
        raise ValueError("Invalid damage interpretation")
    return data


async def list_observation_terms():
    return await call(C.OBSERVATION_MCP_URL, "list_observation_terms", {})


async def list_damage_labels():
    return await call(C.DAMAGE_MCP_URL, "list_damage_labels", {})


async def search_reference_documents(query, collection="pcb_documents"):
    return await call(C.RAG_MCP_URL, "search_reference_documents",
                      {"query": query, "collection": collection, "top_k": 3, "min_score": 0.75})
