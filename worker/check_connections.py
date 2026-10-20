"""실제 해석 MCP 7개 도구 연결 확인. 제공 라벨 테스트이며 이미지 추론은 아니다."""
import asyncio
import json
from . import config as C, enrichment, tools


async def run():
    results = {}
    calls = [
        ("list_observation_terms", enrichment.list_observation_terms()),
        ("list_damage_labels", enrichment.list_damage_labels()),
        ("normalize_observation", enrichment.normalize_observation("전원이 켜지지 않고 바퀴에서 소음이 납니다.")),
        ("interpret_pcb", enrichment.interpret_pcb(["open_circuit"])),
        ("interpret_damage", enrichment.interpret_damage(
            [{"damage_type": "dent", "part": p} for p in sorted(enrichment.DENT_PARTS)])),
        ("retrieve_repair_context", tools.call_rag_mcp("전원이 켜지지 않아요.", ["open_circuit"])),
        ("search_reference_documents", enrichment.search_reference_documents("PCB open circuit repair")),
    ]
    values = await asyncio.gather(*(c for _,c in calls), return_exceptions=True)
    for (name,_), value in zip(calls, values):
        results[name] = {"error": type(value).__name__} if isinstance(value, BaseException) else value
        print(name, "FAILED" if isinstance(value, BaseException) else "OK", flush=True)
    folder = C.HERE/"reports"
    folder.mkdir(exist_ok=True)
    (folder/"mcp-connections.json").write_text(json.dumps(
        {"mode": "real-mcp-provided-labels-not-image-inference", "calls": results},
        ensure_ascii=False, indent=2))
    if any(isinstance(v, BaseException) for v in values):
        raise RuntimeError("MCP connection check failed; see mcp-connections.json")


if __name__ == "__main__":
    asyncio.run(run())
