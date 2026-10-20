"""진단 결과를 사람이 읽는 '고장 리포트.md'로 저장한다. (Spring에 보내는 JSON과 같은 내용)

저장 위치: worker/reports/task-{taskId}_revision-{revision}_고장리포트.md/json
"""

from datetime import datetime
from pathlib import Path
import json

from . import config as C

OUT = C.HERE / "reports"
TITLE = {"DIAGNOSIS_COMPLETED": "진단 완료", "PHOTO_REQUESTED": "추가 사진 요청", "DIAGNOSIS_FAILED": "진단 실패"}


def save(command: dict, result: dict, execution=None) -> Path:
    repair = command["payload"]["repair"]
    p = result["payload"]
    lines = [
        f"# 고장 리포트: 접수 {command['repairId']} ({TITLE[result['eventType']]})",
        "",
        f"- 작성: {datetime.now():%Y-%m-%d %H:%M} · 모델 {p.get('modelVersion', C.MODEL_VERSION)}",
        f"- 증상: {repair.get('symptom') or '-'}",
        f"- 기사 설명: {repair.get('user_input', {}).get('customerDescription') or '-'}",
        f"- 에러코드: {', '.join(repair.get('error_codes') or []) or '-'} · 펌웨어 {repair.get('firmware_version') or '-'}",
        "",
    ]

    if result["eventType"] == "DIAGNOSIS_COMPLETED":
        lines += ["## 결론", "", p["summary"], "", "## 수리 방법", "", p["repairMethod"], "",
                  "## 원인 후보", "", "| 순위 | 원인 | 확신도 | 설명 |", "|---|---|---|---|"]
        lines += [f"| {c['rank']} | {c['cause']} | {c['confidence']:.2f} | {c['explanation']} |" for c in p["candidates"]]
        lines += ["", "## 근거", ""]
        for e in p["evidence"]:
            if e["type"] == "PHOTO":
                a = e["annotation"]
                found = ", ".join(f"{d['type']} {d['confidence']:.2f}" for d in a["defects"])
                lines += [f"### 사진 {e.get('attachmentId', '(예시 사진)')}", "", f"검출: {found}", "",
                          f"![{Path(a['image']).name}]({Path(a['image']).as_uri()})", ""]
                if a.get("dentImage"):
                    lines += [f"![{Path(a['dentImage']).name}]({Path(a['dentImage']).as_uri()})", ""]
            else:
                lines += [f"- {e['type']}: {e.get('citation', '')} {e.get('excerpt', '')}"]
        lines += ["", "## 추천 부품", "", ", ".join(f"{x['partId']} ×{x['quantity']}" for x in p["recommendedParts"]) or "추천 부품 없음"]

    elif result["eventType"] == "PHOTO_REQUESTED":
        lines += ["## 추가 사진 요청", "", f"- {p['round']}회차 / 최대 {C.MAX_PHOTO_ROUNDS}회",
                  f"- 부위: {p['bodyPart']}", f"- 요청: {p['description']}"]

    else:
        lines += ["## 진단 실패", "", f"- 에러: {p['errorCode']}", "- 접수는 수동 검토로 넘어간다"]
        if p["errorCode"] == "REQUIRED_TOOL_FAILED":
            lines += ["- 필수 이미지 분석 도구가 실패했습니다. 결함 없음이나 정상 판정이 아닙니다.",
                      "- 도구 상태를 복구한 뒤 재진단이 필요합니다."]
        if p["errorCode"] == "PROMPT_CONFIG_ERROR":
            lines += ["- 프롬프트 파일·버전 정보가 유효하지 않아 분석을 시작하지 않았습니다."]
        limits = {
            "TOOL_CALL_LIMIT": "실행 전체의 도구 호출 예산을 초과했습니다.",
            "TOOL_REPEAT_LIMIT": "동일 도구·동일 입력의 호출 시도 한도를 초과했습니다.",
            "NODE_EXECUTION_LIMIT": "그래프 노드 실행 횟수 한도를 초과했습니다.",
            "GRAPH_STEP_LIMIT": "LangGraph super-step 한도를 초과했습니다.",
            "RUN_TIMEOUT": "이벤트 전체 실행 시간 한도를 초과했습니다.",
            "TOOL_TIMEOUT": "개별 도구 호출 시간 초과가 재시도 후에도 해결되지 않았습니다.",
        }
        if p["errorCode"] in limits:
            lines += ["- " + limits[p["errorCode"]], "- 실행 제한에 따른 종료이며 정상 판정이 아닙니다."]

    if execution and execution.get("prompts"):
        lines += ["", "## 실행에 사용한 프롬프트", "",
                  "| 단계 | 버전 | SHA-256 | 모델 |", "| --- | --- | --- | --- |"]
        for name, meta in sorted(execution["prompts"].items()):
            lines.append(f"| {name} | {meta['version']} | {meta['sha256']} | {meta['model']} |")
    if execution and execution.get("limits"):
        limits = execution["limits"]
        lines += ["", "## 실행 한도 확인", "",
                  f"- 가중 호출 수: {execution['calls']} / {limits['weighted_calls']}",
                  f"- 노드 실행 수: {execution['node_executions']} / {limits['node_executions']}",
                  f"- 동일 입력 최대 시도: {execution['max_same_input_attempts_used']} / {limits['same_input_attempts']}",
                  f"- 실행 시간: {execution['duration_ms'] / 1000:.3f}초 / {limits['run_seconds']}초"]
        for hit in execution.get("limits_hit", []):
            lines.append(f"- 초과한 한도: {hit['code']}")

    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"task-{command['taskId']}_revision-{command['inputRevision']}_고장리포트.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.with_suffix(".json").write_text(json.dumps(
        {"command": command, "result": result, "execution": execution or {}},
        ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    return path
