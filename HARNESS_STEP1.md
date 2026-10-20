# 하네스 1단계: 도구 실패 처리

- 작업 브랜치: `feat/harness-hardening`
- 기준 커밋: `3517604`
- 검증일: 2026-10-20
- 상태: 사용자 승인 후 `6faafbb`로 `agent-server/feat/harness-hardening`에 푸시 완료.

## 변경 목적

기존에는 이미지 MCP 장애나 오류 응답이 `found=False`로 바뀌어 추가 사진을 요청하거나,
다른 사진에서 결함을 찾으면 정상 리포트를 만들 수 있었다. 도구 장애와 정상적인 검출 없음을 구분한다.

## 처리 정책

| 구분 | 해당 도구 | 처리 |
| --- | --- | --- |
| 필수 이미지 분석 | 선택된 PCB `analyze_pcb_image`, 기계 `detect_surface_defects`·`detect_dents` | 일시 오류는 추가 2회 재시도. 소진·논리 오류·잘못된 결과는 `REQUIRED_TOOL_FAILED`로 종료, 실패 JSON·Markdown 저장 |
| 보조 해석 | `normalize_observation`, `interpret_pcb`, `interpret_damage` | 기존 진행 정책 유지. 오류 이벤트 기록, 리포트 요약 또는 추가 사진 안내에 근거 제한 명시 |
| 보조 검색 | `retrieve_repair_context` | 실패 시 검색 근거를 비우고 근거 제한 명시. 성공한 검색에 결과가 없는 것과 구분 |
| 리포트 LLM | `write_report` | 기존 재시도·출력 검증 유지. 최종 실패 시 `AGENT_ERROR`와 실패 리포트 저장 |
| 실행 상한 | 그래프·호출·시간 한도 | 기존 별도 실패 처리 유지. 보조 도구에서도 상한 오류는 숨기지 않음 |

필수 이미지 분석은 어느 한 장 또는 기계의 두 분석 중 하나라도 실패하면 전체 진단을 실패 처리한다.
정상 응답의 검출 없음은 이전처럼 추가 사진 요청 분기로 간다. 검출 여부·확신도 임계값 자체는 바꾸지 않았다.

보조 도구 실패 시 `summary`/요청 `description`에 코드로 문구를 덧붙이므로 LLM이 이를 생략해도 표시된다.
최종 이벤트 종류·필수 JSON 필드는 기존 계약 그대로다. 도구 이벤트의 `output`에는 오류 종류와
`failurePolicy`, `errorCode`, `tool`을 기록하고 외부 예외 원문은 넣지 않는다.

FastMCP의 접속 오류는 `RuntimeError` 내부의 `httpx2.ConnectError`로 올 수 있다.
명시적 원인과 예외 그룹을 검사하여 일시 오류만 재시도한다. 401, 형식 오류, 일반 RuntimeError는 반복하지 않는다.
`httpx2`는 기존 lock에 이미 설치된 버전을 직접 의존성으로 명시했으며 패키지 버전 업그레이드는 없다.

## 유지한 부분

- `build_graph`는 통합 기준 커밋과 동일하다.
- `decide`, `pick_mcps`, `judge`는 스켈레톤 기준 커밋과 동일하다.
- 노드 9개, 병렬 분기, RAG 실행 위치, 사진별 분석, 추가 사진 최대 3회를 유지한다.
- Kafka 토픽·백엔드 계약·접수 10장 제한은 수정하지 않았다.
- MCP 서버 코드·모델·라벨 매핑은 수정하지 않았다.
- 다른 하네스 단계나 진단 충분성 판단 개선은 이번에 구현하지 않았다.

## 검증

자동 테스트 **59개 통과**. 이 테스트들은 외부 서비스를 대역으로 바꾼다.
원래 35개 테스트 외에 필수/보조 오류, 혼합 분석 일부 실패, 잘못된 결과,
실패 결과 재전송, 감싸진 연결 오류 재시도, 오류 시 리포트 생성·장애 검증 스크립트 검사를 추가했다.

실제 호출 검증은 다음과 같다. Spring 다운로드와 Kafka 전송은 로컬 입출력으로 대체했으며,
이번 단계에서 Spring 저장·SSE까지의 전체 E2E를 다시 실행한 것은 아니다.

| 검증 | 결과 | 리포트 (`worker/reports/`) |
| --- | --- | --- |
| 수정 전 실제 혼합 진단 | 완료 | `task-19001281_revision-1_고장리포트.md/json` |
| 수정 후 실제 PCB+기계 혼합 진단 | 완료, 도구 오류 없음 | `task-59464880_revision-1_고장리포트.md/json` |
| 수정 후 실제 기계 단독 진단 | 완료, 도구 오류 없음 | `task-47950235_revision-1_고장리포트.md/json` |
| 기계 MCP 접속 불가 재현 | 추가 재시도 2회 후 `REQUIRED_TOOL_FAILED`, 사진 재요청 없음 | `task-93706232_revision-1_고장리포트.md/json` |
| RAG 접속 불가 재현 | 추가 재시도 2회 후 근거 제한을 표시한 완료 리포트, 검색 근거 없음 | `task-88671194_revision-1_고장리포트.md/json` |

장애 재현은 실행 중인 팀 서버를 끄지 않고, **검증 프로세스의 해당 MCP 주소만** `http://127.0.0.1:1/mcp`로 변경했다.
따라서 장애 대상 도구는 실제 모델을 실행하지 못하며, 나머지 경로에서는 실제 모델·MCP·LLM을 사용한다.
파일이 존재하는 것뿐 아니라 저장된 JSON, 이벤트 스키마, 오류 코드, 경고 문구, 재시도 통계도 검사했다.
리포트 생성 검증은 진단 정확도 평가를 의미하지 않는다.

## 재현 명령

각 MCP 서버와 OpenAI 설정을 준비한 후 저장소 루트에서 실행한다.

```bash
uv run python -m pytest -q
uv run python -m worker.verify_live --scenario mixed
uv run python -m worker.verify_live --scenario mechanical
uv run python -m worker.verify_live --scenario mixed --fault mechanical-unavailable
uv run python -m worker.verify_live --scenario mixed --fault rag-unavailable
open worker/reports
```

정상 실행 결과는 `live-summary.json`, 장애 검증 결과는 `live-<fault>-summary.json`에 저장된다.
각 실행의 Markdown·JSON·이벤트 파일은 작업 ID별로 남는다. 생성물은 Git에서 제외한다.

## 남은 확인 및 다음 단계

- 새 오류 코드와 보조 근거 제한 표시를 실제 Spring·프론트에서도 확인하는 전체 E2E는 별도 필요하다.
- MCP 서버가 내부 서비스 오류를 일반 도구 오류 문자열로만 내보내면 원래 HTTP 상태를 알 수 없어 무조건 재시도하지 않는다.
- 리포트 LLM 오류 세분화, 모델 대체, 진단 충분성·특정 부위 재요청 개선은 이번 범위에 포함하지 않았다.
- 사용자 확인을 받아 2단계 프롬프트 분리·버전 관리로 진행했다. 이후 단계·추가 푸시는 다시 확인 후 진행한다.
