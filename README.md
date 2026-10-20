# AI Agent Capstone AGENT SERVER

로봇청소기 수리 접수의 구분된 사진과 기사 소견을 받아 분석 도구를 실행하고, 추가 자료 요청 또는 진단 결과 JSON을 생성하는 Agent 서버다.

**현재는 스켈레톤 통합 전 기준 코드다.** 실제 PCB MCP를 연결했고, 기계 분석·RAG는 모의 도구이며 최종 리포트는 템플릿이다. Kafka·Spring 사진 다운로드·실제 LLM은 아직 연결하지 않았다.

## 구현 상태

| 기능 | 현재 상태 |
|---|---|
| 입력 검증·라우팅 | PCB/MECHANICAL 사진 구분을 사용. GPT 사진 재분류·SW 분석 분기 없음 |
| 분석 실행 | 해당 사진의 분석 도구와 RAG를 같은 단계에서 병렬 실행 |
| PCB 분석 | 실제 MCP stdio → YOLO. 응답·결함 라벨·점수·좌표 검증 |
| 기계 분석·RAG | 모의 도구. 실제 찌그러짐 분석·검색 품질을 검증한 상태가 아님 |
| 충분성 판단 | sufficient 및 0.5 임계값을 사용한 임시 규칙 |
| 추가 자료 | 초기 입력 제외 최대3회. 요청 후 그래프 END, 새 이벤트로 재분석 |
| 상태·중복 | SQLite 저장, 이벤트 본문 충돌·차수·누적 사진 검증, 재시작 후 중복 방지 |
| 출력 | Pydantic 검증 리포트 JSON·로컬 결과 이벤트. Markdown 리포트 생성은 미구현 |
| 기본 Harness | 도구 호출 상한·일시 오류 재시도·PCB 세션 timeout·recursion_limit·구조화 로그 |
| Spring v2 | 계약 변환·스키마 검증. 미합의 입력/추가 사진 계약은 명시적으로 거부 |

## 빠른 실행 — 모의 도구

Python 3.12 이상과 uv를 사용한다. API 키·외부 서버 없이 워크플로를 검증한다.

```bash
uv sync
uv run python -m app.demo --scenario additional --log-file .local/demo.jsonl
uv run python -m app.demo --scenario all --log-file .local/demo.jsonl
```

시나리오: `success`, `additional`, `limit`, `tool-error`, `duplicate`, `v2-blocked`, `all`.
`limit`/`tool-error`의 FAILED와 `v2-blocked`의 BLOCKED_EXPECTED는 의도한 결과다. 기대 동작과 다르면 명령이 실패한다.
각 시연은 임시 SQLite를 사용하며 기존 작업 상태를 변경하지 않는다. 모의 실행 로그는 `mode=mock`이다.

로컬 API:

```bash
AGENT_LOG_FILE=.local/server.jsonl \
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
```

별도 터미널에서:

```bash
curl -X POST http://127.0.0.1:8000/internal/events \
  -H 'Content-Type: application/json' -d @examples/diagnosis.json
curl -X POST http://127.0.0.1:8000/internal/events \
  -H 'Content-Type: application/json' -d @examples/additional-input.json
curl http://127.0.0.1:8000/internal/diagnoses/demo-1
curl http://127.0.0.1:8000/internal/outgoing-events
tail -f .local/server.jsonl
```

API 문서: http://127.0.0.1:8000/docs. API 인증은 아직 없으므로 localhost에서 실행한다.
기본 상태 파일은 `.local/agent-state.sqlite3`이며 `AGENT_STATE_PATH`로 변경할 수 있다.
동일 이벤트를 재전송하면 저장된 결과를 반환한다. 새 작업에는 새 task_id/event_id를 사용한다.

## 실제 PCB MCP 실행

[MCP_Error_check의 pcb_agent](https://github.com/skala-ai-agent-team6/MCP_Error_check/tree/883fe6178e65bb5a3366f2a1ff8e30cf0c55551d/pcb_agent)를 사용한다.
프로토콜 순서는 `initialize → list_tools → analyze_pcb_image`다. Agent에서 YOLO를 직접 호출하지 않는다.

```bash
uv sync --extra pcb
# 최초 한 번. 이미 내려받았다면 clone 생략
mkdir -p .local/vendor
git clone https://github.com/skala-ai-agent-team6/MCP_Error_check.git .local/vendor/MCP_Error_check
git -C .local/vendor/MCP_Error_check checkout 883fe6178e65bb5a3366f2a1ff8e30cf0c55551d
uv run --extra pcb python -m app.pcb_demo
```

내 사진은 `--image /절대경로/사진.jpg`로 지정한다. API 키는 필요 없다.

- `.local/pcb-live-result.json`: 전체 결과·결함 박스·박스 이미지 경로.
- `.local/pcb-live.jsonl`: MCP 호출·결함 개수·점수·분기·커밋 로그.
- 실제 PCB와 모의 기계/RAG가 섞인 응답·로그는 `mode=mixed`다.
- stdio 프로세스는 호출마다 시작·종료한다. 최초 모델 로딩은 느릴 수 있다.
- 시연/기본 API 설정의 세션 timeout은35초, 일시 오류는 최대2회 재시도한다.
- 미등록 사진·잘못된 응답은 실패 처리하며 모의 성공으로 대체하지 않는다.

검증 샘플 `l_light_05_missing_hole_01_4_600.jpg`에서는 missing_hole 3개를 검출했다. 박스 점수는 0.953/0.895/0.627이었다.
박스 점수 최솟값을 임시 분기 값으로 사용하며 **고장 원인 확률이 아니다**. 미검출도 정상으로 확정하지 않는다.
SUCCEEDED는 현재 개발 규칙의 흐름 완료를 뜻하며 실제 수리 원인 확정·모델 품질 평가 완료를 뜻하지 않는다.

실제 PCB API는 서버 관리용 photo_id → 로컬 파일 매핑을 설정한다. 요청 본문에서 임의 파일 경로를 받지 않는다.

```bash
uv run --extra pcb python - <<'PY'
import json
from pathlib import Path
image = Path('.local/vendor/MCP_Error_check/pcb_agent/results/test_before/l_light_05_missing_hole_01_4_600.jpg').resolve()
Path('.local/pcb-photos.json').write_text(json.dumps({'pcb-demo-photo': str(image)}))
PY
AGENT_TOOL_MODE=pcb-live PCB_PHOTO_MANIFEST=.local/pcb-photos.json \
AGENT_STATE_PATH=.local/pcb-api.sqlite3 AGENT_LOG_FILE=.local/pcb-api.jsonl \
uv run --extra pcb uvicorn app.main:app --host 127.0.0.1 --port 8000
```

```bash
curl -s http://127.0.0.1:8000/internal/events -H 'Content-Type: application/json' \
  -d '{"event_id":"pcb-e1","event_type":"DIAGNOSIS_REQUESTED","task_id":"pcb-task1","case_id":"pcb-case1","symptom":"기판 손상 의심","attachments":[{"photo_id":"pcb-demo-photo","target":"PCB"}]}'
```

`PCB_MCP_SCRIPT`/`PCB_MCP_PYTHON`/`PCB_MCP_TIMEOUT`으로 서버·Python·timeout을 변경할 수 있다.
현재는 누적 자료 중 PCB의 마지막 사진만 분석하고 해당 사진 ID를 근거로 남긴다. 사진 여러 장 비교·공유 저장소 업로드는 미구현이다.

## 현재 실행 흐름과 이벤트

```text
로컬 이벤트 API / Spring v2 계약 어댑터
  → 입력·차수·중복 검증
  → 사진 구분에 맞는 분석 선택
  → [PCB 분석 ∥ 기계 분석 ∥ RAG 검색]
  → 전체 결과 수집·충분성 판단
      ├─ 충분: 템플릿 리포트 JSON 검증 → 완료 이벤트 저장 → END
      ├─ 부족 + 추가 요청 가능: 요청 생성·저장 → END
      └─ 오류/한도 초과/3차 추가 분석 후 부족: 실패 이벤트 저장 → END

추가 자료 제출 → 같은 작업 ID·새 이벤트 ID·누적 입력으로 다시 실행
```

`interrupt`/`Command(resume)`는 사용하지 않는다. WAITING_INPUT은 업무 상태이며 그래프는 종료된 상태다.
진단 이후 재고 조회·재고 없을 때 승인 후 발주·최종 수리 확정은 Spring/업무 MCP 담당이다.

로컬 입력 `DiagnosisEvent`:

| 필드 | 규칙 |
|---|---|
| event_id | 재전달에도 유지. 같은 ID의 다른 본문은 거부 |
| event_type | DIAGNOSIS_REQUESTED / ADDITIONAL_INPUT_SUBMITTED |
| task_id / case_id | 같은 작업·접수 유지 |
| round_no | 최초0, 추가1~3. 마지막 요청 차수와 일치 |
| symptom | 기사 자연어 소견. 별도 에러 코드 필수 입력 없음 |
| attachments | photo_id·target(PCB/MECHANICAL)의 누적 목록. 기존 메타데이터 유지 |

로컬 출력은 ADDITIONAL_INPUT_REQUESTED / DIAGNOSIS_COMPLETED / DIAGNOSIS_FAILED다.
`mock_readable`/`mock_confidence`는 모의 결과 제어용이며 실제 PCB 분석에서 사용하지 않는다.

## Spring v2 어댑터

[backend의 계약](https://github.com/skala-ai-agent-team6/backend/tree/63e0f968f76c893ec9554589d07b51a81e0460ef/docs)을 기준으로 한다. 공식 schema 사본은 tests/fixtures에 고정했다.

- `POST /internal/backend-v2/events`: v2 명령 검증·원문 보존·내부 입력 변환.
- decimal string 업무 ID, UUID 이벤트/실행 ID, revision·sequence를 검증한다.
- STARTED → 현재 실행의 TOOL_EXECUTED → COMPLETED/FAILED 결과 목록을 생성·저장한다.
- 중복 전송에는 저장된 결과 UUID·순서를 유지한다. 부품 ID를 지어내지 않으며 recommendedParts는 빈 배열이다.
- `GET /internal/backend-v2/outgoing-events`: 저장된 출력 확인. 실제 Kafka 발행은 하지 않는다.

**현재 연동 제약:** 부위가 GENERAL인 사진은 임의 분류하지 않고 거부한다. PHOTO_REQUESTED의 checkpoint 의미는 합의 전이라 해당 v2 대기 경로는409로 거부하고 저장을 rollback한다. errorCodes/firmware가 원문에 있으면 보존하되 Agent가 값을 만들지 않는다.

```bash
curl -X POST http://127.0.0.1:8000/internal/backend-v2/events \
  -H 'Content-Type: application/json' -d @examples/backend-v2.json
curl http://127.0.0.1:8000/internal/backend-v2/outgoing-events
```

## 실행 제한·로그

| 항목 | 현재 적용 | 한계 |
|---|---|---|
| 추가 자료 요청 | 작업 전체 최대3회, 초기 제외 | 3차 자료로 성공 가능. 4차 요청·입력 거부 |
| 그래프 | 이벤트별20 super-step | LLM 턴·사진 횟수와 다름. 초과 시 GRAPH_STEP_LIMIT |
| 도구 호출 | 작업 누적24회 | 검색·재시도 시도 포함 |
| 일시 오류 재시도 | 호출당 최대2회 | 사진 요청 횟수와 별개 |
| 시간 | PCB 세션 timeout, 분석 단계120초 반환 전후 검사 | 전체 작업의 엄격한 취소·누적 시간 제한은 미구현 |

PCB의 missing_hole/mouse_bite/open_circuit/short/spur/spurious_copper **6종은 결함 클래스 수**이며 실행 한도가 아니다.

JSON 로그는 event_id/task_id/case_id/input_round로 연결한다. v2는 correlationId/executionId/inputRevision도 포함한다.
`tool.started/succeeded/failed/retry`, `mcp.call_started/completed`, `analysis.evaluated`, `graph.branch`, `input.requested`, `event.duplicate/committed/rolled_back`을 확인한다.
도구 성공만으로 저장 성공이 보장되지 않으므로 마지막 커밋/롤백 로그까지 확인한다.
기사 소견 원문·사진 ID 목록·전체 응답·토큰을 Agent 로그에 남기지 않는다. 파일은5MB마다 회전하고 이전3개를 보관한다.
`.local/`의 DB·로그·사진·모델·외부 저장소와 `.env`는 Git에서 제외한다.

## 검증 방법과 완료 기준

```bash
uv run --extra pcb pytest -q
uv run python -m app.demo --scenario all --log-file .local/demo.jsonl
uv run --extra pcb python -m app.pcb_demo
```

검증 범위는 성공·추가 자료·3회 소진·도구 오류·중복·상태 복구·v2 계약·병렬 실행·MCP stdio·timeout이다.
외부 모델 없는 단위 테스트와 실제 PCB 샘플 검증을 구분한다. MCP 선택 의존성이 없으면 stdio 테스트 일부는 skip될 수 있다.
수정마다 성공 시나리오의 **최종 JSON 생성·Pydantic/JSON Schema 검증**과 추가 요청/실패 시나리오의 의도한 종결까지 확인한다.
스켈레톤 통합 후에는 Kafka 결과 계약과 Markdown 리포트 파일 생성도 완료 기준에 추가한다.

## 다음 단계 — 스켈레톤 통합 전 계획

[skeleton-agent](https://github.com/skala-ai-agent-team6/skeleton-agent)의 확인 기준 커밋은 `99372b0ababe1971441f35556f38c0cd6cebe4c1`이다. 현재 저장소에는 통합하지 않았다.
Kafka 입출력·Spring 사진 다운로드·LLM 리포트 뼈대를 가져오고, 이 저장소의 검증·상태 관리·실행 제한·로그·테스트를 결합할 계획이다.

최신 합의의 해석/검색 MCP 구성은 아래4개 서버·7개 도구다. **이 표는 연결 계획이며 현재 구현 목록이 아니다.** 사진 추론 모델과 라벨 해석 도구를 구분한다.

| 서버 | 도구 |
|---|---|
| observation-mcp | normalize_observation / list_observation_terms |
| pcb-mcp | interpret_pcb |
| damage-mcp | interpret_damage / list_damage_labels |
| rag-mcp | retrieve_repair_context / search_reference_documents |

목표 순서는 입력 검증 → [소견 정규화 ∥ 사진 분석·라벨 해석] → 통합 RAG → 충분성 판단 → 리포트/추가 요청/실패다.
통합 RAG는 분석 라벨을 입력받으므로 현재 코드의 병렬 모의 검색과 실행 순서가 다르다. 기존 사진 분석 MCP의 analyze_pcb_image와 interpret_pcb는 별개 역할이다.
도메인 불일치·미등록 라벨·검색 없음·도구 장애를 사진 품질 부족과 구분해야 한다. 검색 점수·검출 점수를 진단 확률로 취급하지 않는다.

## Harness 잔여 과제

MCP 교재의8개 요소 전체가 목표이며 기본 실행 제한만으로 완료로 간주하지 않는다.

| 요소 | 남은 작업 |
|---|---|
| Instructions | prompts/*.md 버전 관리·LLM 역할/출력 규칙 |
| Tools | 나머지 실제 MCP·결과 길이 제한·LLM 도구 오류 전달 |
| Context | 토큰 예산·긴 입력 요약·실제 RAG 근거 주입 |
| State/Memory | 이벤트 재분석과 checkpointer/store 요구 대응, 분산 worker claim/lease |
| Agent Loop | 모델 턴·같은 도구 반복·전체 deadline·토큰/비용 예산 |
| Guardrails | 민감정보 처리·변경 도구 차단/승인 경계 검증 |
| Recovery | 실제429/503·Kafka 재처리/DLT·저장/발행 간 장애 복구 |
| Observability | 공통 trace·LangSmith·토큰/비용·실패 추적·평가 기록 |

SQLite는 현재 도구 실행 중 쓰기 잠금을 유지하는 로컬 구현이다. Kafka worker에 결합할 때 DB 트랜잭션 밖 실행·durable inbox/outbox·동일 출력 재전송을 설계해야 한다. 로컬 중복 방지만으로 외부 호출 exactly-once를 보장하지 않는다.

## 파일 안내

- app/workflow.py: 그래프·병렬 호출·충분성 판단·추가 요청·종료.
- app/pcb.py: 실제 PCB stdio 클라이언트·사진 매핑·응답 정규화.
- app/service.py: 입력 중복/차수 검증·SQLite 상태/결과 저장.
- app/backend_v2.py: Spring 계약 검증·변환.
- app/models.py, app/limits.py: 데이터 계약·공통 한도.
- app/observability.py: JSON 로그·파일 회전.
- app/main.py: 로컬 API. app/demo.py, app/pcb_demo.py: 시연 CLI.
- tests/, examples/: 회귀 테스트·계약 fixture·입력 예시.
