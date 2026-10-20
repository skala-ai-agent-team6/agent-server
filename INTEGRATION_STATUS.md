# 스켈레톤 기반 통합

기준 커밋: `skala-ai-agent-team6/skeleton-agent@104df49`.
기존 `agent-server`를 덮어쓰지 않고 별도 작업 브랜치에서 통합한다.
아래의 '검증'은 코드 회귀 테스트와 실제 MCP 테스트를 구분한다.

## 하네스 후속 작업

`feat/harness-hardening`에서 도구 실패 처리 1단계를 보완했습니다. 현재 동작과 추가 검증은 [HARNESS_STEP1.md](HARNESS_STEP1.md)를 참조합니다. 프롬프트 분리·버전 기록 2단계는 [HARNESS_STEP2.md](HARNESS_STEP2.md)를 참조합니다. 아래는 통합 커밋 `3517604` 시점의 기록이며, 당시의 “도구 오류를 검출 없음으로 처리” 부분은 후속 작업에서 변경됐습니다.

## 최신 GitHub 대조 및 수정 구분 (2026-10-20 재점검)

최초 접수 한도는 사용자 요청으로 원본 **10장**으로 복원했다. 전체 업로드 50MB도 복원했고,
추가했던 Tomcat part 수 확대는 제거했다. 백엔드 테스트 25개 통과 및 실제 HTTP에서 11장 거절을 확인했다.
Agent 자체의 전체 사진 순회는 스켈레톤 원래 동작이다.

| 저장소 | 검증에 사용한 기준 | 현재 GitHub HEAD | 결과 |
| --- | --- | --- | --- |
| skeleton-agent | 104df49 | 104df49 | 핵심 그래프·판단 유지, 하네스와 기능 연결 추가 |
| backend | 63e0f96 + 로컬 수정 | 63e0f96 | Kafka/SSE 원본 유지. 사진 분류 입력·dev 실제 업무 MCP 선택은 로컬 기능 수정 |
| MCP_Error_check | b6e2cd6 기반 스켈레톤 내 모델 서버 | b6e2cd6 | PCB 원본 유지, 기계에는 원래 라벨 보존 필드 추가 |
| Rag_mcp | d65c019 | 8b0b467 | 최신 점검 지침·종합 근거 추가는 아직 반영하지 않음 |
| business-mcp | 12c9ac5 | 444a931 | 기존 재고 연동 검증됨. 새 가격·위치·담당자·도착일·호환 모델 정보 전달은 미반영 |

RAG 최신 응답은 기존 필드를 유지하지만 `pcb_guidance`, `damage_guidance`, `integrated_assessment` 등이 추가됐다.
현재 Agent는 이 신규 필드를 리포트에 전달하지 않는다. 업무 어댑터도 기존 재고·공급처 필드만 전달한다.
이번 점검에서는 원격 ref를 fetch해 비교했으며 팀원 저장소의 작업 파일·실행 중 MCP 버전을 바꾸지 않았다.
따라서 아래 실제 실행 성공 기록은 **표에 적힌 검증 기준 버전**의 결과이며 최신 기능 전체 통합 검증은 아니다.

| 수정 위치 | 내용 | 분류 |
| --- | --- | --- |
| worker/harness.py, journal.py, observability.py | 실행·재시도 한도, 재접수 검증, 영속 결과·전송 기록, 로그 | 하네스 |
| worker/main.py | 기존 Kafka 연결 유지 + 처리·저장·재전송 순서와 poll 제한 보완 | 하네스 |
| worker/tools.py | 다운로드 무결성·JSON·호환 부품 검증, 도구 래퍼 | 하네스 |
| worker/graph.py, enrichment.py | 추가 해석 MCP 호출, 결과를 리포트 근거로 연결 | 기능 통합 |
| worker/graph.py, backend Inputs/RepairService/ApiController | 명시적 사진 분류 전달·사용, 미지정이면 GPT 분류 | 기능 변경 |
| mcp_servers/mechanical/server.py | 원래 모델 라벨 raw_label 보존 | 기능 통합 |
| worker/tools.py, graph.py | RAG 기술 문서 전달·부품 추천 및 보고서 지시 보완 | 기능 통합 / 일부 출력 가드레일 |
| worker/business_gateway.py | inventory.lookup → get_part_availability 변환 | 기능 통합, 인증·입력 검증은 가드레일 |
| backend BusinessMcp/application-dev.properties | 개발 환경에서 실제 업무 MCP 선택 가능 | 환경·연동 변경 |
| worker/report.py, tests, 검증 스크립트, 문서 | 리비전별 JSON/Markdown·검증 자료 | 출력·개발 검증 지원 |

## 받은 MD 2개와의 대조 (2026-10-20)

대상: 상위 폴더의 `오케스트레이션_에이전트_개발방향.md`, `LangGraph_핵심정리.md`.

| 문서 항목 | 현재 구현 | 판정 |
| --- | --- | --- |
| Kafka 요청·사진 ID 입력 | Spring 첨부 메타데이터를 받아 인증 API로 다운로드 | 반영 |
| 사진마다 GPT 분류 | GENERAL/미지정에 GPT 사용, 기사가 지정한 분류는 그대로 사용 | 팀의 분리 입력 결정에 맞춰 변경 |
| 소견 구조화 분리 | PCB/기계/SW 단서 분리. 에러 코드는 별도 입력 필드에서 읽음 | 부분 반영 |
| 사진·텍스트 기반 MCP 선택 | 사진 종류로만 이미지 MCP 선택. 텍스트 단서는 경로 선택에 사용하지 않음 | 부분 반영 |
| 낮은 확신·부족한 단서 평가 | 스켈레톤 그대로 결함 하나라도 검출되면 완료 | 미반영 |
| 구체적 추가 사진·설명 요청 | 부위 wheel과 안내 문장이 고정. 상황별 요청 생성은 없음 | 부분 반영 |
| 이벤트 추가 요청·최대 3회 | Kafka→Spring→SSE, 제출 후 새 이벤트, 3회 후 실패 | 반영·실제 검증 |
| RAG 근거·Pydantic JSON·박스 이미지 경로 | 반영. 박스 이미지는 현재 모델의 JPG 출력 사용 | 반영, 최신 RAG 신규 필드는 미반영 |
| LLM 사용 단계 | Agent 직접 호출은 분류·텍스트 분리·리포트. 추가 소견/RAG MCP 내부에서도 LLM 호출 | 시스템 전체로는 문서보다 늘어남 |
| State·조건부 분기·병렬 합류·리듀서 | StateGraph, add_conditional_edges, operator.add | 반영 |
| 반복·시간·호출 상한과 관측 | 재요청/recursion/timeout/call budget, 파일 로그 | 반영 |
| 토큰·비용 상한·checkpointer | 미구현. SQLite 요청/최종 결과 저장은 노드 체크포인트와 다름 | 미반영 |

LangGraph 핵심 정리는 참고 패턴 모음이다. Supervisor, Handoff, Time Travel 등을 전부 구현해야 한다는 기능 명세는 아니다.
현재 추가 자료 요청은 합의한 이벤트 방식이며 LangGraph interrupt를 사용하지 않는 것이 그 자체로 누락은 아니다.
이 대조를 위해 판단 함수나 그래프 연결은 변경하지 않았다.

## 유지한 동작

- 원본 9개 노드와 병렬 분기·합류 구조를 유지한다.
- `decide`는 원본 그대로다. 이미지 분석에서 하나라도 결함을 찾으면 리포트로 간다.
  이전 통합 코드의 모든 부위 충분성·0.5 임계값은 가져오지 않았다.
- 사진마다 PCB 모델을 호출하고, 기계 사진마다 표면 모델과 찌그러짐 모델을 모두 호출한다.
- 추가 자료 요청은 초기 입력과 별개로 최대 3회. 이벤트를 반환하고 종료하며 새 명령으로 재실행한다.
- 기계 결과는 리포트에 직접 전달한다. RAG에는 원본대로 `damage_labels: []`를 보낸다.
- 스켈레톤의 GPT 소견 분리와 GPT 리포트 생성도 유지한다.
- 사진의 명시적인 `PCB`/`MOTOR`/`MECHANICAL` 구분은 재분류하지 않는다.
  `GENERAL`/미지정은 원본 GPT 분류를 사용한다.

## 추가한 연결

| 기능 | 위치 | 실행 시점 |
| --- | --- | --- |
| 소견 정규화 | `worker/enrichment.py` | 기존 소견 분리 후, 해석을 리포트 근거에 추가 |
| PCB 라벨 해석 | `worker/graph.py` | PCB 사진 전체 분석 후 검출 라벨이 있으면 호출 |
| 파손 라벨 해석 | `worker/enrichment.py` | 모델 응답에 명시적 6종 찌그러짐 부위가 있을 때 호출 |
| 소견·파손 목록 및 독립 문서 검색 | `worker/check_connections.py` | 연결 확인 명령에서 사용. 모든 진단에 불필요하게 호출하지 않음 |
| 부품 추천 | `worker/tools.py` | Spring `compatibleParts`의 ID만 허용, 목록 밖 ID·중복 거절 |
| 업무 MCP 어댑터 | `worker/business_gateway.py` | Spring의 `inventory.lookup`을 실제 `get_part_availability`로 변환 |

찌그러짐 6종: `boot`, `doorouter`, `fender`, `front-bumper`, `quaterpanel`, `roof`.
원본 모델의 부위명/`부위명-dent` 또는 명시적 `damage_labels`를 해석 입력으로 사용한다.
기계 서버에 `raw_label` 필드를 추가해 모델의 원래 부위명을 보존한다. 기존 `dent`/`breakage`와 박스 표시는 유지한다.
부위명은 추측하지 않으며, 지정한 6종 외의 라벨을 이 6종으로 임의 변환하지 않는다. RAG 팀 서버 자체의 더 넓은 라벨 사전은 수정하지 않았다.

## 이식한 하네스

| 항목 | 구현 |
| --- | --- |
| 추가 요청 상한 | 원본 3회 판단 + 입력 차수 0~3 검증 |
| 그래프 실행 상한 | `recursion_limit=20` |
| 도구 재시도 | timeout/연결 오류 최대 2회 재시도. 전체 그래프 재시도 대신 호출 단위 |
| HTTP/LLM 재시도 | 상태 코드 429·503 최대 2회 재시도. 401 등 영구 오류는 재시도하지 않음 |
| 호출 예산·시간 | 입력 장수에 따른 호출 예산과 전체 상한, 호출/실행 timeout |
| JSON 계약 | Spring v2 스키마로 명령·진행·최종 이벤트 검증 |
| 결과 보존·중복 방지 | SQLite inbox/outbox. 같은 ID의 다른 본문 거절 |
| 결과 전송 복구 | 최종 결과 저장 후 브로커 오류가 나면 저장 결과만 재전송 |
| 재개 검증 | 요청 키·체크포인트, 연속 revision, 새 executionId, 동일 correlationId, 기존 첨부 보존, 새 첨부/소견 변경, 동일 접수의 중복 작업 차단 |
| 관측 | 기존 JSON 로거 이식: 작업·접수·correlation·execution·revision, 도구 호출 ID·재시도·노드 실패·시간 기록. 파일 순환 저장 및 실행별 metrics 영속화 |
| 파일 출력 | 리비전별 Markdown 및 JSON, 테스트의 전체 이벤트 JSON |
| 다운로드 검증 | 비동기 다운로드, 크기·SHA-256 검증. 실패 시 샘플 사진 대신 FAILED |
| 긴 Kafka 작업 | 실행 한도 + 180초 이상으로 `max_poll_interval_ms` 설정, 한 번에 한 명령 처리 |

PDF 하네스 전체가 완료된 것은 아니다. 토큰·비용 예산, 문맥 요약, PII 마스킹,
LangGraph checkpointer/분산 워커 lease, LangSmith 추적은 아직 없다.
프로세스가 최종 결과 저장 전에 종료되면 이미 실행한 모델 호출은 반복될 수 있다.
도구 timeout은 클라이언트 대기 취소이며 외부 서버의 GPU 추론 종료를 보장하지 않는다.
기계의 2개 호출은 하나의 래퍼에서 최대 2회분으로 보수적으로 계산한다.
MCP가 멈췄을 때 원본의 '근거 없음/검색 없이 진행' 정책은 유지한다.
반면 실행 예산 초과는 잡아 숨기지 않고 실패 처리한다.

## 실행

```bash
uv sync --locked
uv run python -m pytest -q
uv run python -m worker.check_connections
uv run python -m worker.verify_live
```

- `pytest`: 모델을 대체한 회귀·하네스 테스트. 실제 모델 정확도를 검증하지 않는다.
- `check_connections`: 4개 해석/RAG MCP의 7개 도구를 실제 호출한다. 라벨은 제공된 테스트 입력이다.
- `verify_live`: 서로 다른 PCB 2장 / PCB+기계 / 기계 단독을 실제 모델·해석 MCP·RAG·LLM으로 실행한다.
  Spring 다운로드와 Kafka 발행만 로컬 입력·출력으로 대체한다.
- 리포트: `worker/reports/`. 결과 목록은 `live-summary.json`, 각 결과는 Markdown/JSON/이벤트 JSON으로 저장된다.
- 현재 검증은 기존 8011~8014 서버와 새 통합 기계 서버 9105를 사용한다. MCP 서버를 새 clone 경로에서 재시작하려면
  각 저장소 README에 따라 환경·모델·벡터 색인을 준비해야 한다.
- `uv run python -m worker.main`은 실제 Kafka 워커이므로 기존 Agent 워커와 동시에 켜지 않는다.
  `verify_live`는 Kafka를 사용하지 않는다. `e2e_check`는 실제 Spring 접수를 생성한다.
- Spring 없이 테스트할 때는 `worker.verify_live`를 사용한다.
  원본 `worker.send_test`의 가상 첨부 ID는 실제 다운로드에 사용할 수 없으며 자동 샘플 대체도 하지 않는다.
- 업무 MCP 어댑터는 `BUSINESS_GATEWAY_TOKEN`을 설정한 뒤
  `uv run uvicorn worker.business_gateway:app --host 127.0.0.1 --port 8089`로 실행한다.
  기존 8089 gateway가 있으면 먼저 어느 버전을 쓸지 정해야 한다.
  Spring의 MCP URL은 이 어댑터의 `/rpc`, 토큰은 동일 값으로 설정한다.
  이 어댑터는 재고 조회만 허용하며 발주 승인·등록은 기존 Spring에서 수행한다.

## 검증 결과 (2026-10-20)

| 검증 | 결과 | 근거 파일 (`worker/reports/`) |
| --- | --- | --- |
| 회귀·하네스 테스트 | 35개 통과. 모델·외부 서비스는 테스트 대역 | `uv run pytest -q` |
| 실제 Spring/Kafka 혼합 진단 | PCB 2장+기계 1장 모두 분석, 최종 JSON 저장·SSE·리포트 생성 | `e2e-mixed-summary.json` |
| 실제 기계 부위 라벨→파손 해석 | 이미지 모델의 `roof-dent`→`interpret_damage`→리포트 | `e2e-dent-summary.json` |
| 실제 추가 요청 상한 | 빈 시험 이미지 최초 분석 + 추가 3회, 4차 요청 없이 FAILED. 각 재접수 중복 전송도 확인 | `e2e-retry-limit-summary.json` |
| 실제 업무 연결 | LLM 추천 부품→Spring→새 업무 게이트웨이→실제 MCP→창고 조달 제안→승인. 시험 주문 취소로 예약 해제 | `procurement-13-summary.json` |
| MCP 개별 연결 | 4개 해석/RAG 서버의 7개 도구 호출 성공 | `mcp-connections.json` |
| 스켈레톤 유지 | 기준 커밋과 `decide` AST·전체 그래프 연결 동일, 워커가 실제 `build_graph()` 실행 | `worker/graph.py`, `worker/main.py` |

실제 통합 테스트는 전용 로컬 DB의 개발 계정·가상 제품/재고를 사용한다. 모델/MCP/LLM/통신은 대역이 아니다.
Agent 단독 100장 순회 회귀 테스트는 수행했으나, 사용자 요청에 따라 Spring 최초 접수 한도는 원본 10장으로 복원했다. 실제 모델 100장 부하 시험은 수행하지 않았다.
기계·찌그러짐 모델은 강판/자동차 데이터로 학습되었다. 검출 라벨을 실제 모델에서 받았다는 사실과
청소기 고장 진단의 정확도는 다르다. 도메인 불일치 라벨을 청소기 부위로 단정하지 않도록 보고서 지시에 반영했다.

## 직접 실행하고 확인하기

서비스가 켜진 상태에서 저장소 루트에서 실행한다. Spring은 mock AI를 꺼야 하며,
같은 요청을 처리하는 다른 Agent consumer도 중지한다. 서로 다른 consumer group도 같은 명령을 각각 받는다.

```bash
uv run python -m worker.main
# 별도 터미널 — 실제 접수 생성. .local/e2e.env에 전용 dev DB의 DEV_PASSWORD 필요
uv run python -m worker.e2e_check --scenario mixed
uv run python -m worker.e2e_check --scenario mechanical
uv run python -m worker.e2e_check --scenario retry-limit
# 실제 완료된 접수의 재고·승인 확인, 테스트 주문 취소까지 수행
uv run python -m worker.check_procurement --repair-id <검증용_접수_ID>
```

- 사람이 읽는 리포트: `worker/reports/task-<작업ID>_revision-<차수>_고장리포트.md`
- 동일 결과 JSON: 같은 이름의 `.json`; `command`는 입력, `result`는 최종 출력이다.
- 상세 로그: `.local/logs/agent.jsonl` (5MB 단위, 백업 3개). 입력 원문·API 키는 이 로그에 넣지 않는다.
- 이벤트·최종 결과·실행 통계: `.local/integration.sqlite3`의 `jobs`, `events`, `executions`.
- 로컬 검증 시 `KAFKA_AUTO_OFFSET_RESET=latest`를 사용한다. 새 consumer group은 반드시 워커 준비 로그 확인 후 접수한다.
  운영 기본값 `earliest`를 변경한 것은 아니며, 이미 커밋한 offset은 이 설정보다 우선한다.
- 기계 서버: `PORT=9105 uv run --project mcp_servers/mechanical python mcp_servers/mechanical/server.py`.
  기본 9104를 쓰려면 Agent `MECH_MCP_URL`도 맞춘다.
- 업무 게이트웨이: `uv run uvicorn worker.business_gateway:app --host 127.0.0.1 --port 8089`.
  Spring `/rpc` 주소 및 `MCP_TOKEN`과 게이트웨이 `BUSINESS_GATEWAY_TOKEN`이 같아야 한다.
- `.env`, `.local`, 생성 리포트/모델 결과는 Git에서 제외된다.

## 보완 내용과 한계

- 429·503 재시도, RAG 기술문서 전달, 긴 작업 Kafka poll 설정, 다운로드 무결성 검증을 포함한다.
- 재접수 검증과 기존 구조화 로그를 이식하고 빈 리포트 필드를 거절한다.
- 실행 상한·그래프 상한·실행 timeout 실패는 각각 구분된 errorCode로 저장한다.
- 기본 판단은 원본 그대로다. 결함 하나라도 있으면 COMPLETE, 추가 사진 요청 부위는 `wheel` 고정이다.
- LangGraph checkpointer·분산 lease, 토큰/비용 예산, 문맥 요약, PII 마스킹, LangSmith는 기존 구현에도 없던 별도 과제다.
  이 통합을 PDF 하네스 전체 구현 완료로 표현하지 않는다.
- 중복 Kafka 전달은 저장된 결과를 재사용한다. 다만 모델 실행 중 프로세스가 종료되면 해당 실행은 반복될 수 있다.

### 최종 검증 파일

- 최신 혼합 진단: `task-14_revision-1_고장리포트.md/json` (접수 17, PCB 2장+기계 1장).
- 최신 기계·파손 해석: `task-13_revision-1_고장리포트.md/json` (접수 16).
- 3회 요청 후 종료: `task-12_revision-4_고장리포트.md/json` (접수 15).
- 실행 중인 새 워커의 서비스 토큰으로 Spring 첨부 다운로드가 성공했다.
- 처음 혼합 시험에서 이전 agent-server 테스트 워커가 같은 명령에 응답하는 충돌을 발견했다.
  그 워커를 종료한 뒤 새 워커만으로 다시 검증한 결과가 위 파일이다.
