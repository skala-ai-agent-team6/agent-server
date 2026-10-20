# 로봇청소기 수리 진단 Agent Server

Spring에서 Kafka로 전달한 수리 접수를 받아, LangGraph로 사진 분석·소견 해석·RAG 검색을 실행하고 최종 JSON과 Markdown 리포트를 만드는 에이전트 서버입니다.

이 브랜치는 팀의 [`skeleton-agent@104df49`](https://github.com/skala-ai-agent-team6/skeleton-agent/tree/104df49)를 바탕으로 MCP 연결과 실행 제어를 추가한 **통합 개발 버전**입니다. 원본의 9개 노드·분기·판단 함수를 유지하며, 실제 워커가 이 그래프를 실행합니다. 통합과 일부 실제 연동 검증은 끝났지만, 진단 판단 개선과 교재 하네스 전체 구현은 남아 있습니다.

- 작업 브랜치: `feat/agent-harness-integration` — `main`과 별도로 관리합니다.
- 상세 변경 내역·MD 2개 대조·검증 기록: [통합 현황](INTEGRATION_STATUS.md)
- Spring/Kafka 계약: [연동 계약](contract/INTEGRATION.md)
- 아래 현황은 **2026-10-20 점검 기준**입니다.

## 현재 처리 흐름

```text
기사: 사진 + 자연어 소견 입력
  → Spring: 접수·사진 저장 (최초 업로드 최대 10장)
  → Kafka: diagnosis.commands.v2
  → Agent: 입력·중복·재접수 검증
  → classify_photos: 지정한 사진 구분 사용 / 미지정이면 GPT 분류
  → split_text: 소견 분리 + observation MCP 정규화
  → 필요한 이미지 분석 병렬 실행
      ├ PCB 사진 전체 분석 → PCB 라벨 해석
      └ 기계 사진 전체 분석 → 표면·찌그러짐 분석 → 부위 라벨 해석
  → RAG: 소견·PCB 라벨로 사례·문서 검색
  → judge
      ├ 결함 검출 있음 → LLM 리포트 → JSON 검증·저장
      ├ 검출 없음, 추가 요청 3회 미만 → 사진 요청 이벤트·저장·종료
      │   → Spring/SSE 알림 → 기사 추가 제출 → 새 Kafka 명령
      └ 추가 3회 분석 후에도 검출 없음 → 실패 결과·저장·종료
  → Kafka: diagnosis.events.v2 → Spring 저장·SSE
  → Spring: 필요 부품 재고 조회·승인 후 조달 처리
```

**현재 `judge`는 “결함이 하나라도 검출됐는가”를 판단합니다.** 낮은 확신이나 근거 부족을 종합 평가하는 기능은 아직 없습니다. 추가 사진 요청 부위도 `wheel`로 고정되어 있습니다. 이는 하네스 누락으로 실행이 막힌 상태가 아니라, 유지한 스켈레톤의 판단 기능을 앞으로 보완해야 한다는 뜻입니다.

사진 재요청은 초기 입력과 별개로 최대 3회입니다. LangGraph `interrupt` 대신 이벤트를 반환하고 실행을 종료한 뒤, 추가 제출 이벤트로 다시 실행합니다. RAG는 현재 스켈레톤 순서대로 이미지 분석 후 실행합니다.

## 통합된 부분과 남은 기능

| 구분 | 진행된 부분 | 남은 부분 | 분류 |
| --- | --- | --- | --- |
| 스켈레톤 | 원본 그래프·분기·`decide` 유지, 실제 워커에서 실행 | 아래 판단 로직 보완 | 오케스트레이션 |
| Spring·Kafka | 접수 → 분석 → 결과 저장·SSE 실제 검증 | 최신 백엔드 배포 환경 및 프론트 화면 재검증 | 연동 |
| 이미지 분석 | PCB·기계 사진별 분석, 표면·찌그러짐 도구 호출 | 청소기 도메인 진단 정확도 평가 | 모델 연동·평가 |
| 소견·라벨 해석 | 소견 정규화·PCB 라벨·찌그러짐 부위 해석 연결 | 최신 팀 MCP 변경에 맞춘 재검증 | MCP 연동 |
| RAG | 사례·라벨 정의·기술 문서를 리포트 근거로 전달 | 새 지침·종합 평가 필드 전달, 외형 라벨 검색 연결 | MCP 연동 |
| 진단 판단 | 검출 유무로 완료/추가 요청/실패 분기 | 확신·근거 충분성 평가, 텍스트 단서의 라우팅 활용 | **하네스 외 기능** |
| 추가 요청 | 최대 3회 및 재접수 검증 | 상황별 촬영 부위·방법·추가 설명 요청 생성 | 기능 + 하네스 |
| 리포트 | LLM → JSON 검증 → JSON/Markdown 저장, 호환 부품 ID 제한 | 최신 MCP 응답 반영 및 진단 품질 평가 | 출력 기능 + 가드레일 |
| 업무 MCP | Spring `inventory.lookup`을 `get_part_availability`로 변환 | 신규 가격·위치·담당자·도착일·호환 모델 정보 전달 | 연동 |

백엔드 검증에는 `backend@63e0f96`에 사진 분류 입력과 개발 환경의 실제 업무 MCP 선택을 보완한 **별도 로컬 변경**을 사용했습니다. 그 백엔드 변경은 이 저장소의 커밋에 포함되지 않습니다. 최초 10장·전체 업로드 50MB 제한은 원본대로 유지합니다.

### MCP 연결 범위

| 서버/역할 | 도구 | 호출 위치 |
| --- | --- | --- |
| PCB 이미지 모델 | `analyze_pcb_image` | PCB 사진마다 실행, stdio |
| 기계 이미지 모델 | `detect_surface_defects`, `detect_dents` | 기계 사진마다 실행, HTTP |
| observation-mcp | `normalize_observation` | 기사 소견 정규화 |
| pcb-mcp (라벨 해석) | `interpret_pcb` | 검출된 PCB 라벨 해석 |
| damage-mcp | `interpret_damage` | 명시적 찌그러짐 부위 라벨이 있을 때 해석 |
| rag-mcp | `retrieve_repair_context` | 사례·문서 검색 |
| 해석/RAG 보조 도구 | `list_observation_terms`, `list_damage_labels`, `search_reference_documents` | 연결 확인 스크립트에서 사용 |
| business-mcp | `get_part_availability` | 별도 업무 게이트웨이를 통해 Spring이 재고 조회 |

“4개 서버·7개 도구 호출 성공”은 **해석/RAG 서버의 개별 연결 확인 결과**입니다. 모든 진단에서 7개를 전부 호출한다는 뜻은 아닙니다.

기계 모델에는 `raw_label`을 추가해 원래 부위명을 보존했습니다. 찌그러짐 해석 대상은 `boot`, `doorouter`, `fender`, `front-bumper`, `quaterpanel`, `roof`이며, 일반 `dent`만으로 부위를 추측하지 않습니다. 자동차·강판 모델의 검출을 청소기 고장 확정으로 취급하지 않습니다.

### 팀 저장소 버전

| 저장소 | 실제 검증에 사용한 기준 | 점검 시 원격 최신 | 미반영 변경 |
| --- | --- | --- | --- |
| skeleton-agent | `104df49` | `104df49` | 해당 없음 |
| backend | `63e0f96` + 별도 로컬 변경 | `63e0f96` | 백엔드 변경은 별도 관리 |
| MCP_Error_check | `b6e2cd6` 기반 모델 서버 | `b6e2cd6` | 해당 없음 |
| Rag_mcp | `d65c019` | `8b0b467` | `pcb_guidance`, `damage_guidance`, `integrated_assessment` 등 |
| business-mcp | `12c9ac5` | `444a931` | 가격·창고 위치·담당자·예상 도착일·호환 모델 등 |

현재 RAG 호출의 `damage_labels`는 원본대로 빈 목록입니다. 파손 해석 결과는 리포트에 직접 전달하지만, 최신 RAG의 외형 점검 지침까지 연결한 상태는 아닙니다.

## 교재 기준 하네스 현황

하네스는 분석 기능 주변에서 실행 한도·오류·상태·출력·추적을 관리하는 코드입니다. 아래는 `AI-Agent Capstone(+MCP).pdf` 하네스 항목과의 대조이며, **전체 완료가 아닙니다.**

| 교재 항목 | 현재 구현 | 남은 구현 | 상태 |
| --- | --- | --- | --- |
| Instructions | 코드 내부 역할·출력 지시 | 프롬프트 파일 분리, 버전 관리·변경 평가 | 부분 |
| Tools | MCP 연결, 일부 RAG 길이 제한, 오류 결과 수집 | 도구별 결과 크기 제한, 일관된 오류 형식 | 부분 |
| Agent Loop | 추가 요청 3회, `recursion_limit=20`, 호출·시간 상한 | 교재의 턴/동일 도구 반복 기준과 현재 정책 대응 정리 | 부분 |
| Context | 소견·분석·검색 근거 전달 | 토큰 예산, 오래된 문맥 요약·잘라내기 | 미완료 |
| State / Memory | SQLite 요청·이벤트·최종 결과 저장 및 재접수 검증 | 노드 단위 checkpointer, 중간 실행 복원·분산 워커 소유권 | 부분 |
| Guardrails | JSON 계약·다운로드 무결성·호환 부품 검증, 업무 어댑터 읽기 전용, Spring 승인 | PII 마스킹, 변경 도구 승인 경계 검증 | 부분 |
| Recovery | 연결/timeout/429/503 최대 2회 재시도, 저장 결과 재전송 | 필수 도구 실패와 선택 도구 실패 구분, 소진 후 명확한 FAILED 처리 | 부분 |
| Observability | 구조화 파일 로그, 작업/실행/도구 ID, 시간·재시도 통계 | LangSmith 연결, 토큰·비용 및 전체 경로 추적 | 부분 |
| 평가·회귀 검증 | 자동 테스트와 실제 연동 시나리오 | 고정 평가셋, 변경 전후 품질·실패율 비교, 실패 trace 연결 | 부분 |

현재 기본값은 호출당 90초, 이벤트 실행당 1,800초, 그래프 20 super-step입니다. 호출 예산은 사진 수에 따라 계산하며 최대 2,000회로 제한합니다. 이는 교재 예시의 8턴·120초를 그대로 적용한 값이 아닙니다. 도구별 “최초 1회 + 재시도 2회”도 같은 도구의 전체 실행 횟수를 3회로 제한한다는 뜻은 아닙니다.

중복 이벤트는 저장된 결과를 재사용합니다. 다만 최종 결과 저장 전에 프로세스가 죽으면 모델을 다시 실행할 수 있습니다. 일부 MCP 오류는 원본처럼 “검출 없음/검색 없이 진행”으로 처리하므로, 모든 도구 오류가 즉시 실패로 끝나는 것은 아닙니다.

## 다음 개발 순서

1. 남은 하네스: 필수 도구 실패 정책, 프롬프트 분리, 토큰·문맥·PII 관리, 상태 복원과 추적.
2. 판단 기능: 낮은 확신·근거 부족 평가, 상황에 맞는 추가 사진/설명 요청.
3. 최신 RAG·업무 MCP 응답 필드를 연결하고 리포트에 반영.
4. 같은 평가셋과 실제 통합 시나리오로 최종 JSON·리포트·Spring 표시를 재검증.

## 설치 및 실행

저장소 루트에서 실행합니다. Python 환경은 `uv`로 설치합니다.

```bash
uv sync --locked
uv sync --locked --project mcp_servers/pcb
uv sync --locked --project mcp_servers/mechanical
# .env가 없을 때만 예제 복사
[ -f .env ] || cp .env.example .env
```

`.env`에 OpenAI 키, Spring 내부 토큰, Kafka 주소, 실행 중인 MCP 주소를 설정합니다. `.env`에 채운 값은 같은 이름의 셸 환경변수보다 우선합니다. 외부 MCP의 모델·임베딩·벡터 색인은 각 팀 저장소에서 준비해야 하며, 이 저장소 설치만으로 모든 서버가 켜지지는 않습니다.

| 서비스 | 기본 주소/포트 | 비고 |
| --- | --- | --- |
| Spring | `localhost:8080` | 실제 연동 시 mock AI 비활성화 |
| Kafka | `localhost:9092` | 기존 팀 Kafka 또는 이 저장소 compose 중 하나 사용 |
| PCB 이미지 MCP | stdio | `mcp_servers/pcb/.venv` 사용 |
| 기계 이미지 MCP | `localhost:9104/mcp` | 아래 명령으로 별도 실행 |
| 해석/RAG MCP | `8011`~`8014` | [Rag_mcp](https://github.com/skala-ai-agent-team6/Rag_mcp) 환경 준비 |
| 업무 MCP / 게이트웨이 | `8000` / `8089` | [business-mcp](https://github.com/skala-ai-agent-team6/business-mcp), 재고 연동 시 필요 |

```bash
# 기계 모델 서버 (별도 터미널)
uv run --project mcp_servers/mechanical python mcp_servers/mechanical/server.py

# 업무 게이트웨이 (필요 시 별도 터미널)
uv run uvicorn worker.business_gateway:app --host 127.0.0.1 --port 8089

# 실제 Kafka 워커
uv run python -m worker.main
```

Spring의 업무 MCP 주소는 게이트웨이 `/rpc`로 지정하고, Spring 토큰과 `BUSINESS_GATEWAY_TOKEN`을 맞춥니다. 이 어댑터는 재고 조회만 하며 조달 승인 처리는 Spring이 담당합니다. 이미 켜진 워커와 중복 실행하지 않습니다. 서로 다른 consumer group도 같은 명령을 각각 받습니다.

## 검증 및 리포트 확인

### 1. 외부 서버 없이 회귀 테스트

```bash
uv run python -m pytest -q
```

모델·외부 서비스를 테스트 대역으로 바꿉니다. 그래프 분기, 전체 사진 순회, 추가 요청 3회 제한, 재시도·예산·중복·재접수·JSON 저장을 검증합니다. 실제 모델 품질 검증은 아닙니다.

### 2. 실제 모델·MCP·LLM 테스트

각 MCP 서버와 OpenAI 설정이 준비된 상태에서 실행합니다. Spring/Kafka 입출력만 로컬 파일로 대체합니다.

```bash
uv run python -m worker.check_connections
uv run python -m worker.verify_live --scenario mixed
uv run python -m worker.verify_live --scenario mechanical
```

`--scenario all` 또는 `multi-pcb`는 형제 디렉토리 `../MCP_Error_check`의 추가 시험 사진도 필요합니다. 단독 검증 명령은 매 실행 새로운 작업 ID를 생성하며, 입력 내부 ID도 일치하도록 검증합니다.

### 3. 실제 Spring·Kafka부터 리포트까지

전용 개발 DB와 서비스들을 실행하고, `.local/e2e.env`에 개발 계정 `DEV_PASSWORD`를 설정합니다. 아래 명령은 **실제 시험 접수를 생성**하며 형제 `MCP_Error_check` 저장소의 시험 이미지가 필요합니다.

```bash
# 워커가 준비된 다음 별도 터미널에서 실행
uv run python -m worker.e2e_check --scenario mixed
uv run python -m worker.e2e_check --scenario mechanical
uv run python -m worker.e2e_check --scenario retry-limit
```

새 검증 consumer group에 `KAFKA_AUTO_OFFSET_RESET=latest`를 쓰면 반드시 워커 준비 후 접수합니다. 이미 커밋된 offset은 이 설정보다 우선합니다. 원본 `worker.send_test`의 가상 첨부 ID는 Spring에 존재하지 않으므로 그대로 보내면 다운로드가 실패합니다.

### 결과 위치

| 파일 | 내용 |
| --- | --- |
| `worker/reports/task-<taskId>_revision-<revision>_고장리포트.md` | 사람이 읽는 리포트 |
| 같은 이름의 `.json` | 입력 `command`와 최종 `result` |
| `worker/reports/live-summary.json` | 단독 실제 모델 검증 결과 |
| `worker/reports/e2e-*-summary.json` | Spring/Kafka 시나리오 결과 |
| `.local/logs/agent.jsonl` | 실행·도구·재시도 로그 |
| `.local/integration.sqlite3` | 이벤트·최종 결과·실행 통계 |

```bash
# macOS에서 리포트 폴더 열기 (실행 후 생성됨)
open worker/reports
```

`.env`, `.local`, 생성 리포트 및 모델 결과는 Git에 올리지 않습니다. 따라서 clone 직후에는 리포트 폴더가 없으며 테스트/진단을 실행하면 생성됩니다.

### 기록된 검증 결과

| 검증 | 결과 | 범위 |
| --- | --- | --- |
| Agent 회귀·하네스 테스트 | 35개 통과 | 테스트 대역 사용, 정확도 평가 아님 |
| Spring/Kafka 혼합 분석 | PCB 2장 + 기계 1장, 최종 JSON·리포트·DB·SSE 확인 | 실제 모델·MCP·LLM |
| 찌그러짐 분석 | `roof-dent` → 파손 해석 → 리포트 확인 | 실제 모델·MCP·LLM |
| 추가 요청 상한 | 초기 + 추가 3회 후 실패, 4차 요청 없음 | 실제 통합 시나리오 |
| 업무 연결 | 추천 부품 → 재고 조회 → 조달 제안·승인 → 시험 주문 취소 | 개발 DB·가상 재고, 실제 MCP |
| 해석/RAG 연결 | 4개 서버 7개 도구 호출 | 제공한 시험 라벨·검색어 |
| 백엔드 | 별도 저장소 테스트 25개 및 11장 접수 거절 확인 | 백엔드 로컬 변경 포함 |

상세 산출물 이름은 [통합 현황](INTEGRATION_STATUS.md)에 기록했습니다. 실제 모델 100장 부하 시험, 프론트 화면 전체 검증, 청소기 진단 정확도 평가 및 최신 MCP 신규 필드 검증은 아직 하지 않았습니다.

## 주요 코드

| 경로 | 역할 |
| --- | --- |
| `worker/main.py` | Kafka 수신·진행/결과 발행·저장된 결과 재전송 |
| `worker/graph.py` | 스켈레톤 그래프·분기·진단 판단 |
| `worker/tools.py`, `enrichment.py` | 모델·해석 MCP·RAG·LLM 연결 |
| `worker/harness.py` | 호출/시간/그래프 상한·재시도·노드 관측 |
| `worker/journal.py` | SQLite 상태·이벤트 저장·중복/재접수 검증 |
| `worker/observability.py` | 구조화 로그 |
| `worker/report.py` | JSON·Markdown 저장 |
| `worker/business_gateway.py` | Spring 재고 요청을 업무 MCP에 맞게 변환 |
| `tests/`, `worker/*check*.py`, `worker/verify_live.py` | 회귀·실제 연결 검증 |
