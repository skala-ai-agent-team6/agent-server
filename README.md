# AI Agent Capstone AGENT SERVER

구분된 사진과 기사 소견을 받아 수리 진단을 진행하는 **로컬 모의 이벤트 처리 서버**다.
상위 기능명세·DB 설계 문서는 수정하지 않는다. 입력 구분과 이벤트 재분석은 대화에서 확정한 최신 방향을 따른다.

## 백엔드 v2 연동 범위

기준: backend commit `63e0f968f76c893ec9554589d07b51a81e0460ef`의 `docs/INTEGRATION.md`와 `docs/kafka-event.schema.json`.

- `POST /internal/backend-v2/events`: 백엔드의 원본 v2 명령 envelope를 받아 내부 입력으로 변환한다.
- `DIAGNOSIS_REQUESTED`와 `DIAGNOSIS_RESUMED`, decimal string 업무 ID, UUID 이벤트/실행 ID를 검증한다.
- `correlationId`, `inputRevision`, `executionId`를 결과에 그대로 전달한다. 결과 sequence는 실행별 1부터 증가한다.
- STARTED → 현재 실행의 TOOL_EXECUTED → COMPLETED/FAILED 결과 목록을 생성한다. 과거 실행의 도구 기록은 재발행하지 않는다.
- 결과 UUID·순서는 SQLite에 저장한 후 재전달에도 그대로 유지한다. v2 원문도 상태에 보존해 eventId의 다른 본문 재사용을 거부한다.
- 결과는 summary/modelVersion/repairMethod/candidates/recommendedParts/evidence 형식이다. 모의 부품 ID를 지어내지 않으므로 recommendedParts는 빈 배열이다.
- `GET /internal/backend-v2/outgoing-events`: 저장된 v2 결과 확인. 현재 broker에 전송하지는 않는다.
- `app/backend_v2.py`: Kafka consumer에서 재사용할 `process_backend_command()` 진입점 및 계약 변환 함수.
- 공식 백엔드 schema 사본은 `tests/fixtures`에 고정하고 실제 출력의 UUID/date-time·필드·형식을 검증한다.

**이번 변경에서 보류한 세 가지:**
1. 백엔드 사진은 현재 `GENERAL`이다. 이를 임의로 PCB/MECHANICAL로 분류하지 않는다. 구분되지 않은 v2 사진은 409로 거부한다. 예제의 명시적 `PCB`는 변환 테스트용이며 현재 Spring 업로드 결과를 그대로 표현한 것은 아니다.
2. 접수 시 errorCodes/firmware 필수 여부는 백엔드 담당 범위다. v2 원문에 있으면 보존하되 Agent가 필수로 요구하거나 값을 만들어 넣지 않는다.
3. PHOTO_REQUESTED의 필수 checkpoint 의미는 미합의다. 대기 경로를 v2로 내보내려 하면 명시적 409를 반환하고 상태·출력 저장도 rollback한다. 가짜 checkpoint나 interrupt를 도입하지 않는다. **사진 재요청 v2 종단간 연결은 아직 불가**하며 로컬 이벤트 API로 동작을 확인한다.

모의 v2 완료 흐름 확인:

```bash
curl -X POST http://127.0.0.1:8000/internal/backend-v2/events \
  -H 'Content-Type: application/json' -d @examples/backend-v2.json
curl http://127.0.0.1:8000/internal/backend-v2/outgoing-events
```

Kafka 토픽은 명령 `diagnosis.commands.v2`, 결과 `diagnosis.events.v2`, consumer group은 `langgraph-diagnosis-v2`, 메시지 key는 taskId다. **이번에는 메시지 계약 변환까지 구현했으며 실제 Kafka consumer/producer·offset·DLT는 미구현**이다. 현재 출력 목록은 그래프 완료 시 한 번에 만들어지므로 실시간 진행 이벤트 전송도 미연결이다. 실제 연동 때 backend mock AI를 꺼야 한다.

## 실행 흐름

```text
Spring 저장 → Kafka 이벤트 → Agent 이벤트 처리 함수
  → 구분된 사진·자연어 소견으로 필요한 분석 선택
  → 분석 MCP 결과 수집 → 확신·단서 충분성 판단
     ├─ 충분: RAG 검색 → 리포트 JSON → 완료 이벤트 → END
     └─ 부족:
         ├─ 추가 요청 3회 미만: 추가 자료 요청 이벤트 → END
         └─ 3차 제출 후에도 부족: 실패·사람 검토 이벤트 → END

Spring이 추가 사진·설명을 저장 → 같은 작업 ID로 새 Kafka 이벤트
  → 기존 정보 + 새 자료를 입력으로 그래프를 다시 실행
```

**interrupt, Command(resume), LangGraph 체크포인트 대기, Agent의 발주 승인 대기는 사용하지 않는다.**
진단 이후 창고 재고 조회·재고가 없을 때 승인 후 발주·기사 최종 보고서 처리는 Spring/업무 MCP 담당이다.

## 현재 구현과 범위

- FastAPI `/internal/events`가 최초/추가 이벤트를 받는다. 실제 Kafka consumer를 대신하는 로컬 테스트 입구다.
- `EventProcessor.process()`가 전송 수단과 독립된 이벤트 처리 진입점이다. 실제 consumer에서 같은 함수를 호출할 수 있다.
- PCB 사진과 찌그러짐/기계 사진은 `PCB`, `MECHANICAL`로 구분하고 원본 대신 `photo_id`를 받는다.
- 기사 소견은 자연어 `symptom`이다. 별도 `error_codes` 입력은 없다.
- 사진 종류 재분류는 하지 않는다. 자연어 SW 단서 추출은 임시 키워드 규칙이다.
- 분석 결과 `sufficient=false` 또는 `confidence<0.5`일 때 해당 영역의 사진/설명 추가 요청.
- 초기 입력은 0차, 추가 요청은 최대 3차. 3차 자료로 성공할 수 있으며 여전히 부족하면 실패한다.
- 일시 오류 최대 2회 재시도, 작업 전체 도구 호출 최대 24회. 서버 오류 재시도와 사진 요청 횟수는 독립이다.
- SQLite에 작업 상태·처리한 이벤트·출력 이벤트를 한 트랜잭션으로 저장한다. 동일 이벤트 재전달 시 기존 응답을 그대로 돌려주고 출력 이벤트를 중복 생성하지 않는다.
- 잘못된 차수·다른 접수 ID·종료 작업·입력 변경 없는 추가 제출·기존 사진 누락·event_id 본문 충돌은 거부한다.
- 최종 리포트는 Pydantic으로 검증한다.

**실제 LLM, MCP 통신, 사진 분석, Spring API, Kafka 송수신, SSE, 업무 DB는 미연결이다.**
`mock_readable`과 `mock_confidence`는 가짜 도구의 결과를 제어한다. 실제 이미지 품질이나 진단 확률이 아니다.
실제 YOLO 어댑터 연결 시 모델 지원 범위·탐지 없음·개별 박스 점수와 진단 확신의 차이를 반영해야 한다.
SW는 실제 규격이 미정이므로 현재 모의 도구는 추가 설명 필요로 반환한다. 자연어로 에러 코드를 지어내지 않는다.

## 실행

Python 3.12 이상과 uv:

```bash
uv sync
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
```

http://127.0.0.1:8000/docs 에서 확인한다. 기존 가상환경을 옮겼다면 `uv sync`로 경로를 갱신한다.

```bash
# 최초 요청: 점수가 낮아 추가 자료 요청 이벤트를 반환하고 그래프는 종료
curl -X POST http://127.0.0.1:8000/internal/events \
  -H 'Content-Type: application/json' -d @examples/diagnosis.json

# 새 이벤트: 같은 작업의 기존 자료 + 추가 사진으로 다시 분석
curl -X POST http://127.0.0.1:8000/internal/events \
  -H 'Content-Type: application/json' -d @examples/additional-input.json

# 마지막 작업 상태와 저장된 출력 이벤트 확인
curl http://127.0.0.1:8000/internal/diagnoses/demo-1
curl http://127.0.0.1:8000/internal/outgoing-events

uv run pytest -q
```

같은 예제 파일을 다시 보내면 처음 처리했던 결과를 반환한다. 새 작업을 시험하려면 새 task_id와 event_id를 사용한다.
서버 기본 상태 파일은 `.local/agent-state.sqlite3`이며 Git에서 제외한다.
별도 경로는 `AGENT_STATE_PATH=/원하는/경로.sqlite3`로 지정한다. 업무 PostgreSQL과 별개인 로컬 개발용 저장소다.

## 이벤트 계약 초안

입력은 `DiagnosisEvent`:
- `event_id`: Kafka 재전달에도 동일하게 유지하는 메시지 식별자.
- `event_type`: `DIAGNOSIS_REQUESTED` 또는 `ADDITIONAL_INPUT_SUBMITTED`.
- `task_id`, `case_id`: Spring이 만든 같은 진단 작업·접수 ID. 문자열.
- `round_no`: 최초 0, 추가 제출 1~3. 마지막 추가 요청의 차수와 일치해야 한다.
- `symptom`: 기존 설명과 추가 소견을 반영한 자연어 텍스트.
- `attachments`: 기존 사진과 새 사진을 포함한 누적 목록. 기존 photo_id·대상 등 메타데이터는 유지한다.

출력은 작업 결과의 `outgoing_event`:
- `ADDITIONAL_INPUT_REQUESTED`: `round_no`, `targets`, `instructions`.
- `DIAGNOSIS_COMPLETED`: Pydantic 형식의 리포트.
- `DIAGNOSIS_FAILED`: 오류 코드와 `manual_review_required=true`.

본문의 `status=WAITING_INPUT`은 **업무상 추가 제출 대기**를 뜻한다. 그래프 자체는 END에 도달한 상태다.
추가 자료가 들어오면 체크포인트를 재개하지 않고 새 그래프 실행을 시작한다.

## 파일 역할

- `app/models.py`: 이벤트·사진·최종 리포트 데이터 계약.
- `app/workflow.py`: 노드·분기·도구 실행·추가 요청·종료.
- `app/adapters.py`: 분석/검색 도구 인터페이스 및 가짜 구현.
- `app/service.py`: 이벤트 중복 방지·차수 검증·SQLite 상태/출력 이벤트 저장.
- `app/backend_v2.py`: Spring v2 명령 검증·원문 보존·결과 envelope 변환.
- `app/main.py`: 로컬 테스트 API. 기존 `/photos`, `/mock-approval`, 최초 `/internal/diagnoses` POST는 제거했다.
- `examples/`: 최초 요청과 추가 자료 이벤트 예시.
- `tests/test_workflow.py`: 낮은 확신·재분석·3회 제한·중복·재시작·오류·종료 검증.

## 실제 연동 전에 남은 작업

1. Kafka 토픽·이벤트 형식, Spring 상태 저장/횟수 관리 책임을 담당자와 합의한다. 로컬 계약은 팀 확정 규격이 아니다.
2. Kafka consumer와 출력 이벤트 publisher를 연결한다. 현재 출력은 SQLite에만 보관하며 Spring에 실제 발행하지 않는다. 발행 확인·재전송·consumer offset 커밋 정책이 필요하다.
3. 실제 MCP 클라이언트로 교체하고 사진 ID를 원본에 연결한다. 현재 모의 도구는 순차 실행이다. 독립 MCP의 병렬 호출은 결과 정규화 후 구현한다.
4. 자연어 단서 추출과 최종 리포트를 LLM 구조화 출력으로 연결한다. 현재 리포트는 고정 모의 문구다.
5. 실제 네트워크 timeout·취소를 구현한다. 지금은 분석 단계 반환 전후의 120초 경과 검사이므로 멈춘 호출을 중단하지 못하고, 여러 이벤트의 누적 시간도 제한하지 않는다.
6. 서비스 인증, 입력 권한, 민감 정보 마스킹을 연결한다. 이 로컬 API는 인증이 없으므로 localhost에서만 실행한다.
7. 현재 로컬 저장소는 장기 도구 호출 동안 DB 쓰기 잠금을 유지한다. 분산 worker 운영 전 durable claim/lease와 실행 외부 부작용의 멱등성으로 교체한다. 입력 이벤트의 중복 방지 자체가 외부 API 실행의 exactly-once를 보장하지는 않는다.
8. 재처리 가능한 인프라 오류의 Kafka 재시도·최종 실패 정책을 합의한다. 현재는 내부 재시도 소진 후 실패 이벤트를 생성한다.
