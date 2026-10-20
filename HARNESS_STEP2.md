# 하네스 2단계: 프롬프트 분리·버전 관리

- 브랜치: `feat/harness-hardening`
- 1단계 기준: `6faafbb` (사용자 승인 후 원격 푸시 완료)
- 2단계 상태: 구현·검증 완료 및 사용자 커밋·푸시 승인 완료. 대상은 `agent-server/feat/harness-hardening`입니다.
- 검증일: 2026-10-20

## 구현

| 항목 | 구현 내용 |
| --- | --- |
| 프롬프트 파일 | 사진 분류·소견 분리·리포트 생성 지시문을 `prompts/*.md`로 이동 |
| 기존 동작 보존 | 기존 `6faafbb` 코드에서 문구를 추출. 대표 입력을 치환한 3개 프롬프트가 이전 문자열과 바이트 단위로 동일함을 비교 |
| 버전 관리 | `manifest.json`에 묶음/파일 버전 `1.0.0`, 파일 SHA-256, 출력 스키마 이름 기록 |
| 실행 전 검증 | 파일 누락·해시 불일치·잘못된 치환 항목을 거절하고 `PROMPT_CONFIG_ERROR` 실패 결과·리포트 생성 |
| 실행 중 일관성 | 시작 시 파일을 메모리에 읽어 고정. 같은 실행 안에서 파일 변경이 섞이지 않음 |
| 로그 | `prompt.used`에 버전·프롬프트 해시·출력 스키마 해시·모델 기록. 기사 소견·프롬프트 본문 제외 |
| 상태·리포트 | SQLite 실행 통계와 로컬 JSON `execution.prompts`, Markdown의 실행 정보 표에 사용 이력 기록 |
| 중복 재전송 | 기존 저장 결과와 당시 버전 기록 재사용. 현재 프롬프트로 과거 이력을 덮어쓰지 않음 |

`worker/graph.py`는 수정하지 않았다. 메시지 역할, LLM 모델·temperature, 기존 Pydantic 출력 스키마,
Kafka 최종 결과 payload도 그대로다. 로컬 저장 JSON에는 기존 `command`·`result` 외에 `execution`을 추가했다.
팀 MCP 서버 내부 프롬프트는 각 서버가 관리하며 이 단계에 포함하지 않는다.

## 검증

| 검증 | 결과 |
| --- | --- |
| 전체 자동 테스트 | 68개 통과. 외부 모델은 테스트 대역 |
| 이전 프롬프트와 문자열 비교 | 사진 분류·소견 분리·리포트 생성 3종 동일 |
| 실제 PCB+기계 혼합 진단 | 도구 오류 없이 `DIAGNOSIS_COMPLETED`, JSON·Markdown 생성 |
| 실제 사진 분류 LLM | PCB 시험 사진을 `PCB`로 분류, `classify_photo` 사용 버전 기록 |
| 리포트 메타데이터 | `split_text`, `write_report`의 버전·해시·모델이 현재 파일 및 SQLite 기록과 일치 |
| 계약 | 생성된 이벤트 전부 기존 JSON Schema 검증 통과 |
| 오류·재전송 | 파일 누락/변조/형식 오류 시 모델 호출 전 실패, 실행 중 파일 변경 격리, 중복 재전송 시 원래 이력 유지 |

실제 혼합 진단은 사진 구분이 이미 지정되어 분류 프롬프트를 호출하지 않는다. 그래서 사진 분류는 별도 실제 LLM 호출로 검증했다.
첫 실제 호출 검증은 Spring 다운로드와 Kafka 전송을 로컬 입출력으로 대체했다. 이후 사용자 요청에 따라 아래의 실제 Spring·Kafka 전체 경로도 추가 검증했다.
문구 보존과 리포트 생성 검증이며, 진단 정확도가 개선됐다는 의미는 아니다.

### 로컬 산출물

- 실제 리포트: `worker/reports/task-6680833_revision-1_고장리포트.md`
- 입력·최종 결과·실행 버전: 같은 이름의 `.json`
- 이벤트: 같은 이름의 `_events.json`
- 분류 검증: `worker/reports/prompt-classification-check.json`
- 검증 요약: `worker/reports/harness-step2-summary.json`
- 실제 분류 프롬프트 로그: `.local/logs/prompt-check.jsonl`

```bash
uv run python -m pytest -q
uv run python -m worker.verify_live --scenario mixed
open worker/reports
```

프롬프트 수정 절차는 [prompts/README.md](prompts/README.md)를 따른다. 버전 상향은 리뷰 규칙이고,
파일 내용과 등록 해시의 일치 여부는 코드로 검사한다. 출력 스키마 정의는 `worker/tools.py`에 남겨두며 실제 스키마 해시를 기록한다.

## 다음 단계

사용자 확인 후 3단계 실행 한도 보완으로 진행한다. 토큰·비용 상한, 문맥 요약, PII 마스킹,
중간 상태 복원, LangSmith 연결은 아직 이번 단계의 구현 대상이 아니다.


## 추가 검증: 실제 Spring·Kafka 전체 경로 (2026-10-20)

기존 워커가 유휴 상태이고 Kafka lag가 0임을 확인한 뒤 같은 설정·consumer group으로 최신 코드를 재시작했다.
전용 개발 계정으로 실제 Spring 접수를 생성했다. 사진 다운로드·모델/MCP·LLM·Kafka·PostgreSQL·API·SSE를 대체하지 않았다.

| 시나리오 | 접수 / 작업 | 결과 | 리포트 (`worker/reports/`) |
| --- | --- | --- | --- |
| PCB 2장 + 기계 1장 | 19 / 15 | SUCCEEDED. Spring 후보 2개·근거 10개 저장 | `task-15_revision-1_고장리포트.md/json` |
| 기계 단독 | 20 / 16 | SUCCEEDED. Spring 후보 3개·근거 7개 저장 | `task-16_revision-1_고장리포트.md/json` |
| 빈 시험 이미지 + 추가 제출 3회 | 21 / 17 | FAILED / `INCONCLUSIVE_AFTER_THIRD_PHOTO`. 요청 3개·revision 4, 4차 요청 없음 | `task-17_revision-4_고장리포트.md/json` |

- Spring 조회 API와 PostgreSQL의 저장 결과를 Agent JSON과 직접 비교했다. 정상 진단의 payload가 일치한다.
- 실행 중 RUNNING 상태에서 진행 SSE와 최종 완료/실패 SSE 수신을 확인했다.
- 3회 추가 제출에서 동일 제출의 중복 요청이 같은 응답을 반환하는 것도 확인했다.
- 해당 작업의 수신 이벤트 11/8/38개가 모두 APPLIED이며 무시/DLT로 분류된 이벤트는 없었다.
- 생성된 리포트의 프롬프트 버전·해시가 현재 파일과 일치한다.
- 검증 기록: `worker/reports/harness-step2-full-e2e-summary.json`, `e2e-mixed-summary.json`, `e2e-mechanical-summary.json`, `e2e-retry-limit-summary.json`.
- 프론트 브라우저의 실제 렌더링은 확인하지 않았다. 화면에서 사용하는 API·SSE까지 확인한 범위다.
- 이 추가 검증은 정상/재요청 경로다. 1단계의 MCP 접속 장애 주입은 앞서 Agent 단독 경로에서 검증했으며, 이번 Spring E2E에는 장애를 주입하지 않았다.
- 검증 후 최신 Agent 워커를 실행 상태로 유지했다. 검증 완료 후 사용자에게 2단계 푸시 승인을 받았다.
