# 하네스 3단계: 실행 한도 보완

- 날짜: 2026-10-21
- 브랜치: `feat/harness-hardening`, 기준 커밋 `c172acb`
- 상태: 구현·검증 완료, 사용자 확인 및 브랜치 푸시 승인 완료.

## 사용자 추가 입력과 내부 호출을 구분

**추가 사진/자료 입력은 작업 전체에서 최대 3회다.** 최초 접수는 제외한다.
매번 다른 사진을 제출해도 횟수는 초기화되지 않는다. 3차 추가 자료를 분석한 뒤에도 진단이 안 되면
`INCONCLUSIVE_AFTER_THIRD_PHOTO`로 끝나며 4차 요청을 만들지 않는다.
기존 Spring·Agent의 재접수 계약과 제한은 그대로 유지했다.

이번에 추가한 동일 입력 반복 제한은 Agent 내부의 도구 호출 제어다. 사용자 재입력 3회와 별도다.

| 한도 | 단위와 기본값 | 초과 시 |
| --- | --- | --- |
| 추가 입력 | 최초 제외, 작업 전체에서 최대 3회 | 기존 추가 자료 종료/재접수 거절 |
| 그래프 진행 | Kafka 이벤트 실행당 최대 20 super-step | `GRAPH_STEP_LIMIT` |
| 노드 실행 | 이벤트 실행당 모든 분기의 노드 실행 합계 최대 20회 | `NODE_EXECUTION_LIMIT` |
| 전체 도구 호출 | 사진 수 기반 기존 예산: `min(2000, max(30, (장수*5+10)*3))` | `TOOL_CALL_LIMIT` |
| 동일 도구·동일 입력 | 이벤트 실행당 최초 시도·재시도를 합쳐 최대 3회 | `TOOL_REPEAT_LIMIT` |
| 전체 시간 | 이벤트 실행당 1,800초 | `RUN_TIMEOUT` |
| 개별 호출 | 시도당 90초, 일시 오류 추가 재시도 최대 2회 | 직접 LLM 호출은 `TOOL_TIMEOUT`; 필수 이미지 도구는 기존 `REQUIRED_TOOL_FAILED`와 도구 오류 종류 기록 |

새 사진·다른 입력은 별도 도구 호출로 인정하므로 사진 4장째라는 이유로 막지 않는다.
동일 입력은 함수의 위치/키워드 인자를 기본값까지 정규화한 값으로 판별한다. 이미지 도구는 첨부 파일 경로가 입력이므로
파일 바이트가 같더라도 서로 다른 첨부 경로는 다른 입력이다. 내용 유사성/이미지 중복 검출 기능은 아니다.
일반 MCP 호출 래퍼는 실제 도구 이름과 인자를 함께 구분한다. 입력 해시는 메모리에만 보관하고 로그에 쓰지 않는다.

기계 분석의 기존 래퍼는 표면·찌그러짐 도구를 함께 호출하므로 전체 예산에서 최대 2회분으로 보수적으로 계산한다.
동일 입력 시도는 래퍼 기준 3회이며, `calls`는 가중 호출 수, `tool_attempts`는 래퍼 시도 수다.
외부 MCP 내부의 추가 LLM 호출까지 세는 수치는 아니다.

## 교재와의 대응

`AI-Agent Capstone(+MCP).pdf` 인쇄 71쪽 표 6-3은 최대 8턴·120초·같은 도구 3회를 권장하며,
팀이 값을 변경하면 Harness 명세에 남기도록 한다.
이 프로젝트는 고정된 9개 노드에서 사진별 분석을 반복하므로, 8턴 대신 **20 super-step + 노드 실행 합계 20회**를 사용한다.
시간은 기존 다중 사진·로컬 추론 환경에 맞춘 1,800초를 유지한다.
같은 도구 전체 3회 대신 **같은 도구+같은 입력 3회와 사진 수 기반 전체 호출 예산**을 적용한다.
이는 교재 숫자를 그대로 적용한 구현이 아니라 프로젝트 정책으로 조정한 값이다.

카운터와 시간 한도는 Kafka 이벤트마다 새로 시작한다. 사람의 추가 제출 대기 시간이나 여러 이벤트를 합친 작업 전체 시간 한도는 아니다.
별개로 사용자 추가 입력 횟수는 영속 상태·계약으로 작업 전체에 걸쳐 유지한다.

## 변경 범위

- `worker/harness.py`: 동일 입력 반복·노드 실행 상한, 세부 실패 코드, 실제 시작한 재시도 집계, 실행 통계.
- `worker/config.py`, `.env.example`: `MAX_NODE_EXECUTIONS=20`, `MAX_SAME_INPUT_ATTEMPTS=3` 설정 추가.
- `worker/enrichment.py`: 공용 래퍼에서 실제 MCP 도구명을 식별.
- `worker/main.py`, `report.py`: 한도별 실패 결과, JSON 통계·Markdown 설명/사용량 표.
- `worker/verify_limits.py`: 합성 도구/그래프로 한도 실패 6종을 재현하고 리포트 저장.
- `worker/e2e_check.py`: 재요청 시나리오에서 서로 다른 이미지 파일을 제출하도록 검증 개선.
- `worker/graph.py`, 프롬프트, 모델/MCP 서버, 백엔드 소스는 수정하지 않았다.

## 검증 결과

자동 테스트 **85개 통과**. 대역을 사용한 테스트와 실제 호출 검증을 구분한다.

| 검증 | 결과 |
| --- | --- |
| 동일 입력 반복·재시도 | 최초 + 재시도 2회가 합계 3회. 이후 동일 입력 호출은 실행 전에 차단 |
| 위치/키워드 호출·동시 호출 | 동일 인자로 인식, 동시에 4번 요청해도 3번만 실행 |
| 서로 다른 입력 100개 | 도구 대역 100회 실행, 반복 한도에 잘못 걸리지 않음. 실제 모델 100장 부하 검증은 아님 |
| 노드/시간/호출 한도 | 각각 실패 JSON·Markdown 생성, 사진 재요청으로 넘기지 않음 |
| 잘못된 설정 | 0/음수 횟수, NaN/무한 시간 거절 |
| 사용자 재입력 | 새 사진 ID·크기·해시를 추가해도 요청 3회 유지, 4회차 명령 거절 |
| 합성 장애 시연 | 호출/반복/노드/super-step/전체 시간/도구 시간 6종 실패 리포트 생성 |

### 실제 Spring·Kafka 검증

실행 중이던 워커가 유휴·Kafka lag 0인 것을 확인하고 최신 코드로 재시작했다.
전용 개발 DB에 접수하여 Spring → Kafka → Agent → 실제 모델·MCP·LLM → Kafka → Spring DB/API/SSE를 검증했다.

| 시나리오 | 접수 / 작업 | 결과 | 리포트 (`worker/reports/`) |
| --- | --- | --- | --- |
| PCB 2장 + 기계 1장 | 22 / 18 | SUCCEEDED, 한도 오탐 없음 | `task-18_revision-1_고장리포트.md/json` |
| 기계 단독 | 23 / 19 | SUCCEEDED, 한도 오탐 없음 | `task-19_revision-1_고장리포트.md/json` |
| 서로 다른 사진: 최초 + 추가 3회 | 24 / 20 | FAILED / `INCONCLUSIVE_AFTER_THIRD_PHOTO`, revision 4, 요청 3개 | `task-20_revision-4_고장리포트.md/json` |

재요청 시나리오는 크기가 다른 흰색 시험 이미지 4개를 사용했고, Spring에 전달된 첨부 SHA-256 4개가 전부 다른 것을 확인했다.
Spring 조회 API 및 PostgreSQL 결과를 Agent JSON과 대조했다. 모든 이벤트는 APPLIED이며 진행/완료·실패 SSE도 수신했다.
프론트 브라우저 화면 렌더링·진단 정확도는 이 검증 범위가 아니다.

- 실제 통합 결과: `worker/reports/harness-step3-full-e2e-summary.json`
- 합성 장애 시연 결과: `worker/reports/limit-check-summary.json`
- 최신 워커 로그: `/tmp/harness-step3-kafka-worker.log`, 구조화 로그 `.local/logs/agent.jsonl`

```bash
uv run python -m pytest -q
# 합성 도구/로컬 이벤트. 실제 모델·Spring·Kafka를 호출하지 않음
uv run python -m worker.verify_limits
# 실제 연동: 최신 워커와 각 서비스가 실행 중이어야 함
uv run python -m worker.e2e_check --scenario mixed
uv run python -m worker.e2e_check --scenario mechanical
uv run python -m worker.e2e_check --scenario retry-limit
```

사용자 확인을 받아 다음 단계로 진행한다. 토큰·비용 예산, 문맥 요약, PII 마스킹, 중간 실행 복원, LangSmith는 아직 별도 작업이다.
외부 서버의 GPU 추론 종료까지 timeout이 보장하는 것은 아니며, 로컬 대기/협력적 비동기 실행을 취소한다.
