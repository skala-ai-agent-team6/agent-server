# AI Agent Capstone AGENT SERVER 

로봇청소기 수리 진단 Agent의 **로컬 모의 실행 골격**이다. 기존 상위 프로젝트의 기능명세·DB 설계는 수정하지 않는다.

## 현재 구현
- FastAPI 요청 검증, LangGraph 분석 선택 → 모의 도구 호출 → 결과 종합.
- 추가 사진 interrupt/resume, 동일 작업 상태 유지, 최대 3차 재분석 후 실패 종료.
- 진단 완료와 별도인 발주 승인 대기 및 승인/거절 모의 처리.
- 일시 오류 최대 2회 재시도, 작업당 도구 호출 최대 24회, 분석 단계 경과 시간 검사.
- 작업 ID 중복 요청 방지, 다른 본문 재사용·지난 사진 차수·중복 승인 거부.

**실제 LLM, 이미지 분석, MCP 네트워크, Spring API, Kafka, SSE, 업무 DB, 실제 발주는 연결하지 않았다.** 현재 라우터는 키워드 규칙이며 결과는 `[MOCK]` 문구가 붙은 시연 데이터다. 사진을 읽지 않는다. `mock_readable`은 테스트 분기를 선택하는 값이다.

## 실행
Python 3.12 이상과 uv 사용:

```bash
uv sync
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
```

http://127.0.0.1:8000/docs 에서 실행할 수 있다.

```bash
uv run pytest -q
curl -X POST http://127.0.0.1:8000/internal/diagnoses \
  -H 'Content-Type: application/json' -d @examples/diagnosis.json
```

위 예제는 첫 사진 요청에서 멈춘다. 다음 API로 재개한다:

```bash
curl -X POST http://127.0.0.1:8000/internal/diagnoses/demo-1/photos \
  -H 'Content-Type: application/json' \
  -d '{"round_no":1,"attachments":[{"id":"photo-2","target":"MOTOR","object_key":"mock/clear.jpg","mock_readable":true}]}'
```

진단이 완료되고 모의 발주 승인 대기로 전환된다:

```bash
curl -X POST http://127.0.0.1:8000/internal/diagnoses/demo-1/mock-approval \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"mock-order-demo-1","approved":true}'
```

## 파일 역할
- `app/models.py`: 팀과 합의할 입력 계약 초안. ID는 문자열.
- `app/workflow.py`: LangGraph State·노드·분기·interrupt·실행 제한.
- `app/adapters.py`: 전문 분석/검색 인터페이스와 모의 구현.
- `app/main.py`: 로컬 실행 API 및 중복·잘못된 재개 검증.
- `tests/test_workflow.py`: 실패·재개·승인·중복 요청 시나리오.

## 현재 제한과 실제 연동 순서
1. 각 MCP 도구명·입출력 형식 및 Spring 요청/재개 계약을 합의한다. 이 저장소 API는 초안이며 기존 Spring 공개 API와 별개다.
2. 라우팅·종합을 LLM 구조화 출력으로 교체하고 모의 도구를 실제 MCP 클라이언트로 교체한다. 매뉴얼 검색·부품 조회도 연결한다.
3. `agent_execution`, 후보·근거·사진 요청·완료 상태를 Spring 내부 API로 저장한다. 현재 실행 기록은 그래프 상태 안에만 있으며 DB 스키마와 동일한 저장 DTO는 아니다.
4. InMemorySaver를 PostgreSQL 체크포인트로 교체하고 `checkpoint_ref`를 업무 job과 연결한다. **현재 서버 재시작 시 모든 작업이 사라진다.**
5. Spring 서비스 인증과 승인 이벤트 검증을 구현한다. **mock-approval은 테스트 전용이며 승인 권한 검증이 없으므로 외부 공개하지 않는다.** 실제 승인 주체는 Spring이 검증한다.
6. 현재 API는 동기 실행·단일 프로세스·공유 잠금이다. Kafka worker, durable job claim/lease, 비동기 202 응답, 콜백 멱등성, SSE 중계는 팀 연동 단계에서 구현한다.
7. 현재 시간 제한은 도구 반환 전후 경과 검사다. 멈춘 네트워크 호출을 취소하지 못한다. 실제 MCP 연결 시 요청 timeout·취소와 대기 시간을 제외한 누적 실행 예산을 구현한다.
8. 실제 프롬프트 버전 관리, 토큰 예산, 개인정보 마스킹, 오류/성능 평가를 추가한다.

진단 상태(`RUNNING/WAITING_INPUT/SUCCEEDED/FAILED`)와 발주 대기(`workflow_status`)는 구분한다. 추가 사진 횟수는 초기 사진을 제외한다. interrupt 노드는 재개 시 다시 실행되므로 요청 횟수 증가와 같은 변경은 앞선 별도 노드에서 처리한다.

공식 참고: https://docs.langchain.com/oss/python/langgraph/interrupts
