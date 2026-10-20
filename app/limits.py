"""업무 제한과 LangGraph 실행 제한을 구분한다."""

# 초기 접수는 제외한다. 추가 자료 요청 1~3차를 허용하고 4차는 만들지 않는다.
MAX_ADDITIONAL_REQUESTS = 3

# 한 이벤트 처리 중의 super-step 상한. 사진 요청 횟수와 다르다.
# 20은 현재 개발 기본값이며 교재가 요구하는 고정 숫자가 아니다.
DEFAULT_GRAPH_STEP_LIMIT = 20
