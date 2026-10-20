"""설정값 모음. 전부 환경변수로 바꿀 수 있다. 키·토큰은 .env에만 넣는다."""

import os
from pathlib import Path

from dotenv import dotenv_values

HERE = Path(__file__).parent
ROOT = HERE.parent  # 레포 맨 위
for k, v in dotenv_values(ROOT / ".env").items():
    if v:  # .env에 값이 있으면 셸에 남은 옛 키보다 우선. 빈 칸은 무시
        os.environ[k] = v

# Kafka: contract/INTEGRATION.md 계약 그대로 (backend 레포에서 복사)
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
COMMANDS_TOPIC = "diagnosis.commands.v2"  # Spring → 우리
EVENTS_TOPIC = "diagnosis.events.v2"  # 우리 → Spring
DLT_TOPIC = "diagnosis.commands.v2.dlt"  # 처리 못 한 명령
GROUP_ID = os.getenv("GROUP_ID", "langgraph-diagnosis-v2")

# Spring 내부 API (사진 다운로드)
SPRING_URL = os.getenv("SPRING_BASE_URL", "http://localhost:8080")
SPRING_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")

# 로컬 검증 명령에서만 사용하는 예시 사진. 다운로드 실패 시 자동 대체하지 않는다.
SAMPLE_PHOTOS = [str(HERE / "samples/pcb_sample.jpg"), str(HERE / "samples/mech_sample.jpg")]

# MCP 서버
MECH_MCP_URL = os.getenv("MECH_MCP_URL", "http://127.0.0.1:9104/mcp")  # mechanical-mcp, 미리 켜 둔다
RAG_MCP_URL = os.getenv("RAG_MCP_URL", "http://127.0.0.1:8014/mcp")  # Rag_mcp 레포의 rag 서버. 꺼져 있으면 검색 없이 진행
PCB_MCP_SERVER = str(ROOT / "mcp_servers/pcb/mcp_server.py")  # stdio, 부를 때마다 실행
PCB_MCP_PYTHON = str(ROOT / "mcp_servers/pcb/.venv/bin/python")  # setup.sh가 만든다

# GPT (OPENAI_API_KEY가 없으면 규칙으로 대신한다)
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-4o-mini")
USE_GPT = bool(os.getenv("OPENAI_API_KEY"))

MAX_PHOTO_ROUNDS = 3  # 추가 사진 요청 최대 횟수 (계약)
MAX_RETRY = 2  # 도구 일시 오류의 추가 재시도 횟수
MODEL_VERSION = "skeleton-0.2"

# 통합 추가분. 원본 모델과 완료 판단은 그대로 두고 연결·실행 제어만 설정한다.
OBSERVATION_MCP_URL = os.getenv("OBSERVATION_MCP_URL", "http://127.0.0.1:8011/mcp")
PCB_INTERPRET_MCP_URL = os.getenv("PCB_INTERPRET_MCP_URL", "http://127.0.0.1:8012/mcp")
DAMAGE_MCP_URL = os.getenv("DAMAGE_MCP_URL", "http://127.0.0.1:8013/mcp")
PCB_MCP_SERVER = os.getenv("PCB_MCP_SCRIPT", PCB_MCP_SERVER)
PCB_MCP_PYTHON = os.getenv("PCB_MCP_PYTHON", PCB_MCP_PYTHON)
TOOL_TIMEOUT = float(os.getenv("TOOL_TIMEOUT", "90"))
RUN_TIMEOUT = float(os.getenv("RUN_TIMEOUT", "1800"))
MAX_TOOL_CALLS = int(os.getenv("MAX_TOOL_CALLS", "2000"))
RECURSION_LIMIT = int(os.getenv("RECURSION_LIMIT", "20"))
MAX_NODE_EXECUTIONS = int(os.getenv("MAX_NODE_EXECUTIONS", "20"))
MAX_SAME_INPUT_ATTEMPTS = int(os.getenv("MAX_SAME_INPUT_ATTEMPTS", "3"))
STATE_PATH = Path(os.getenv("AGENT_STATE_PATH", str(ROOT / ".local/integration.sqlite3")))
KAFKA_MAX_POLL_INTERVAL_MS = max(
    int((RUN_TIMEOUT + 180) * 1000),
    int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "0")))

KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
