"""작업·노드·도구 실행을 추적하는 JSON 로그. 입력 원문과 토큰은 기록하지 않는다."""
import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOGGER = logging.getLogger("repair_agent")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            **getattr(record, "agent_fields", {}),
        }, ensure_ascii=False, allow_nan=False)


def configure_logging(level=None, log_file=None):
    level = (level or os.getenv("AGENT_LOG_LEVEL", "INFO")).upper()
    if level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        raise ValueError("AGENT_LOG_LEVEL must be DEBUG/INFO/WARNING/ERROR")
    LOGGER.setLevel(level)
    LOGGER.propagate = False
    if not any(getattr(h, "agent_console", False) for h in LOGGER.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.agent_console = True
        handler.setFormatter(JsonFormatter())
        LOGGER.addHandler(handler)
    log_file = log_file or os.getenv("AGENT_LOG_FILE")
    if log_file:
        path = Path(log_file).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        if not any(getattr(h, "baseFilename", None) == str(path) for h in LOGGER.handlers):
            handler = RotatingFileHandler(path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
            handler.setFormatter(JsonFormatter())
            LOGGER.addHandler(handler)


def context(command):
    return {"event_id": command["eventId"], "task_id": command["taskId"],
            "case_id": command["repairId"], "input_round": command["payload"]["round"],
            **{key: command[key] for key in ("correlationId", "executionId", "inputRevision")}}


def log_event(name, *, level=None, **fields):
    if level is None:
        if name in ("node.failed", "analysis.failed", "graph.limit_reached"):
            level = logging.ERROR
        elif name in ("tool.failed", "tool.retry", "event.rolled_back", "backend.command_rejected"):
            level = logging.WARNING
        else:
            level = logging.INFO
    LOGGER.log(level, name, extra={"agent_fields": fields})
