"""Kafka 워커. 실행: uv run python -m worker.main

1) diagnosis.commands.v2 에서 명령을 받는다
2) 그래프를 돌린다 (중간 진행 상황은 바로바로 diagnosis.events.v2 로 보낸다)
3) 마지막 결과(완료/사진요청/실패)를 보낸다
4) 그다음에야 Kafka에 '처리했다'(commit)를 기록한다 → 중간에 죽으면 다시 받는다
"""

import asyncio
import json
import logging
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from . import config as C
from . import report
from .observability import configure_logging
from langgraph.errors import GraphRecursionError
from .graph import build_graph

log = logging.getLogger("worker")
GRAPH = build_graph()
from . import harness
from .journal import Journal, CommandConflict
from .tool_policy import RequiredToolFailure
from .prompts import PromptConfigError
from jsonschema.exceptions import ValidationError


async def flush(journal, command, producer):
    rows = journal.db.execute("SELECT seq,body FROM events WHERE job=? AND sent=0 ORDER BY seq",
                              (command["eventId"],)).fetchall()
    for seq, body in rows:
        await producer.send_and_wait(C.EVENTS_TOPIC, body.encode(), key=command["taskId"].encode())
        with journal.db:
            journal.db.execute("UPDATE events SET sent=1 WHERE job=? AND seq=?", (command["eventId"], seq))


async def handle(command: dict, producer: AIOKafkaProducer, journal=None):
    """완료 결과·이벤트를 먼저 저장하고 전송. 전송 재시도에서는 모델을 다시 부르지 않는다."""
    owned = journal is None
    journal = journal or Journal(C.STATE_PATH)
    try:
        result = journal.begin(command)
        if result is None:
            lock = asyncio.Lock()

            async def emit(kind, payload):
                async with lock:
                    with journal.db:
                        journal.append(command, kind, payload)
                    try:
                        await flush(journal, command, producer)
                    except Exception as exc:
                        log.warning("진행 이벤트 전송 보류: %s", type(exc).__name__)

            await emit("DIAGNOSIS_STARTED", {"stage": "STARTED"})
            try:
                metrics = {}
                state = await harness.invoke(GRAPH, command, emit, metrics)
                result = state["result"]
                if result["eventType"] == "PHOTO_REQUESTED":
                    result["payload"]["requestKey"] = str(uuid.uuid5(uuid.NAMESPACE_URL, command["eventId"] + ":photo"))
                journal.finish(command, result)  # JSON 계약 위반도 아래 실패 결과로 처리
            except Exception as exc:
                log.error("진단 실행 실패: %s", type(exc).__name__)
                code = ("GRAPH_STEP_LIMIT" if isinstance(exc, GraphRecursionError) else
                        exc.code if isinstance(exc, harness.LimitExceeded) else
                        "REQUIRED_TOOL_FAILED" if isinstance(exc, RequiredToolFailure) else
                        "PROMPT_CONFIG_ERROR" if isinstance(exc, PromptConfigError) else
                        "TOOL_TIMEOUT" if isinstance(exc, harness.ToolCallTimeout) else
                        "RUN_TIMEOUT" if isinstance(exc, TimeoutError) else "AGENT_ERROR")
                result = {"eventType": "DIAGNOSIS_FAILED", "payload": {"errorCode": code}}
                journal.finish(command, result)
            finally:
                with journal.db:
                    journal.db.execute("CREATE TABLE IF NOT EXISTS executions (event_id TEXT PRIMARY KEY, metrics TEXT NOT NULL)")
                    journal.db.execute("INSERT OR REPLACE INTO executions VALUES (?,?)",
                                       (command["eventId"], json.dumps(metrics)))
        # 재전송 시 현재 프롬프트 버전을 덧씌우지 않고 원래 실행 기록을 사용한다.
        execution = {}
        if journal.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='executions'").fetchone():
            row = journal.db.execute("SELECT metrics FROM executions WHERE event_id=?",
                                     (command["eventId"],)).fetchone()
            if row:
                execution = json.loads(row[0])
        path = report.save(command, result, execution=execution)
        await flush(journal, command, producer)
        if result["payload"].get("errorCode") in harness.LIMIT_CODES | {
                "AGENT_ERROR", "GRAPH_STEP_LIMIT", "REQUIRED_TOOL_FAILED", "PROMPT_CONFIG_ERROR", "TOOL_TIMEOUT"}:
            await producer.send_and_wait(C.DLT_TOPIC, json.dumps(command).encode(), key=command["taskId"].encode())
        log.info("task %s → %s (리포트: %s)", command["taskId"], result["eventType"], path)
        return result
    finally:
        if owned:
            journal.close()


# 한줄요약: 시작하고, 반복해서 그래프돌려 결과를 보낸다. 

#  Kafka 우체국 창구
async def run():
    consumer = AIOKafkaConsumer(C.COMMANDS_TOPIC, bootstrap_servers=C.KAFKA, group_id=C.GROUP_ID,
                                enable_auto_commit=False, auto_offset_reset=C.KAFKA_AUTO_OFFSET_RESET,
                                max_poll_records=1, max_poll_interval_ms=C.KAFKA_MAX_POLL_INTERVAL_MS)
    producer = AIOKafkaProducer(bootstrap_servers=C.KAFKA, acks="all", enable_idempotence=True)
    await consumer.start()
    await producer.start()
    try:
        async with asyncio.timeout(60):
            while not consumer.assignment():
                await asyncio.sleep(.1)
        log.info("대기 중: %s @ %s (GPT %s)", C.COMMANDS_TOPIC, C.KAFKA, "켜짐" if C.USE_GPT else "꺼짐: 규칙으로 대신")
        async for msg in consumer:
            try:
                command = json.loads(msg.value)
                ok = isinstance(command, dict) and command.get("eventType") in ("DIAGNOSIS_REQUESTED", "DIAGNOSIS_RESUMED")
            except ValueError:
                ok = False
            if not ok:  # 깨진 메시지·모르는 명령 → DLT
                await producer.send_and_wait(C.DLT_TOPIC, msg.value, key=msg.key)
            else:
                while True:
                    try:
                        await handle(command, producer)
                        break
                    except (CommandConflict, ValidationError, KeyError, TypeError):
                        await producer.send_and_wait(C.DLT_TOPIC, msg.value, key=msg.key)
                        break
                    except Exception as exc:
                        log.warning("저장 결과 재전송 대기: %s", type(exc).__name__)
                        await asyncio.sleep(3)
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    configure_logging(log_file=str(C.ROOT / ".local/logs/agent.jsonl"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # MCP 호출마다 찍히는 HTTP 로그 끄기
    asyncio.run(run())
