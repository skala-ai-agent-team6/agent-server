"""Spring 없이 시험: Spring인 척 명령 1개를 Kafka에 넣고, 워커가 보낸 결과를 받아 계약(JSON Schema)으로 검사한다.

실제 다운로드 가능한 첨부 ID·크기·해시로 COMMAND를 수정해야 한다.
Spring 없이 실제 모델만 확인할 때는 worker.verify_live를 사용한다.
"""

import asyncio
import json
import uuid
import hashlib
from pathlib import Path

import jsonschema
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from . import config as C

SCHEMA = json.loads((C.ROOT / "contract/kafka-event.schema.json").read_text())  # Spring 팀이 만든 계약
FINAL = ("DIAGNOSIS_COMPLETED", "PHOTO_REQUESTED", "DIAGNOSIS_FAILED")
TASK_ID = str(uuid.uuid4().int % 100000000 + 1)

COMMAND = {  # contract/kafka-examples.json 첫 번째 예시와 같은 모양
    "eventId": str(uuid.uuid4()), "eventType": "DIAGNOSIS_REQUESTED", "schemaVersion": 2,
    "occurredAt": "2026-10-20T01:00:00Z", "repairId": TASK_ID, "taskId": TASK_ID,
    "correlationId": str(uuid.uuid4()), "inputRevision": 1, "executionId": str(uuid.uuid4()), "sequence": 0,
    "payload": {
        "repair": {"id": TASK_ID, "product_snapshot": {"id": "1", "model_code": "R1", "components": []},
                   "symptom": "충전이 안 되고 바퀴 모터에서 소음", "error_codes": ["E05"], "firmware_version": "1.2",
                   "user_input": {"customerDescription": "펌웨어 업데이트 후부터 그렇다고 함"}},
        "task": {"id": TASK_ID},
        "attachments": [{"id": "1", "request_id": None, "mime_type": "image/jpeg",
                         "byte_size": Path(C.SAMPLE_PHOTOS[0]).stat().st_size,
                         "sha256": hashlib.sha256(Path(C.SAMPLE_PHOTOS[0]).read_bytes()).hexdigest(),
                         "downloadPath": "/internal/attachments/1"}],
        "attachmentIds": ["1"], "addedAttachmentIds": [], "requestId": None, "requestKey": None,
        "round": 0, "checkpoint": None,
    },
}


async def main():
    consumer = AIOKafkaConsumer(C.EVENTS_TOPIC, bootstrap_servers=C.KAFKA, group_id=f"test-{uuid.uuid4()}")
    await consumer.start()
    await asyncio.sleep(2)  # 결과 토픽 구독이 붙을 때까지

    producer = AIOKafkaProducer(bootstrap_servers=C.KAFKA)
    await producer.start()
    await producer.send_and_wait(C.COMMANDS_TOPIC, json.dumps(COMMAND).encode(), key=TASK_ID.encode())
    await producer.stop()
    print(f"보냄: {COMMAND['eventType']} task {TASK_ID}\n")

    async with asyncio.timeout(180):
        async for msg in consumer:
            e = json.loads(msg.value)
            if e["taskId"] != TASK_ID:
                continue
            jsonschema.validate(e, SCHEMA)  # 계약 위반이면 여기서 멈춘다
            p = e["payload"]
            print(f"#{e['sequence']} {e['eventType']:20} {p.get('stage') or p.get('mcpServer') or ''}")
            if e["eventType"] in FINAL:
                print("\n최종 결과:\n" + json.dumps(p, ensure_ascii=False, indent=2))
                break
    await consumer.stop()
    print("\n계약 검사 통과")


if __name__ == "__main__":
    asyncio.run(main())
