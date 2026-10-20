"""Spring Kafka v2 계약 변환. 실제 Kafka 송수신은 별도 어댑터로 연결한다."""
from datetime import datetime, timezone
from typing import Annotated, Literal
from uuid import UUID, uuid4
from pydantic import BaseModel, ConfigDict, Field, AwareDatetime, model_validator
from app.models import Attachment, DiagnosisEvent
from app.limits import MAX_ADDITIONAL_REQUESTS
from app.service import EventConflict
from app.observability import log_event

DecimalID = Annotated[str, Field(pattern=r"^[1-9][0-9]*$")]


class ContractPending(EventConflict):
    """팀 합의 전 자동 추정해서 채우면 안 되는 입력/출력."""
    def __init__(self, message, *, field="unspecified"):
        super().__init__(message)
        self.field = field


class BackendAttachment(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: DecimalID
    request_id: DecimalID | None
    mime_type: str = Field(min_length=1)
    byte_size: int = Field(gt=0)
    sha256: str = Field(min_length=1)
    downloadPath: str = Field(min_length=1)
    kind: str = "IMAGE"
    target: str | None = None


class CommandPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    repair: dict
    task: dict
    attachments: list[BackendAttachment]
    attachmentIds: list[DecimalID]
    addedAttachmentIds: list[DecimalID]
    requestId: DecimalID | None
    requestKey: UUID | None
    round: int = Field(ge=0, le=MAX_ADDITIONAL_REQUESTS)
    checkpoint: str | None


class BackendCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    eventId: UUID
    eventType: Literal["DIAGNOSIS_REQUESTED", "DIAGNOSIS_RESUMED"]
    schemaVersion: Literal[2]
    occurredAt: AwareDatetime
    repairId: DecimalID
    taskId: DecimalID
    correlationId: UUID
    inputRevision: int = Field(ge=1)
    executionId: UUID
    sequence: Literal[0]
    payload: CommandPayload

    @model_validator(mode="after")
    def consistent(self):
        p = self.payload
        initial = self.eventType == "DIAGNOSIS_REQUESTED"
        if initial:
            if p.round != 0 or self.inputRevision != 1 or p.requestId or p.requestKey or p.addedAttachmentIds:
                raise ValueError("Invalid initial command")
        elif p.round == 0 or not p.requestId or not p.requestKey:
            raise ValueError("Resume requires round, requestId and requestKey")
        if self.inputRevision != p.round + 1:
            raise ValueError("inputRevision must match input round")
        ids = [a.id for a in p.attachments]
        if len(set(ids)) != len(ids) or ids != p.attachmentIds:
            raise ValueError("attachmentIds must match attachments")
        if len(set(p.addedAttachmentIds)) != len(p.addedAttachmentIds) or not set(p.addedAttachmentIds) <= set(ids):
            raise ValueError("Invalid addedAttachmentIds")
        if str(p.repair.get("id")) != self.repairId or str(p.task.get("id")) != self.taskId:
            raise ValueError("Nested IDs do not match envelope")
        return self


def to_internal(command: BackendCommand) -> DiagnosisEvent:
    p = command.payload
    photos = []
    for a in p.attachments:
        if a.kind != "IMAGE":
            raise ContractPending("Non-image attachment routing has not been agreed", field="attachment.kind")
        # GENERAL을 PCB 등으로 추정하지 않는다. 사진 구분은 팀 합의 대기.
        if a.target not in ("PCB", "MECHANICAL"):
            raise ContractPending("Photo category contract pending: expected explicit PCB/MECHANICAL", field="attachment.target")
        photos.append(Attachment(photo_id=a.id, target=a.target))
    symptom = p.repair.get("symptom")
    if not isinstance(symptom, str) or not symptom.strip():
        raise ValueError("repair.symptom must be natural-language text")
    return DiagnosisEvent(
        event_id=str(command.eventId),
        event_type="DIAGNOSIS_REQUESTED" if command.eventType == "DIAGNOSIS_REQUESTED" else "ADDITIONAL_INPUT_SUBMITTED",
        task_id=command.taskId, case_id=command.repairId, round_no=p.round,
        symptom=symptom, attachments=photos, source_context=command.model_dump(mode="json"))


def to_backend_events(command: BackendCommand, result: dict) -> list[dict]:
    """현재 실행의 결과만 변환한다. 저장한 UUID·sequence를 유지해서 재전송한다."""
    records = []

    def emit(kind, payload):
        records.append({
            "eventId": str(uuid4()), "eventType": kind, "schemaVersion": 2,
            "occurredAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "repairId": command.repairId, "taskId": command.taskId,
            "correlationId": str(command.correlationId), "inputRevision": command.inputRevision,
            "executionId": str(command.executionId), "sequence": len(records)+1, "payload": payload,
        })

    emit("DIAGNOSIS_STARTED", {"stage": "STARTED"})
    for execution in result["executions"]:
        if execution["event_id"] != str(command.eventId) or execution["status"] != "SUCCEEDED":
            continue
        name = execution["tool_name"]
        agent = {"analyze_pcb": "PCB", "analyze_mechanical": "MOTOR"}.get(name, "RAG")
        output = execution["output"]
        metadata = output if isinstance(output, dict) else {}
        emit("TOOL_EXECUTED", {"tool": name, "toolVersion": metadata.get("tool_version", "mock-v1"),
                               "modelVersion": metadata.get("model_version", "mock-v1"),
                               "agentType": agent, "mcpServer": metadata.get("mcp_server", "mock"), "output": output})
    if result["status"] == "WAITING_INPUT":
        # checkpoint 의미는 보류한다. 임의 가짜 값이나 LangGraph interrupt를 만들지 않는다.
        raise ContractPending("PHOTO_REQUESTED checkpoint contract pending; v2 photo requests are not enabled", field="checkpoint")
    if result["status"] == "FAILED":
        code = "INCONCLUSIVE_AFTER_THIRD_PHOTO" if result["error_code"] == "PHOTO_LIMIT" else result["error_code"]
        emit("DIAGNOSIS_FAILED", {"errorCode": code})
        return records
    report = result["report"]
    candidates, evidence = [], []
    allowed_ids = set(command.payload.attachmentIds)
    for c in report["candidates"]:
        candidates.append({"rank": c["rank"], "cause": c["cause_code"], "confidence": c["confidence"],
                           "explanation": c["cause_description"]})
        for photo_id in c["evidence"]["photo_ids"]:
            if photo_id not in allowed_ids:
                raise ValueError("Evidence references an unknown attachment")
            evidence.append({"type": "PHOTO", "attachmentId": photo_id, "candidateRank": c["rank"]})
        for source in c["evidence"]["manuals"]:
            evidence.append({"type": "MANUAL", "candidateRank": c["rank"],
                             "sourceUri": source["source_uri"], "sourceVersion": source["source_version"],
                             "excerpt": source["excerpt"], "citation": source["source_uri"]})
    emit("DIAGNOSIS_COMPLETED", {
        "summary": " / ".join(c["cause_description"] for c in report["candidates"]),
        "modelVersion": "template-mixed-v1" if report["mode"] == "mixed" else "mock-v1", "repairMethod": " / ".join(c["repair_guide"] for c in report["candidates"]),
        "candidates": candidates, "recommendedParts": [], "evidence": evidence})
    return records


def process_backend_command(processor, command: BackendCommand):
    try:
        internal = to_internal(command)
    except (ContractPending, ValueError) as exc:
        log_event("backend.command_rejected", mode="mock", task_id=command.taskId,
                  event_id=str(command.eventId), error_type=type(exc).__name__,
                  reason_code="CONTRACT_PENDING" if isinstance(exc, ContractPending) else "INVALID_COMMAND",
                  contract_field=getattr(exc, "field", None))
        raise
    return processor.process(internal, prepare_result=lambda result: to_backend_events(command, result))
