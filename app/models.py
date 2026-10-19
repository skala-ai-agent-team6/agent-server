from typing import Literal
from pydantic import BaseModel, ConfigDict, Field

Domain = Literal["PCB", "MOTOR", "SW"]

class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")

class Attachment(Contract):
    id: str = Field(min_length=1)
    target: Domain
    object_key: str = Field(min_length=1)
    # 모의 분석 전용 입력. 실제 사진 품질 판정 결과로 교체한다.
    mock_readable: bool = True

class DiagnosisRequest(Contract):
    task_id: str = Field(min_length=1)
    case_id: str = Field(min_length=1)
    symptom: str = Field(min_length=1)
    error_codes: list[str] = Field(default_factory=list)
    attachments: list[Attachment] = Field(default_factory=list)
    mock_order_required: bool = False

class PhotoSubmission(Contract):
    round_no: int = Field(ge=1, le=3)
    attachments: list[Attachment] = Field(min_length=1)

class Approval(Contract):
    order_id: str
    approved: bool
