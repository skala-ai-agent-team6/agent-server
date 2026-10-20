from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from app.limits import MAX_ADDITIONAL_REQUESTS

Domain = Literal["PCB", "MECHANICAL"]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Attachment(Contract):
    photo_id: str = Field(min_length=1)
    target: Literal["PCB", "MECHANICAL"]
    # 모의 도구 전용. 실제 사진을 읽거나 모델 점수를 계산하지 않는다.
    mock_readable: bool = True
    mock_confidence: float = Field(default=0.9, ge=0, le=1)


class DiagnosisEvent(Contract):
    """Kafka 메시지로 사용할 계약 초안. 추가 자료는 누적 스냅샷으로 받는다."""
    event_id: str = Field(min_length=1)
    event_type: Literal["DIAGNOSIS_REQUESTED", "ADDITIONAL_INPUT_SUBMITTED"]
    task_id: str = Field(min_length=1)
    case_id: str = Field(min_length=1)
    round_no: int = Field(default=0, ge=0, le=MAX_ADDITIONAL_REQUESTS)
    symptom: str = Field(min_length=1)  # 기사 소견은 자연어. 별도 에러 코드 입력 없음.
    attachments: list[Attachment] = Field(default_factory=list)
    source_context: dict | None = None  # v2 원문 보존. 로컬 입력에는 불필요.

    @model_validator(mode="after")
    def validate_round(self):
        if (self.event_type == "DIAGNOSIS_REQUESTED") != (self.round_no == 0):
            raise ValueError("최초 요청은 round_no=0, 추가 자료는 round_no=1~3이어야 합니다")
        ids = [a.photo_id for a in self.attachments]
        if len(ids) != len(set(ids)):
            raise ValueError("photo_id는 중복될 수 없습니다")
        return self


class Evidence(Contract):
    photo_ids: list[str]
    manuals: list[dict]
    finding: str


class Candidate(Contract):
    rank: int
    cause_code: str
    cause_description: str
    confidence: float = Field(ge=0, le=1)
    evidence: Evidence
    repair_guide: str
    part_id: str | None = None
    annotated_image: str | None = None


class DiagnosisReport(Contract):
    task_id: str
    case_id: str
    candidates: list[Candidate]
    mode: Literal["mock", "mixed"] = "mock"
