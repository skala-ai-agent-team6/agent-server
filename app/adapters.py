from typing import Protocol


class AnalysisTools(Protocol):
    def analyze(self, domain: str, request: dict) -> dict: ...
    def search(self, symptom: str) -> list[dict]: ...


class MockTools:
    """네트워크·모델·사진 분석을 실행하지 않는 시연용 도구."""
    def analyze(self, domain: str, request: dict) -> dict:
        photos = [a for a in request["attachments"] if a["target"] == domain]
        readable = [a for a in photos if a["mock_readable"]]
        # 새 자료가 이전 흐린 사진의 판단을 대체한다. 실제 어댑터에서 모델 응답 정규화 필요.
        latest = photos[-1] if photos else None
        sufficient = bool(latest and latest["mock_readable"])
        confidence = latest["mock_confidence"] if sufficient else 0.0
        return {
            "domain": domain,
            "sufficient": sufficient,
            "confidence": confidence,
            "finding": f"[MOCK] {domain} 이상 후보" if sufficient else "자료 또는 지원 도구 부족",
            "evidence_ids": [a["photo_id"] for a in readable],
            "annotated_image": None,
        }

    def search(self, symptom: str) -> list[dict]:
        return [{"source_uri": "mock://manual/repair-v1", "source_version": "mock-v1",
                 "excerpt": "[MOCK] 담당 기사 확인 후 해당 부위를 점검한다."}]
