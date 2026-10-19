from typing import Protocol

class AnalysisTools(Protocol):
    def analyze(self, domain: str, request: dict) -> dict: ...
    def search(self, symptom: str) -> list[dict]: ...

class MockTools:
    """네트워크·모델·실제 사진 분석을 실행하지 않는 시연용 도구."""
    def analyze(self, domain: str, request: dict) -> dict:
        photos = [a for a in request["attachments"] if a["target"] == domain]
        sufficient = bool(request["error_codes"]) if domain == "SW" else any(
            a["mock_readable"] for a in photos
        )
        return {
            "domain": domain, "sufficient": sufficient,
            "finding": f"[MOCK] {domain} 이상 후보" if sufficient else "자료 부족",
            "evidence_ids": [a["id"] for a in photos if a["mock_readable"]],
            "error_codes": request["error_codes"] if domain == "SW" else [],
        }

    def search(self, symptom: str) -> list[dict]:
        return [{"source_uri": "mock://manual/repair-v1", "source_version": "mock-v1",
                 "excerpt": "[MOCK] 전원 차단 후 해당 부위를 점검한다."}]
