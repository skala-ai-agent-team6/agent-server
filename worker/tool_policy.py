"""도구 장애를 '정상 분석·검출 없음'과 구분한다. 모델 판단이나 그래프 분기는 바꾸지 않는다."""
import json
import math


class ToolResponseError(ValueError):
    """통신은 성공했지만 오류 응답이거나 필요한 결과 형식이 아니다."""
    def __init__(self, tool):
        self.tool = tool
        super().__init__(f"Invalid result from {tool}")


class RequiredToolFailure(RuntimeError):
    def __init__(self, tool, error_type):
        self.tool = tool
        self.error_type = error_type
        super().__init__(f"Required tool failed: {tool}")


def check_response(response, tool):
    if getattr(response, "is_error", False):
        raise ToolResponseError(tool)
    return response


def check_object(data, tool):
    if not isinstance(data, dict) or data.get("error"):
        raise ToolResponseError(tool)
    try:
        json.dumps(data, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ToolResponseError(tool) from exc
    return data


def check_image_result(data, tool):
    """이미지 도구 3종의 공통 계약. 추가 필드·모델 라벨은 그대로 보존한다."""
    check_object(data, tool)
    defects = data.get("defects")
    if (type(data.get("is_defective")) is not bool or not isinstance(defects, list)
            or data["is_defective"] != bool(defects)
            or not isinstance(data.get("annotated_image"), str)
            or not data["annotated_image"].strip()):
        raise ToolResponseError(tool)
    for defect in defects:
        if not isinstance(defect, dict):
            raise ToolResponseError(tool)
        confidence = defect.get("confidence")
        if (not isinstance(defect.get("type"), str) or not defect["type"].strip()
                or type(confidence) not in (float, int)
                or not math.isfinite(confidence) or not 0 <= confidence <= 1):
            raise ToolResponseError(tool)
    return data


def failure_output(tool, exc, *, required):
    # 외부 서버의 오류 본문·경로·토큰은 출력하지 않는다.
    return {"error": type(exc).__name__, "tool": getattr(exc, "tool", tool),
            "status": "failed", "failurePolicy": "required" if required else "optional",
            "errorCode": "REQUIRED_TOOL_FAILED" if required else "OPTIONAL_TOOL_FAILED"}


def limitation_note(findings):
    names = list(dict.fromkeys(f["output"].get("tool", f["source"])
                              for f in findings if f["output"].get("error")))
    if not names:
        return ""
    return ("근거 제한: " + ", ".join(names) +
            " 호출에 실패하여 해당 해석·검색 근거를 사용하지 못했습니다. "
            "미확인 항목을 정상으로 판단하지 말고 수리 전 확인해 주세요.")
