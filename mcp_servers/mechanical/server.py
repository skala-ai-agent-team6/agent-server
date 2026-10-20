import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

os.environ["YOLO_VERBOSE"] = "False"
# YOLO 잡담 끄기

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from huggingface_hub import hf_hub_download
from PIL import Image
from ultralytics import YOLO

BASE = Path(__file__).parent
MODEL_REPO = os.getenv("MODEL_REPO", "hazemaaa/steel-defect-yolo11n")
MODEL_FILE = os.getenv("MODEL_FILE", "production/best.pt")
# 모델을 바꿀 땐 이 두 값만 바꾼다
PHOTO_DIR = Path(os.getenv("PHOTO_DIR", BASE / "photos"))  # worker는 파일 경로를 직접 넘긴다
OUT_DIR = Path(os.getenv("OUT_DIR", BASE / "results"))
# 지금은 photo_id = PHOTO_DIR 안의 파일 이름(확장자 제외). 저장소(MinIO)가 생기면 여기만 바꾼다
MIN_CONF = 0.25

MODEL = YOLO(hf_hub_download(MODEL_REPO, MODEL_FILE))
# 서버 켤 때 한 번만 로드

DENT_REPO = os.getenv("DENT_REPO", "vineetsarpal/yolov11n-car-damage")
DENT_FILE = os.getenv("DENT_FILE", "best.pt")
DENT_MODEL = YOLO(hf_hub_download(DENT_REPO, DENT_FILE))
# 찌그러짐 모델. 자동차 외판으로 학습돼서 원래 이름이 front-bumper-dent, Headlight-damage 같은 자동차 부위다.
# 기존 표시용 일반 결함명은 유지한다. 해석 MCP에는 raw_label로 원래 부위명을 함께 전달한다.


def plain_name(name: str) -> str:
    """자동차 부위 이름 → 일반 결함명. bonnet-dent → dent, Headlight-damage → breakage"""
    return "dent" if name.lower().endswith("dent") else "breakage"

mcp = FastMCP("mechanical-mcp")


def find_photo(photo_id: str) -> Path:
    if Path(photo_id).is_file():
        return Path(photo_id)
    # 같은 PC에서 부를 땐 파일 경로도 받는다 (agent-server worker)
    for ext in (".jpg", ".jpeg", ".png"):
        path = PHOTO_DIR / f"{photo_id}{ext}"
        if path.is_file():
            return path
    ids = sorted(p.stem for p in PHOTO_DIR.glob("*") if p.suffix.lower() in (".jpg", ".jpeg", ".png"))
    raise ToolError(f"photo_id '{photo_id}' 사진이 없다. 있는 photo_id: {', '.join(ids[:20])}")
    # 없는 id면 재시도에 필요한 목록을 같이 돌려준다


@mcp.tool(annotations={"readOnlyHint": True})
def list_photos() -> dict:
    """분석할 수 있는 기계 부품 사진의 photo_id 목록을 돌려준다."""
    return {"photo_ids": sorted(p.stem for p in PHOTO_DIR.glob("*") if p.suffix.lower() in (".jpg", ".jpeg", ".png"))}


@mcp.tool(annotations={"readOnlyHint": True})
def detect_surface_defects(photo_id: str, conf: float = MIN_CONF) -> dict:
    """기계 부품(모터 하우징, 축, 기어 등) 사진에서 금속 표면 결함을 찾는다.

    결함 종류: crazing(잔금), inclusion(이물 박힘), patches(얼룩·녹), pitted_surface(패임),
    rolled-in_scale(압연 스케일), scratches(긁힘).
    찌그러짐·휨·균열은 찾지 못한다. 강판 클로즈업으로 학습한 모델이라 배경이 많은 사진은 오탐이 많다.
    photo_id: 사진 id. conf: 이 확신도 미만은 버림(0~1).
    """
    path = find_photo(photo_id)
    image = Image.open(path).convert("L").convert("RGB")
    # 흑백으로 학습된 모델 → 흑백을 3채널로 복제

    with redirect_stdout(sys.stderr):
        r = MODEL.predict(image, conf=conf, iou=0.6, verbose=False)[0]

    defects = [
        {"type": MODEL.names[int(c)], "confidence": round(float(s), 3), "box_xyxy": [round(v) for v in b.tolist()]}
        for c, s, b in zip(r.boxes.cls, r.boxes.conf, r.boxes.xyxy)
    ]
    counts = {}
    for d in defects:
        counts[d["type"]] = counts.get(d["type"], 0) + 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    annotated = OUT_DIR / f"{Path(photo_id).stem}_boxed.jpg"
    Image.fromarray(r.plot()[..., ::-1]).save(annotated)
    # 박스 그린 사진 저장 (리포트에는 경로만 넣는다)

    return {
        "photo_id": photo_id,
        "is_defective": bool(defects),
        "max_confidence": max((d["confidence"] for d in defects), default=0.0),
        "defect_counts": counts,
        "defects": defects,
        "annotated_image": str(annotated),
        "model": f"{MODEL_REPO}/{MODEL_FILE}",
    }


@mcp.tool(annotations={"readOnlyHint": True})
def detect_dents(photo_id: str, conf: float = MIN_CONF) -> dict:
    """부품 사진에서 찌그러짐(dent)과 깨짐·파손(breakage)을 찾는다.

    photo_id: 사진 id 또는 파일 경로. conf: 이 확신도 미만은 버림(0~1).
    """
    path = find_photo(photo_id)
    with redirect_stdout(sys.stderr):
        r = DENT_MODEL.predict(str(path), conf=conf, iou=0.6, verbose=False)[0]
    original_names = dict(r.names)
    r.names = {i: plain_name(n) for i, n in r.names.items()}  # 박스 그림에도 일반 이름만

    defects = [
        {"type": r.names[int(c)], "raw_label": original_names[int(c)], "confidence": round(float(s), 3), "box_xyxy": [round(v) for v in b.tolist()]}
        for c, s, b in zip(r.boxes.cls, r.boxes.conf, r.boxes.xyxy)
    ]
    counts = {}
    for d in defects:
        counts[d["type"]] = counts.get(d["type"], 0) + 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    annotated = OUT_DIR / f"{Path(photo_id).stem}_dent_boxed.jpg"
    Image.fromarray(r.plot()[..., ::-1]).save(annotated)

    return {
        "photo_id": photo_id,
        "is_defective": bool(defects),
        "max_confidence": max((d["confidence"] for d in defects), default=0.0),
        "defect_counts": counts,
        "defects": defects,
        "annotated_image": str(annotated),
        "model": "dent-detector (YOLO11n)",  # 원 모델 이름(자동차)은 README에만 적는다
    }


if __name__ == "__main__":
    mcp.run(transport="http", host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "9104")), show_banner=False)
    # Streamable HTTP: http://127.0.0.1:9104/mcp
