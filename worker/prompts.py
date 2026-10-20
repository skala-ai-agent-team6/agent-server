"""검증한 프롬프트를 실행 시작 시 고정하고, 사용 버전만 기록한다."""
import hashlib
import json
from pathlib import Path
from string import Formatter

PROMPT_DIR = Path(__file__).resolve().parent.parent / "prompts"
SPECS = {
    "classify_photo": ("PhotoKind", set()),
    "split_text": ("Clues", {"text"}),
    "write_report": ("Report", {"text", "findings", "compatible_parts"}),
}


class PromptConfigError(ValueError):
    pass


def load_bundle():
    """파일 누락·변조·잘못된 치환 항목은 실행 전에 거절한다."""
    try:
        manifest = json.loads((PROMPT_DIR / "manifest.json").read_text(encoding="utf-8"))
        version = manifest["bundle_version"]
        if not isinstance(version, str) or not version.strip() or set(manifest["prompts"]) != set(SPECS):
            raise ValueError("Invalid manifest")
        bundle = {}
        for name, (schema, fields) in SPECS.items():
            spec = manifest["prompts"][name]
            if (spec["file"] != name + ".md" or spec["output_schema"] != schema
                    or not isinstance(spec["version"], str) or not spec["version"].strip()):
                raise ValueError("Invalid prompt entry")
            raw = (PROMPT_DIR / spec["file"]).read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            if digest != spec["sha256"]:
                raise ValueError("Prompt hash mismatch")
            template = raw.decode("utf-8")
            parsed = list(Formatter().parse(template))
            if (not template.strip() or {field for _, field, _, _ in parsed if field is not None} != fields
                    or any(fmt or conv for _, field, fmt, conv in parsed if field is not None)):
                raise ValueError("Invalid prompt placeholders")
            bundle[name] = {"template": template, "metadata": {
                "bundle_version": version, "version": spec["version"], "sha256": digest,
                "output_schema": schema}}
        return bundle
    except (OSError, UnicodeError, ValueError, KeyError, TypeError) as exc:
        raise PromptConfigError("Prompt files or manifest are invalid") from exc


def render(name, schema, **values):
    from . import config as C
    from .harness import current, record

    if name not in SPECS or schema.__name__ != SPECS[name][0] or set(values) != SPECS[name][1]:
        raise PromptConfigError("Prompt arguments or output schema do not match")
    budget = current.get()
    # 그래프 실행 중에는 같은 스냅샷을 사용한다. 도구 단독 호출은 현재 파일을 검증한다.
    bundle = budget.prompt_bundle if budget and budget.prompt_bundle is not None else load_bundle()
    entry = bundle[name]
    schema_hash = hashlib.sha256(json.dumps(schema.model_json_schema(), sort_keys=True,
                                           ensure_ascii=False).encode()).hexdigest()
    metadata = {**entry["metadata"], "schema_sha256": schema_hash, "model": C.GPT_MODEL}
    if budget:
        budget.prompt_versions[name] = metadata
    record("prompt.used", prompt=name, **metadata)  # 기사 소견·이미지·프롬프트 본문은 기록하지 않는다.
    return entry["template"].format(**values)
