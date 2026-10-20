# Agent 프롬프트

이 폴더는 Agent가 직접 호출하는 LLM 지시문을 관리한다. 최초 `1.0.0`은
`6faafbb`의 인라인 문구를 그대로 옮겼다. 문구·메시지 역할·temperature·출력 스키마는 변경하지 않았다.

| 파일 | 역할 | 치환 값 | 출력 스키마 (`worker/tools.py`) |
| --- | --- | --- | --- |
| `classify_photo.md` | 사진을 PCB / MECH / OTHER로 분류 | 없음 | `PhotoKind` |
| `split_text.md` | 소견을 PCB / 기계 / SW 단서로 분리 | `text` | `Clues` |
| `write_report.md` | 분석·검색 근거를 종합한 한국어 리포트 | `compatible_parts`, `text`, `findings` | `Report` |

소견·PCB·파손 해석 및 RAG MCP 내부 프롬프트는 각 팀 서버가 관리한다. 이 폴더가 그 프롬프트까지 버전 관리하는 것은 아니다.

## 버전 기록과 검증

- `manifest.json`: 묶음 버전, 각 파일 버전·SHA-256·출력 스키마 이름.
- 실행 시작 시 세 파일을 검증하고 내용을 메모리에 고정한다. 실행 중 디스크 파일을 바꿔도 해당 실행에는 반영되지 않는다.
- 누락·해시 불일치·치환 항목 오류는 `PROMPT_CONFIG_ERROR`로 종료한다. 이전 코드에 숨겨둔 기본 프롬프트로 대체하지 않는다.
- `prompt.used` 로그에는 이름·버전·해시·실제 출력 스키마 해시·모델을 기록한다. 프롬프트 본문이나 기사 소견은 기록하지 않는다.
- 실행한 프롬프트만 SQLite 실행 통계와 로컬 리포트 JSON의 `execution.prompts`에 남긴다. 미실행한 분류나 테스트 대역은 사용한 것으로 기록하지 않는다.
- JSON의 `command`·`result`는 유지한다. Kafka 결과 payload에는 프롬프트 메타데이터를 추가하지 않는다.
- 중복 요청 재전송은 저장된 실행 통계를 사용한다. 현재 파일의 새 버전으로 과거 리포트 이력을 덮어쓰지 않는다.

## 수정 절차

1. 해당 `.md`를 수정한다. 기존 `{text}` 같은 치환 항목은 유지한다. 새 치환 항목은 `worker/prompts.py`와 호출부를 함께 수정해야 한다.
2. `manifest.json`의 해당 프롬프트 `version`과 `bundle_version`을 올리고, 수정한 파일의 `sha256`을 갱신한다.
3. 회귀 테스트 및 동일 입력의 실제 리포트 검증을 수행한다. 문구가 바뀌면 리포트 품질도 비교한다.
4. 파일·manifest·검증 기록을 함께 검토한 뒤 커밋한다.

```bash
# 파일 바이트 기준 해시 확인 (예: 리포트 프롬프트)
shasum -a 256 prompts/write_report.md
uv run python -m pytest -q
uv run python -m worker.verify_live --scenario mixed
```

줄바꿈도 해시에 포함한다. 버전 번호 상향은 리뷰 규칙이며, 코드가 Git 이력과 비교해 강제하는 것은 아니다.
프롬프트 내용이 같아도 LLM 결과 문장까지 항상 동일하다는 보장은 없다. JSON 계약·근거 보존·버전 기록을 함께 확인한다.
