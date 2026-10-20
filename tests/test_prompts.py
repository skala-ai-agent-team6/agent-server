import asyncio
import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker import config as C, harness, main, prompts, report, tools
from worker.journal import Journal
from test_integration import command, fixture_tools, Producer

SPLIT_TEXT = tools.split_text
WRITE_REPORT = tools.write_report


def copy_bundle(tmp_path, monkeypatch):
    directory = tmp_path / 'prompts'
    shutil.copytree(prompts.PROMPT_DIR, directory)
    monkeypatch.setattr(prompts, 'PROMPT_DIR', directory)
    return directory


def revise(directory, text, version='1.0.1'):
    target = directory / 'split_text.md'
    target.write_text(text)
    manifest = json.loads((directory / 'manifest.json').read_text())
    manifest['prompts']['split_text'].update(version=version, sha256=hashlib.sha256(target.read_bytes()).hexdigest())
    (directory / 'manifest.json').write_text(json.dumps(manifest))


def test_prompt_inputs_with_braces_are_not_interpreted_as_template():
    text = '소견 {findings} / {__class__} / $HOME / {{text}}'
    result = prompts.render('split_text', tools.Clues, text=text)
    assert result == '로봇청소기 고장 증상을 단서별로 나눠라. 없으면 빈 목록.\n증상: ' + text
    with pytest.raises(prompts.PromptConfigError):
        prompts.render('write_report', tools.Clues, text=text)


@pytest.mark.parametrize('bad', ['missing', 'hash', 'placeholder', 'path', 'schema'])
def test_invalid_bundle_fails_before_model_and_writes_failure(tmp_path, monkeypatch, bad):
    directory = copy_bundle(tmp_path, monkeypatch)
    if bad == 'missing':
        (directory / 'split_text.md').unlink()
    elif bad == 'hash':
        (directory / 'split_text.md').write_text('changed {text}')
    elif bad == 'placeholder':
        revise(directory, 'changed {unknown}')
    else:
        p = directory / 'manifest.json'
        manifest = json.loads(p.read_text())
        manifest['prompts']['split_text']['file' if bad == 'path' else 'output_schema'] = '../split_text.md' if bad == 'path' else 'Report'
        p.write_text(json.dumps(manifest))
    graph = SimpleNamespace(ainvoke=AsyncMock())
    monkeypatch.setattr(main, 'GRAPH', graph)
    monkeypatch.setattr(report, 'OUT', tmp_path/'reports')
    async def run():
        journal = Journal(tmp_path/'state.db')
        try:
            result = await main.handle(command(), Producer(), journal)
            assert result['payload']['errorCode'] == 'PROMPT_CONFIG_ERROR'
            graph.ainvoke.assert_not_awaited()
            assert '분석을 시작하지 않았습니다' in next(report.OUT.glob('*.md')).read_text()
            assert json.loads(next(report.OUT.glob('*.json')).read_text())['result'] == result
        finally:
            journal.close()
    asyncio.run(run())


def test_prompt_snapshot_stays_fixed_during_execution(tmp_path, monkeypatch):
    directory = copy_bundle(tmp_path, monkeypatch)
    original = prompts.render('split_text', tools.Clues, text='소견')
    class Graph:
        async def ainvoke(self, *args, **kwargs):
            revise(directory, '새 지시 {text}')
            assert prompts.render('split_text', tools.Clues, text='소견') == original
            return {'result': 'ok'}
    metrics = {}
    asyncio.run(harness.invoke(Graph(), command(), AsyncMock(), metrics))
    assert metrics['prompts']['split_text']['version'] == '1.0.0'
    assert prompts.render('split_text', tools.Clues, text='소견') == '새 지시 소견'


def test_prompt_metadata_saved_and_replay_does_not_use_new_version(fixture_tools, monkeypatch, tmp_path):
    directory = copy_bundle(tmp_path, monkeypatch)
    monkeypatch.setattr(C, 'USE_GPT', True)
    monkeypatch.setattr(tools, 'split_text', SPLIT_TEXT)
    monkeypatch.setattr(tools, 'write_report', WRITE_REPORT)
    calls = []
    def model(schema):
        async def invoke(value):
            calls.append((schema.__name__, value))
            if schema is tools.Clues:
                return tools.Clues(pcb=['충전'], mech=[], sw=[])
            return tools.Report(summary='결과', repairMethod='점검', candidates=[
                tools.Candidate(cause='PCB', confidence=.5, explanation='관찰')])
        return SimpleNamespace(ainvoke=invoke)
    monkeypatch.setattr(tools, 'gpt', model)
    async def run():
        journal, cmd = Journal(tmp_path/'state.db'), command()
        try:
            first = await main.handle(cmd, Producer(), journal)
            assert first['eventType'] == 'DIAGNOSIS_COMPLETED'
            path = next(report.OUT.glob('*.json'))
            saved = json.loads(path.read_text())
            metadata = saved['execution']['prompts']
            assert set(metadata) == {'split_text', 'write_report'}
            assert all(v['version'] == '1.0.0' and len(v['schema_sha256']) == 64 for v in metadata.values())
            assert '실행에 사용한 프롬프트' in path.with_suffix('.md').read_text()
            revise(directory, '바뀐 지시 {text}')
            second = await main.handle(cmd, Producer(), journal)
            assert second == first and len(calls) == 2
            assert json.loads(path.read_text())['execution']['prompts'] == metadata
        finally:
            journal.close()
    asyncio.run(run())


def test_prompt_logs_versions_without_user_text(caplog):
    import logging
    with caplog.at_level(logging.INFO, logger='repair_agent'):
        prompts.render('split_text', tools.Clues, text='private-technician-note')
    records = [r for r in caplog.records if r.getMessage() == 'prompt.used']
    assert records
    fields = records[-1].agent_fields
    assert fields['prompt'] == 'split_text' and len(fields['sha256']) == 64
    assert 'private-technician-note' not in json.dumps(fields)
