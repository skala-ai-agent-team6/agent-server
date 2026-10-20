import asyncio
import copy
import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from jsonschema.exceptions import ValidationError

from worker import config as C, harness, main, report, tools
from worker.journal import Journal, CommandConflict
from test_integration import command, fixture_tools, Producer


async def invoke_body(body, metrics=None):
    return await harness.invoke(SimpleNamespace(ainvoke=body), command(), AsyncMock(), metrics)


def test_same_input_counts_keyword_calls_and_retries(monkeypatch):
    monkeypatch.setattr(harness.asyncio, 'sleep', AsyncMock())
    actual = []
    @harness.guarded()
    async def detect(photo, option=1):
        actual.append(photo)
        if len(actual) < 3:
            raise ConnectionError()
        return True
    async def graph(*args, **kwargs):
        assert await detect('image-a')
        await detect(photo='image-a', option=1)
    metrics = {}
    with pytest.raises(harness.LimitExceeded) as caught:
        asyncio.run(invoke_body(graph, metrics))
    assert caught.value.code == 'TOOL_REPEAT_LIMIT'
    assert len(actual) == 3 and metrics['retries'] == 2
    assert metrics['max_same_input_attempts_used'] == 3
    assert metrics['tool_attempts']['detect'] == 3
    assert 'image-a' not in json.dumps(metrics)


def test_hundred_different_inputs_not_blocked_by_three_attempt_limit():
    actual = []
    @harness.guarded()
    async def detect(photo):
        actual.append(photo)
    async def graph(*args, **kwargs):
        for i in range(100):
            await detect(f'photo-{i}')
        return {'result': 'ok'}
    cmd = command()
    cmd['payload']['attachments'] = [{'id': str(i + 1)} for i in range(100)]
    metrics = {}
    asyncio.run(harness.invoke(SimpleNamespace(ainvoke=graph), cmd, AsyncMock(), metrics))
    assert len(actual) == 100 and metrics['max_same_input_attempts_used'] == 1
    assert metrics['tool_attempts']['detect'] == 100
    assert cmd['payload']['round'] == 0


def test_parallel_same_input_limit_is_atomic():
    actual = []
    @harness.guarded()
    async def detect(photo):
        actual.append(photo)
        await asyncio.sleep(0)
    async def graph(*args, **kwargs):
        results = await asyncio.gather(*(detect('same') for _ in range(4)), return_exceptions=True)
        assert sum(isinstance(r, harness.LimitExceeded) for r in results) == 1
    asyncio.run(invoke_body(graph))
    assert len(actual) == 3


def test_mcp_shared_wrapper_separates_tool_names():
    @harness.guarded(name_arg='name')
    async def call(url, name, arguments):
        return name
    async def graph(*args, **kwargs):
        for _ in range(3):
            await call('local', 'interpret_pcb', {'labels': ['short']})
            await call('local', 'interpret_damage', {'labels': ['short']})
    metrics = {}
    asyncio.run(invoke_body(graph, metrics))
    assert metrics['tool_attempts'] == {'interpret_pcb': 3, 'interpret_damage': 3}


@pytest.mark.parametrize('code', ['TOOL_CALL_LIMIT', 'TOOL_REPEAT_LIMIT', 'NODE_EXECUTION_LIMIT', 'RUN_TIMEOUT', 'TOOL_TIMEOUT'])
def test_limit_saves_failed_json_and_report_without_photo_request(monkeypatch, tmp_path, code):
    monkeypatch.setattr(report, 'OUT', tmp_path/'reports')
    monkeypatch.setattr(harness.asyncio, 'sleep', AsyncMock())
    stopped = []
    @harness.guarded()
    async def call(value):
        if code in ('RUN_TIMEOUT', 'TOOL_TIMEOUT'):
            try:
                await asyncio.Event().wait()
            finally:
                stopped.append(True)
        return value
    @harness.observed
    async def node(state, config):
        return {}
    if code == 'TOOL_CALL_LIMIT': monkeypatch.setattr(C, 'MAX_TOOL_CALLS', 1)
    if code == 'NODE_EXECUTION_LIMIT': monkeypatch.setattr(C, 'MAX_NODE_EXECUTIONS', 2)
    if code == 'RUN_TIMEOUT': monkeypatch.setattr(C, 'RUN_TIMEOUT', .01)
    if code == 'TOOL_TIMEOUT': monkeypatch.setattr(C, 'TOOL_TIMEOUT', .01)
    async def graph(*args, **kwargs):
        if code == 'NODE_EXECUTION_LIMIT':
            for _ in range(3): await node({}, {})
        else:
            for i in range(4): await call('same' if code == 'TOOL_REPEAT_LIMIT' else i)
        raise AssertionError('limit should stop execution')
    monkeypatch.setattr(main, 'GRAPH', SimpleNamespace(ainvoke=graph))
    async def run():
        journal, producer, cmd = Journal(tmp_path/'state.db'), Producer(), command()
        try:
            result = await main.handle(cmd, producer, journal)
            assert result == {'eventType': 'DIAGNOSIS_FAILED', 'payload': {'errorCode': code}}
            assert not any(e['eventType'] == 'PHOTO_REQUESTED' for e in producer.events)
            saved = json.loads(next(report.OUT.glob('*.json')).read_text())
            assert saved['result'] == result
            assert code in next(report.OUT.glob('*.md')).read_text()
            if code != 'TOOL_TIMEOUT': assert saved['execution']['limits_hit'][0]['code'] == code
            if code in ('RUN_TIMEOUT', 'TOOL_TIMEOUT'): assert stopped
            if code == 'TOOL_TIMEOUT': assert saved['execution']['retries'] == 2
        finally:
            journal.close()
    asyncio.run(run())


def test_new_event_has_separate_budget():
    @harness.guarded()
    async def call(value): return value
    async def graph(*args, **kwargs):
        for _ in range(3): await call('same')
    for _ in range(2):
        metrics = {}
        asyncio.run(invoke_body(graph, metrics))
        assert metrics['calls'] == 3


@pytest.mark.parametrize('name,value', [('MAX_TOOL_CALLS', 0), ('MAX_NODE_EXECUTIONS', -1),
    ('MAX_SAME_INPUT_ATTEMPTS', 0), ('RECURSION_LIMIT', 0), ('RUN_TIMEOUT', float('nan')),
    ('TOOL_TIMEOUT', float('inf'))])
def test_invalid_limits_are_rejected(monkeypatch, name, value):
    monkeypatch.setattr(C, name, value)
    with pytest.raises(ValueError): harness.validate_limits()


def test_different_photos_do_not_reset_three_additional_inputs(fixture_tools, tmp_path):
    tools.call_pcb_mcp.return_value = {**fixture_tools, 'is_defective': False, 'defects': []}
    async def run():
        journal, producer, cmd = Journal(tmp_path/'state.db'), Producer(), command()
        try:
            for round_no in range(4):
                result = await main.handle(cmd, producer, journal)
                if round_no == 3:
                    assert result['payload']['errorCode'] == 'INCONCLUSIVE_AFTER_THIRD_PHOTO'
                    break
                assert result['eventType'] == 'PHOTO_REQUESTED'
                assert result['payload']['round'] == round_no + 1
                cmd = copy.deepcopy(cmd)
                cmd.update(eventId=str(uuid.uuid4()), eventType='DIAGNOSIS_RESUMED',
                           inputRevision=round_no + 2, executionId=str(uuid.uuid4()))
                photo = {**cmd['payload']['attachments'][0], 'id': str(round_no + 2),
                         'sha256': f'different-photo-{round_no}', 'byte_size': 100 + round_no,
                         'downloadPath': f'/internal/attachments/{round_no + 2}'}
                cmd['payload']['attachments'].append(photo)
                cmd['payload']['attachmentIds'].append(photo['id'])
                cmd['payload']['addedAttachmentIds'] = [photo['id']]
                for key in ('round', 'requestKey', 'checkpoint'):
                    cmd['payload'][key] = result['payload'][key]
                cmd['payload']['requestId'] = str(round_no + 1)
            fourth = copy.deepcopy(cmd)
            fourth.update(eventId=str(uuid.uuid4()), inputRevision=5, executionId=str(uuid.uuid4()))
            fourth['payload']['round'] = 4
            with pytest.raises(ValidationError, match="maximum of 3"): journal.begin(fourth)
            assert sum(e['eventType']=='PHOTO_REQUESTED' for e in producer.events) == 3
        finally:
            journal.close()
    asyncio.run(run())
