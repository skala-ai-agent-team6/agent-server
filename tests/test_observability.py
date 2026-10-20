import io
import json
import logging
from uuid import uuid4
import pytest
from app.adapters import MockTools
from app.models import DiagnosisEvent
from app.observability import LOGGER, JsonFormatter, configure_logging
from app.service import EventProcessor
from app.workflow import build_graph
from app.demo import run_scenario, SCENARIOS


@pytest.fixture
def capture_logs():
    stream = io.StringIO()
    configure_logging('INFO')
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    LOGGER.addHandler(handler)
    try:
        yield stream
    finally:
        LOGGER.removeHandler(handler)
        handler.close()


def message(confidence=0.9):
    return DiagnosisEvent.model_validate({
        'event_id': str(uuid4()), 'event_type': 'DIAGNOSIS_REQUESTED',
        'task_id': 'log-test', 'case_id': 'log-case', 'round_no': 0,
        'symptom': 'PRIVATE_OPINION_DO_NOT_LOG',
        'attachments':[{'photo_id':'private-photo-id', 'target':'PCB', 'mock_confidence':confidence}]})


def records(stream):
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_logs_show_path_and_summary_without_raw_inputs(capture_logs):
    processor = EventProcessor(build_graph(MockTools()))
    processor.process(message())
    logs = records(capture_logs)
    names = [r['event'] for r in logs]
    assert names[0] == 'event.received'
    assert names[-1] == 'event.committed'
    assert [r['node'] for r in logs if r['event'] == 'node.started'] == ['route','analyze','complete']
    tools = [r for r in logs if r['event'] == 'tool.succeeded']
    by_name = {r['tool']: r for r in tools}
    assert set(by_name) == {'analyze_pcb','search_manual'}
    assert by_name['analyze_pcb']['output_summary']['confidence'] == 0.9
    assert by_name['search_manual']['output_summary']['source_count'] == 1
    assert all(r.get('mode') == 'mock' for r in logs)
    assert all(r['duration_ms'] >= 0 for r in logs if 'duration_ms' in r)
    assert 'PRIVATE_OPINION_DO_NOT_LOG' not in capture_logs.getvalue()
    assert 'private-photo-id' not in capture_logs.getvalue()
    assert 'mock://manual' not in capture_logs.getvalue()
    assert 'annotated_image' not in capture_logs.getvalue()
    processor.close()


def test_duplicate_logs_without_second_tool_execution(capture_logs):
    processor = EventProcessor(build_graph(MockTools()))
    msg = message()
    processor.process(msg)
    before = capture_logs.getvalue().count('"event": "tool.started"')
    processor.process(msg)
    assert records(capture_logs)[-1]['event'] == 'event.duplicate'
    assert capture_logs.getvalue().count('"event": "tool.started"') == before
    processor.close()


def test_waiting_branch_is_visible(capture_logs):
    processor = EventProcessor(build_graph(MockTools()))
    processor.process(message(0.2))
    logs = records(capture_logs)
    decision = next(r for r in logs if r['event'] == 'analysis.evaluated')
    assert decision['missing'] == ['PCB']
    assert decision['sufficient'] is False
    assert next(r for r in logs if r['event'] == 'graph.branch')['next_node'] == 'request_input'
    assert next(r for r in logs if r['event'] == 'input.requested')['request_round'] == 1
    assert logs[-1]['status'] == 'WAITING_INPUT'
    processor.close()


@pytest.mark.parametrize('scenario', SCENARIOS)
def test_debug_scenarios(scenario, tmp_path, capture_logs):
    result = run_scenario(scenario, tmp_path, 'test-run')
    assert result['status'] in ('SUCCEEDED','FAILED','BLOCKED_EXPECTED')
    logs = records(capture_logs)
    assert logs[0]['event'] == 'demo.started'
    assert logs[-1]['event'] == 'demo.finished'
    if scenario == 'tool-error':
        assert len([r for r in logs if r['event'] == 'tool.retry']) == 2
    if scenario == 'v2-blocked':
        assert any(r['event'] == 'backend.command_rejected' for r in logs)
        assert not any(r['event'] == 'event.committed' for r in logs)


def test_backend_checkpoint_rollback_is_distinct_from_commit(capture_logs):
    from app.backend_v2 import BackendCommand, process_backend_command, ContractPending
    from pathlib import Path
    raw = json.loads((Path(__file__).parents[1]/'examples/backend-v2.json').read_text())
    raw['payload']['attachments'] = []
    raw['payload']['attachmentIds'] = []
    processor = EventProcessor(build_graph(MockTools()))
    with pytest.raises(ContractPending):
        process_backend_command(processor, BackendCommand.model_validate(raw))
    logs = records(capture_logs)
    assert logs[-1]['event'] == 'event.rolled_back'
    assert logs[-1]['contract_field'] == 'checkpoint'
    assert not any(r['event'] == 'event.committed' for r in logs)
    assert processor.backend_outgoing() == []
    processor.close()
