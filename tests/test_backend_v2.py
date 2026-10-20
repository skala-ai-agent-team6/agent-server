import json
from pathlib import Path
from uuid import uuid4, UUID
import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
from app.main import create_app
from app.backend_v2 import BackendCommand, to_internal, to_backend_events, ContractPending
from app.adapters import MockTools

SCHEMA = json.loads((Path(__file__).parent / 'fixtures/backend-v2.schema.json').read_text())
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())


def command(target='PCB'):
    return {
        'eventId': str(uuid4()), 'eventType': 'DIAGNOSIS_REQUESTED', 'schemaVersion': 2,
        'occurredAt': '2026-10-20T03:32:57Z', 'repairId': '100', 'taskId': '1',
        'correlationId': str(uuid4()), 'inputRevision': 1, 'executionId': str(uuid4()), 'sequence': 0,
        'payload': {'repair': {'id': '100', 'symptom': '기사가 기판 손상을 의심함',
                               'error_codes': [], 'firmware_version': None, 'user_input': {'opinion': '원문'}},
                    'task': {'id': '1'},
                    'attachments': [{'id': '10', 'request_id': None, 'kind': 'IMAGE', 'target': target,
                                     'mime_type': 'image/png', 'byte_size': 100, 'sha256': 'a'*64,
                                     'downloadPath': '/internal/attachments/10'}],
                    'attachmentIds': ['10'], 'addedAttachmentIds': [], 'requestId': None,
                    'requestKey': None, 'round': 0, 'checkpoint': None}}


def test_v2_contract_and_duplicate_replay(tmp_path):
    path = str(tmp_path/'events.sqlite3')
    app = create_app(store_path=path)
    c = TestClient(app)
    raw = command()
    VALIDATOR.validate(raw)
    response = c.post('/internal/backend-v2/events', json=raw)
    assert response.status_code == 200, response.text
    body = response.json()
    events = body['backend_events']
    assert events[0]['eventType'] == 'DIAGNOSIS_STARTED'
    assert events[-1]['eventType'] == 'DIAGNOSIS_COMPLETED'
    assert [e['sequence'] for e in events] == list(range(1, len(events)+1))
    for event in events:
        VALIDATOR.validate(event)
        UUID(event['eventId'])
        for field in ('repairId', 'taskId', 'correlationId', 'inputRevision', 'executionId'):
            assert event[field] == raw[field]
    payload = events[-1]['payload']
    assert payload['candidates'][0]['confidence'] == 0.9
    assert payload['recommendedParts'] == []
    assert payload['evidence'][0]['attachmentId'] == '10'
    assert c.post('/internal/backend-v2/events', json=raw).json() == body
    assert c.get('/internal/backend-v2/outgoing-events').json() == events
    app.state.processor.close()
    next_app = create_app(store_path=path)
    c2 = TestClient(next_app)
    assert c2.post('/internal/backend-v2/events', json=raw).json() == body
    assert c2.get('/internal/backend-v2/outgoing-events').json() == events
    next_app.state.processor.close()


def test_metadata_conflict_not_silently_ignored():
    c = TestClient(create_app())
    raw = command()
    c.post('/internal/backend-v2/events', json=raw)
    raw['executionId'] = str(uuid4())
    assert c.post('/internal/backend-v2/events', json=raw).status_code == 409


@pytest.mark.parametrize('field,value', [('taskId','demo-1'), ('eventId','not-uuid'),
                                         ('schemaVersion',1), ('sequence',1), ('inputRevision',2)])
def test_bad_envelope(field, value):
    raw = command()
    raw[field] = value
    assert TestClient(create_app()).post('/internal/backend-v2/events', json=raw).status_code == 422


def test_general_category_is_pending_not_guessed():
    c = TestClient(create_app())
    response = c.post('/internal/backend-v2/events', json=command('GENERAL'))
    assert response.status_code == 409
    assert 'category contract pending' in response.json()['detail']
    assert c.get('/internal/backend-v2/outgoing-events').json() == []
    assert c.get('/internal/diagnoses/1').status_code == 404


def test_checkpoint_pending_rolls_back_state_and_outputs():
    c = TestClient(create_app())
    raw = command()
    raw['payload']['attachments'] = []
    raw['payload']['attachmentIds'] = []
    first = c.post('/internal/backend-v2/events', json=raw)
    assert first.status_code == 409
    assert 'checkpoint contract pending' in first.json()['detail']
    assert c.get('/internal/diagnoses/1').status_code == 404
    assert c.get('/internal/backend-v2/outgoing-events').json() == []
    assert c.get('/internal/outgoing-events').json() == []
    assert c.post('/internal/backend-v2/events', json=raw).status_code == 409


class BrokenTools(MockTools):
    def analyze(self, domain, request):
        raise ConnectionError('simulated')


def test_failed_event_matches_backend_schema():
    c = TestClient(create_app(BrokenTools()))
    response = c.post('/internal/backend-v2/events', json=command())
    assert response.status_code == 200
    body = response.json()
    assert body['photo_request_count'] == 0
    assert body['backend_events'][-1]['eventType'] == 'DIAGNOSIS_FAILED'
    assert body['backend_events'][-1]['payload']['errorCode'] == 'ConnectionError'
    for event in body['backend_events']:
        VALIDATOR.validate(event)


def test_resume_preserves_source_without_requiring_error_codes():
    raw = command()
    raw['eventType'] = 'DIAGNOSIS_RESUMED'
    raw['inputRevision'] = 2
    raw['payload'].update({'round': 1, 'requestId': '2', 'requestKey': str(uuid4()),
                           'addedAttachmentIds': ['10'], 'checkpoint': None})
    parsed = BackendCommand.model_validate(raw)
    internal = to_internal(parsed)
    assert internal.event_type == 'ADDITIONAL_INPUT_SUBMITTED'
    assert internal.round_no == 1
    assert internal.source_context['payload']['repair']['user_input'] == {'opinion':'원문'}
    assert internal.source_context['payload']['checkpoint'] is None


def test_third_photo_error_code_translation():
    raw = command()
    events = to_backend_events(BackendCommand.model_validate(raw), {
        'executions': [], 'status': 'FAILED', 'error_code': 'PHOTO_LIMIT'})
    assert events[-1]['payload']['errorCode'] == 'INCONCLUSIVE_AFTER_THIRD_PHOTO'
    VALIDATOR.validate(events[-1])
