import copy
import json
import uuid
import pytest
from worker.journal import Journal, CommandConflict
from worker.send_test import COMMAND


def prepared(tmp_path):
    journal = Journal(tmp_path / 'state.db')
    initial = copy.deepcopy(COMMAND)
    journal.begin(initial)
    request = {'round': 1, 'requestKey': str(uuid.uuid4()), 'checkpoint': 'saved',
               'bodyPart': 'wheel', 'description': '추가 사진'}
    journal.finish(initial, {'eventType': 'PHOTO_REQUESTED', 'payload': request})
    resumed = copy.deepcopy(initial)
    resumed.update(eventId=str(uuid.uuid4()), eventType='DIAGNOSIS_RESUMED',
                   executionId=str(uuid.uuid4()), inputRevision=2)
    resumed['payload'].update({k: request[k] for k in ('round', 'requestKey', 'checkpoint')})
    resumed['payload']['requestId'] = '1'
    photo = {**resumed['payload']['attachments'][0], 'id': '2', 'downloadPath': '/internal/attachments/2'}
    resumed['payload']['attachments'].append(photo)
    resumed['payload']['attachmentIds'].append('2')
    resumed['payload']['addedAttachmentIds'] = ['2']
    return journal, initial, resumed


@pytest.mark.parametrize('mutation', [
    lambda r, i: r.update(inputRevision=3),
    lambda r, i: r.update(executionId=i['executionId']),
    lambda r, i: r.update(correlationId=str(uuid.uuid4())),
    lambda r, i: r['payload']['attachments'][0].update(sha256='tampered'),
    lambda r, i: r['payload'].update(addedAttachmentIds=[]),
    lambda r, i: r['payload'].update(attachmentIds=['1']),
    lambda r, i: r['payload'].update(attachments=i['payload']['attachments'],
                                    attachmentIds=['1'], addedAttachmentIds=[]),
])
def test_reject_invalid_resume(tmp_path, mutation):
    journal, initial, resumed = prepared(tmp_path)
    mutation(resumed, initial)
    with pytest.raises(CommandConflict):
        journal.begin(resumed)
    journal.close()


def test_valid_resume_and_duplicate(tmp_path):
    journal, _, resumed = prepared(tmp_path)
    assert journal.begin(resumed) is None
    assert journal.begin(resumed) is None
    journal.close()


def test_reject_concurrent_case(tmp_path):
    journal, initial, _ = prepared(tmp_path)
    another = copy.deepcopy(initial)
    another.update(eventId=str(uuid.uuid4()), taskId='999999')
    another['payload']['task']['id'] = '999999'
    with pytest.raises(CommandConflict, match='active'):
        journal.begin(another)
    journal.close()


def test_structured_log_includes_context_without_input(tmp_path):
    from worker.observability import configure_logging, context, log_event
    path = tmp_path / 'agent.jsonl'
    configure_logging(log_file=path)
    log_event('test.context', **context(COMMAND), tool='example')
    row = json.loads(path.read_text().strip())
    assert all(k in row for k in ('event_id', 'task_id', 'case_id', 'executionId', 'correlationId', 'inputRevision'))
    assert COMMAND['payload']['repair']['symptom'] not in path.read_text()


def test_blank_report_rejected():
    from worker.tools import Report, Candidate
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Report(summary=' ', repairMethod='check', candidates=[Candidate(cause='PCB', confidence=.5, explanation='test')])
    with pytest.raises(ValidationError):
        Candidate(cause='PCB', confidence=.5, explanation=' ')
