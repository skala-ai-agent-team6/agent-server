from fastapi.testclient import TestClient
from app.main import create_app
from app.adapters import MockTools
from app.models import DiagnosisEvent
from app.workflow import build_graph


def request(confidence=0.9, readable=True):
    return {"event_id": "initial-event", "event_type": "DIAGNOSIS_REQUESTED",
            "task_id": "1", "case_id": "100", "round_no": 0, "symptom": "왼쪽 바퀴 멈춤",
            "attachments": [{"photo_id": "initial", "target": "MECHANICAL",
                             "mock_readable": readable, "mock_confidence": confidence}]}


def additional(previous, n, confidence=0.9):
    return {**previous, "event_id": f"additional-{n}", "event_type": "ADDITIONAL_INPUT_SUBMITTED",
            "round_no": n, "attachments": previous["attachments"] + [
                {"photo_id": f"photo-{n}", "target": "MECHANICAL", "mock_readable": True,
                 "mock_confidence": confidence}]}


def test_success_and_duplicate_event():
    app = create_app()
    c = TestClient(app)
    first = c.post('/internal/events', json=request()).json()
    assert first['status'] == 'SUCCEEDED'
    assert first['selected'] == ['MECHANICAL']
    assert first['outgoing_event']['event_type'] == 'DIAGNOSIS_COMPLETED'
    assert first['report']['candidates'][0]['evidence']['photo_ids'] == ['initial']
    assert c.post('/internal/events', json=request()).json() == first
    assert len(c.get('/internal/outgoing-events').json()) == 1
    assert c.post('/internal/events', json={**request(), 'symptom': '다른 증상'}).status_code == 409
    assert c.post('/internal/events', json={**request(), 'event_id': 'new-initial'}).status_code == 409


def test_graph_ends_and_new_event_reanalyzes_without_interrupt():
    graph = build_graph(MockTools())
    assert graph.checkpointer is None
    c = TestClient(create_app())
    data = request(confidence=0.2)
    initial = c.post('/internal/events', json=data).json()
    assert initial['status'] == 'WAITING_INPUT'
    assert initial['photo_request_count'] == 1
    assert initial['outgoing_event']['payload']['targets'] == ['MECHANICAL']
    data = additional(data, 1)
    result = c.post('/internal/events', json=data).json()
    assert result['status'] == 'SUCCEEDED'
    assert len(result['request']['attachments']) == 2
    assert result['photo_request_count'] == 1
    assert result['tool_call_count'] > initial['tool_call_count']
    assert c.post('/internal/events', json=data).json() == result
    assert c.post('/internal/events', json={**data, 'event_id': 'replay-with-new-id'}).status_code == 409


def test_three_rounds_then_failure_and_duplicate_does_not_increment():
    c = TestClient(create_app())
    data = request(confidence=0.2)
    first = c.post('/internal/events', json=data).json()
    assert c.post('/internal/events', json=data).json() == first
    for n in range(1, 4):
        data = additional(data, n, confidence=0.2)
        result = c.post('/internal/events', json=data).json()
        assert result['photo_request_count'] == min(n+1, 3)
        assert c.post('/internal/events', json=data).json() == result
    assert result['status'] == 'FAILED'
    assert result['error_code'] == 'PHOTO_LIMIT'
    assert result['outgoing_event']['payload']['manual_review_required'] is True
    assert len(c.get('/internal/outgoing-events').json()) == 4


def test_success_on_third_submission():
    c = TestClient(create_app())
    data = request(0.1)
    c.post('/internal/events', json=data)
    for n in range(1, 4):
        data = additional(data, n, confidence=0.9 if n == 3 else 0.1)
        result = c.post('/internal/events', json=data).json()
    assert result['status'] == 'SUCCEEDED'
    assert result['photo_request_count'] == 3


def test_stale_wrong_case_missing_snapshot_and_unchanged_input():
    c = TestClient(create_app())
    data = request(0.1)
    c.post('/internal/events', json=data)
    photo = additional(data, 1)
    for body in [{**photo, 'round_no': 2}, {**photo, 'case_id': 'other'},
                 {**photo, 'attachments': photo['attachments'][1:]},
                 {**photo, 'attachments': data['attachments']}]:
        assert c.post('/internal/events', json=body).status_code == 409
    assert c.get('/internal/diagnoses/1').json()['photo_request_count'] == 1


class BrokenTools(MockTools):
    def analyze(self, domain, request):
        raise TimeoutError('simulated')


def test_tool_error_does_not_request_photos():
    c = TestClient(create_app(BrokenTools()))
    result = c.post('/internal/events', json=request()).json()
    assert result['status'] == 'FAILED'
    assert result['retry_count'] == 2
    assert result['tool_call_count'] == 4
    assert result['photo_request_count'] == 0
    assert result['outgoing_event']['event_type'] == 'DIAGNOSIS_FAILED'


def test_call_budget_is_shared_across_events():
    c = TestClient(create_app(max_calls=3))
    data = request(0.1)
    c.post('/internal/events', json=data)
    result = c.post('/internal/events', json=additional(data, 1)).json()
    assert result['error_code'] == 'BudgetExceeded'
    assert result['tool_call_count'] == 3


def test_persistence_after_recreating_processor(tmp_path):
    path = str(tmp_path / 'state.sqlite3')
    app = create_app(store_path=path)
    data = request(0.1)
    first = TestClient(app).post('/internal/events', json=data).json()
    app.state.processor.close()
    app2 = create_app(store_path=path)
    c = TestClient(app2)
    assert c.post('/internal/events', json=data).json() == first
    assert len(c.get('/internal/outgoing-events').json()) == 1
    assert c.post('/internal/events', json=additional(data, 1)).json()['status'] == 'SUCCEEDED'
    app2.state.processor.close()


def test_natural_language_only_and_text_only_additional_input():
    c = TestClient(create_app())
    data = {**request(), 'symptom': '업데이트 이후 멈춤', 'attachments': []}
    result = c.post('/internal/events', json=data).json()
    assert result['selected'] == []
    assert result['status'] == 'WAITING_INPUT'
    update = {**data, 'event_type': 'ADDITIONAL_INPUT_SUBMITTED', 'event_id': 'text-update',
              'round_no': 1, 'symptom': '업데이트 이후 시작 버튼을 누르면 멈춤'}
    assert c.post('/internal/events', json=update).json()['photo_request_count'] == 2
    assert c.post('/internal/events', json={**data, 'error_codes': ['E01']}).status_code == 422


def test_removed_routes_and_unknown_task():
    c = TestClient(create_app())
    assert c.get('/internal/diagnoses/missing').status_code == 404
    assert c.post('/internal/diagnoses/1/mock-approval', json={}).status_code == 404
    assert c.post('/internal/diagnoses/1/photos', json={}).status_code == 404


def test_categorized_inputs_only_call_corresponding_tools():
    c = TestClient(create_app())
    data = request()
    data['attachments'].append({'photo_id': 'pcb', 'target': 'PCB'})
    result = c.post('/internal/events', json=data).json()
    assert result['selected'] == ['MECHANICAL', 'PCB']
    assert sorted(e['tool_name'] for e in result['executions']) == ['analyze_mechanical', 'analyze_pcb', 'search_manual']


def test_duplicate_photo_ids_rejected():
    data = request()
    data['attachments'] *= 2
    c = TestClient(create_app())
    assert c.post('/internal/events', json=data).status_code == 422


def test_software_words_do_not_add_analysis_branch_and_rag_is_used():
    c = TestClient(create_app())
    data = {**request(), 'symptom': '업데이트 이후 SW 오류가 의심됩니다'}
    result = c.post('/internal/events', json=data).json()
    assert result['status'] == 'SUCCEEDED'
    assert result['selected'] == ['MECHANICAL']
    names = [e['tool_name'] for e in result['executions']]
    assert sorted(names) == ['analyze_mechanical', 'search_manual']
    assert result['report']['candidates'][0]['evidence']['manuals']
