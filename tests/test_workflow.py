import pytest
from fastapi.testclient import TestClient
from app.main import create_app
from app.adapters import MockTools


def request(readable=True, order=False):
    return {"task_id": "1", "case_id": "100", "symptom": "왼쪽 바퀴 멈춤", "mock_order_required": order,
            "attachments": [{"id": "initial", "target": "MOTOR", "object_key": "mock/motor.jpg", "mock_readable": readable}]}

def photo(n, readable):
    return {"round_no": n, "attachments": [{"id": f"photo-{n}", "target": "MOTOR", "object_key": f"mock/{n}.jpg", "mock_readable": readable}]}

def test_success_and_idempotent_start():
    c = TestClient(create_app())
    first = c.post('/internal/diagnoses', json=request()).json()
    assert first['status'] == 'SUCCEEDED'
    assert first['selected'] == ['MOTOR']
    assert first['candidates'][0]['evidence']['analysis']['evidence_ids'] == ['initial']
    assert c.post('/internal/diagnoses', json=request()).json()['tool_call_count'] == first['tool_call_count']
    assert c.post('/internal/diagnoses', json={**request(), 'symptom': '다른 증상'}).status_code == 409

def test_resume_preserves_state_and_rejects_replay():
    c = TestClient(create_app())
    initial = c.post('/internal/diagnoses', json=request(False)).json()
    assert initial['status'] == 'WAITING_INPUT'
    assert initial['photo_request_count'] == 1
    result = c.post('/internal/diagnoses/1/photos', json=photo(1, True)).json()
    assert result['status'] == 'SUCCEEDED'
    assert len(result['request']['attachments']) == 2
    assert result['photo_request_count'] == 1
    assert c.post('/internal/diagnoses/1/photos', json=photo(1, True)).status_code == 409

def test_three_rounds_then_failure():
    c = TestClient(create_app())
    c.post('/internal/diagnoses', json=request(False))
    for n in range(1, 4):
        result = c.post('/internal/diagnoses/1/photos', json=photo(n, False)).json()
        assert result['photo_request_count'] == min(n+1, 3)
    assert result['status'] == 'FAILED'
    assert result['error_code'] == 'PHOTO_LIMIT'
    assert result['pending'] == []

@pytest.mark.parametrize('approved,status', [(True, 'ORDERED'), (False, 'REJECTED')])
def test_approval(approved, status):
    c = TestClient(create_app())
    first = c.post('/internal/diagnoses', json=request(order=True)).json()
    assert first['status'] == 'SUCCEEDED'  # 진단 완료와 발주 대기는 독립 상태
    assert first['order']['status'] == 'PENDING'
    assert first['workflow_status'] == 'WAITING_APPROVAL'
    assert c.post('/internal/diagnoses/1/mock-approval', json={'order_id':'wrong','approved':True}).status_code == 409
    result = c.post('/internal/diagnoses/1/mock-approval', json={'order_id':'mock-order-1','approved':approved}).json()
    assert result['order']['status'] == status
    assert c.post('/internal/diagnoses/1/mock-approval', json={'order_id':'mock-order-1','approved':approved}).status_code == 409

class BrokenTools(MockTools):
    def analyze(self, domain, request):
        raise TimeoutError('simulated')

def test_tool_failure_does_not_request_photos():
    c = TestClient(create_app(BrokenTools()))
    result = c.post('/internal/diagnoses', json=request()).json()
    assert result['status'] == 'FAILED'
    assert result['retry_count'] == 2
    assert result['tool_call_count'] == 3
    assert result['photo_request_count'] == 0

def test_call_budget():
    c = TestClient(create_app(max_calls=1))
    result = c.post('/internal/diagnoses', json=request()).json()
    assert result['error_code'] == 'BudgetExceeded'
    assert result['tool_call_count'] == 1

def test_unknown_and_stale_round():
    c = TestClient(create_app())
    assert c.get('/internal/diagnoses/missing').status_code == 404
    c.post('/internal/diagnoses', json=request(False))
    assert c.post('/internal/diagnoses/1/photos', json=photo(2, True)).status_code == 409
