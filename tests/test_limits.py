import json
from pathlib import Path
from uuid import uuid4
from fastapi.testclient import TestClient
from langgraph.graph import START, StateGraph
from app.main import create_app
from app.models import DiagnosisEvent
from app.service import EventProcessor
from app.workflow import State


def initial():
    return {'event_id':'limits-initial', 'event_type':'DIAGNOSIS_REQUESTED', 'task_id':'limit-task',
            'case_id':'limit-case', 'round_no':0, 'symptom':'기판 확인 요청',
            'attachments':[{'photo_id':'initial','target':'PCB','mock_confidence':0.1}]}


def test_fourth_input_rejected_and_no_fourth_request_emitted():
    c = TestClient(create_app())
    data = initial()
    c.post('/internal/events', json=data)
    for n in range(1, 4):
        data = {**data, 'event_id':f'input-{n}', 'event_type':'ADDITIONAL_INPUT_SUBMITTED', 'round_no':n,
                'attachments':data['attachments']+[{'photo_id':f'photo-{n}','target':'PCB','mock_confidence':0.1}]}
        response = c.post('/internal/events', json=data)
        assert response.status_code == 200
        assert response.json()['photo_request_count'] <= 3
    assert response.json()['status'] == 'FAILED'
    # 새로운 메시지 ID를 써도 종료 작업을 다시 분석할 수 없다.
    assert c.post('/internal/events', json={**data,'event_id':'late-third'}).status_code == 409
    assert c.post('/internal/events', json={**data,'event_id':'fourth','round_no':4}).status_code == 422
    assert c.get('/internal/diagnoses/limit-task').json()['photo_request_count'] == 3
    outgoing = c.get('/internal/outgoing-events').json()
    requests = [e for e in outgoing if e['event_type']=='ADDITIONAL_INPUT_REQUESTED']
    assert [e['payload']['round_no'] for e in requests] == [1,2,3]
    assert len(outgoing) == 4  # 요청3개 + 실패1개


def test_real_cycle_stops_at_graph_limit_and_persists_failure():
    builder = StateGraph(State)
    def loop(state):
        return {'tool_call_count':state['tool_call_count']+1}
    builder.add_node('loop',loop)
    builder.add_edge(START,'loop')
    builder.add_edge('loop','loop')
    processor = EventProcessor(builder.compile(), graph_step_limit=3)
    msg = DiagnosisEvent.model_validate(initial())
    try:
        result = processor.process(msg)
        assert result['status'] == 'FAILED'
        assert result['error_code'] == 'GRAPH_STEP_LIMIT'
        assert result['tool_call_count'] == 3
        assert result['photo_request_count'] == 0
        assert result['outgoing_event']['event_type'] == 'DIAGNOSIS_FAILED'
        assert processor.process(msg) == result
        assert len(processor.outgoing()) == 1
    finally:
        processor.close()


def test_step_limit_does_not_lose_completed_tool_counts():
    c = TestClient(create_app(graph_step_limit=2))
    data = initial()
    data['attachments'][0]['mock_confidence'] = 0.9
    result = c.post('/internal/events', json=data).json()
    assert result['status'] == 'FAILED'
    assert result['error_code'] == 'GRAPH_STEP_LIMIT'
    assert result['tool_call_count'] == 2  # 분석 + RAG가 완료됐지만 완료 노드 전 한도 도달
    assert len(result['executions']) == 2
    assert result['report'] is None


def test_v2_graph_limit_produces_failure_contract():
    raw = json.loads((Path(__file__).parents[1]/'examples/backend-v2.json').read_text())
    c = TestClient(create_app(graph_step_limit=1))
    response = c.post('/internal/backend-v2/events',json=raw)
    assert response.status_code == 200
    result = response.json()
    assert result['backend_events'][-1]['eventType']=='DIAGNOSIS_FAILED'
    assert result['backend_events'][-1]['payload']['errorCode']=='GRAPH_STEP_LIMIT'
