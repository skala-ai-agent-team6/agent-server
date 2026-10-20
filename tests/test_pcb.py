from copy import deepcopy
from threading import Barrier
import pytest
from fastapi.testclient import TestClient
from app.adapters import MockTools
from app.main import create_app
from app.pcb import PCBClient, PCBTools, PCBToolError
from app.models import DiagnosisEvent
from app.service import EventProcessor
from app.workflow import build_graph


def event():
    return DiagnosisEvent(event_id='pcb-e', task_id='pcb-t', case_id='pcb-c',
                          event_type='DIAGNOSIS_REQUESTED', symptom='기판 손상',
                          attachments=[{'photo_id': 'pcb-1', 'target': 'PCB'}])


def detection():
    return {'is_defective': True, 'defect_counts': {'missing_hole': 1},
            'defects': [{'type': 'missing_hole', 'confidence': .87, 'box_xyxy': [1, 2, 10, 20]}],
            'annotated_image': '/result/annotated.jpg'}


class Client:
    def __init__(self, value):
        self.value, self.calls = value, 0

    def call(self, path):
        self.calls += 1
        return self.value


def test_live_adapter_normalizes_real_output_and_deduplicates(tmp_path):
    image = tmp_path/'image.jpg'
    image.write_bytes(b'test')
    client = Client(detection())
    tools = PCBTools(client, {'pcb-1': image})
    processor = EventProcessor(build_graph(tools), mode=tools.mode)
    result = processor.process(event())
    assert result['mode'] == result['report']['mode'] == 'mixed'
    assert result['results'][0]['defect_counts'] == {'missing_hole': 1}
    assert result['report']['candidates'][0]['cause_code'] == 'DETECTED_PCB'
    assert result['results'][0]['diagnosis_confirmed'] is False
    assert processor.process(event()) == result
    assert client.calls == 1
    processor.close()


def test_no_detections_requests_input_but_still_searches_rag(tmp_path):
    image = tmp_path/'image.jpg'
    image.touch()
    tools = PCBTools(Client({'is_defective': False, 'defect_counts': {}, 'defects': []}), {'pcb-1': image})
    processor = EventProcessor(build_graph(tools), mode=tools.mode)
    result = processor.process(event())
    assert result['status'] == 'WAITING_INPUT'
    assert result['photo_request_count'] == 1
    assert result['sources'][0]['source_uri'].startswith('mock://')
    assert result['tool_call_count'] == 2
    processor.close()


@pytest.mark.parametrize('change', ['class', 'nan', 'box', 'counts'])
def test_malformed_detections_fail_without_photo_request_or_retry(tmp_path, change):
    image = tmp_path/'image.jpg'
    image.touch()
    raw = deepcopy(detection())
    if change == 'class':
        raw['defects'][0]['type'] = 'unsupported'
    elif change == 'nan':
        raw['defects'][0]['confidence'] = float('nan')
    elif change == 'box':
        raw['defects'][0]['box_xyxy'] = []
    else:
        raw['defect_counts'] = {}
    processor = EventProcessor(build_graph(PCBTools(Client(raw), {'pcb-1': image})), mode='mixed')
    result = processor.process(event())
    assert result['status'] == 'FAILED'
    assert result['error_code'] == 'PCBToolError'
    assert result['photo_request_count'] == result['retry_count'] == 0
    assert any(e['status'] == 'FAILED' for e in result['executions'])
    processor.close()


def test_unmapped_photo_fails_without_silent_mock_fallback():
    client = Client(detection())
    processor = EventProcessor(build_graph(PCBTools(client, {})), mode='mixed')
    result = processor.process(event())
    assert result['error_code'] == 'PCBToolError'
    assert client.calls == 0
    processor.close()


def test_pcb_mechanical_and_rag_start_in_same_stage_even_when_insufficient():
    class ParallelTools(MockTools):
        barrier = Barrier(3)

        def analyze(self, domain, request):
            self.barrier.wait(timeout=3)
            value = super().analyze(domain, request)
            value['sufficient'] = False
            return value

        def search(self, symptom):
            self.barrier.wait(timeout=3)
            return super().search(symptom)

    msg = event().model_dump()
    msg['attachments'].append({'photo_id': 'mechanical', 'target': 'MECHANICAL'})
    processor = EventProcessor(build_graph(ParallelTools()))
    result = processor.process(DiagnosisEvent.model_validate(msg))
    assert result['status'] == 'WAITING_INPUT'
    assert result['missing'] == ['MECHANICAL', 'PCB']
    assert result['tool_call_count'] == 3
    assert result['sources']
    processor.close()


def test_real_stdio_protocol_with_small_test_server(tmp_path):
    pytest.importorskip('mcp')
    script = tmp_path/'server.py'
    script.write_text('''from mcp.server.fastmcp import FastMCP
mcp = FastMCP("pcb-test")
@mcp.tool()
def analyze_pcb_image(image_path: str, conf: float = .25) -> dict:
    return {"is_defective": False, "defect_counts": {}, "defects": [], "image": image_path}
if __name__ == "__main__":
    mcp.run()
''')
    raw = PCBClient(script, timeout=10).call('/test/photo.jpg')
    assert raw['image'] == '/test/photo.jpg'
    assert raw['defects'] == []


def test_live_health_identifies_mixed_mode():
    app = create_app(PCBTools(Client(detection()), {}))
    assert TestClient(app).get('/health').json()['mode'] == 'mixed'


def test_stdio_timeout_terminates_unresponsive_server(tmp_path):
    pytest.importorskip('mcp')
    from time import monotonic
    script = tmp_path/'silent.py'
    script.write_text('import time\ntime.sleep(60)\n')
    started = monotonic()
    with pytest.raises(TimeoutError):
        PCBClient(script, timeout=.2).call('/unused.jpg')
    assert monotonic() - started < 5


def test_mcp_error_response_is_not_retried_as_success(tmp_path):
    pytest.importorskip('mcp')
    script = tmp_path/'bad.py'
    script.write_text('''from mcp.server.fastmcp import FastMCP
mcp = FastMCP("pcb-test")
@mcp.tool()
def analyze_pcb_image(image_path: str, conf: float = .25) -> dict:
    return {"error": "missing image"}
if __name__ == "__main__":
    mcp.run()
''')
    with pytest.raises(PCBToolError):
        PCBClient(script, timeout=10).call('/missing.jpg')
