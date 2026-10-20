"""API 키 없이 실제 stdio PCB MCP와 Agent 분기를 확인한다."""
import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4
from app.models import DiagnosisEvent
from app.observability import configure_logging
from app.pcb import PCBClient, PCBTools
from app.service import EventProcessor
from app.workflow import build_graph


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server-script', default='.local/vendor/MCP_Error_check/pcb_agent/mcp_server.py')
    parser.add_argument('--image', default='.local/vendor/MCP_Error_check/pcb_agent/results/test_before/l_light_05_missing_hole_01_4_600.jpg')
    parser.add_argument('--log-file', default='.local/pcb-live.jsonl')
    parser.add_argument('--result-file', default='.local/pcb-live-result.json')
    parser.add_argument('--timeout', type=float, default=35)
    args = parser.parse_args()
    configure_logging(log_file=args.log_file)
    run_id = str(uuid4())
    tools = PCBTools(PCBClient(args.server_script, timeout=args.timeout), {'pcb-demo-photo': args.image})
    with TemporaryDirectory() as directory:
        processor = EventProcessor(build_graph(tools), str(Path(directory)/'demo.sqlite3'), mode=tools.mode)
        try:
            result = processor.process(DiagnosisEvent.model_validate({
                'event_id': run_id, 'task_id': run_id, 'case_id': run_id,
                'event_type': 'DIAGNOSIS_REQUESTED', 'symptom': 'PCB 손상 의심. 기판 사진 확인 요청',
                'attachments': [{'photo_id': 'pcb-demo-photo', 'target': 'PCB'}]}))
        finally:
            processor.close()
    output = Path(args.result_file)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({'task_id': run_id, 'status': result['status'], 'mode': result['mode'],
                      'pcb': 'live', 'rag': 'mock', 'mechanical': 'mock', 'report': 'template',
                      'tool_call_count': result['tool_call_count'],
                      'photo_request_count': result['photo_request_count'],
                      'error_code': result['error_code'], 'results': result.get('results', []),
                      'result_file': str(output.resolve()), 'log_file': str(Path(args.log_file).resolve())},
                     ensure_ascii=False, indent=2))
    if result['status'] == 'FAILED':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
