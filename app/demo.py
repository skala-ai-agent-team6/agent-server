"""네트워크·API 키 없이 Agent 실행 로그를 보는 로컬 시나리오."""
import argparse
import json
import tempfile
from pathlib import Path
from uuid import uuid4
from app.adapters import MockTools
from app.backend_v2 import BackendCommand, ContractPending, process_backend_command
from app.models import DiagnosisEvent
from app.observability import configure_logging, log_event
from app.service import EventProcessor
from app.workflow import build_graph

SCENARIOS = ('success', 'additional', 'limit', 'tool-error', 'duplicate', 'v2-blocked')


class BrokenTools(MockTools):
    def analyze(self, domain, request):
        raise TimeoutError('demo-only')


def expect(condition, message):
    if not condition:
        raise RuntimeError(message)


def run_scenario(name, directory, run_id):
    processor = EventProcessor(build_graph(BrokenTools() if name == 'tool-error' else MockTools()),
                               str(directory / f'{name}.sqlite3'))
    task = f'{run_id}-{name}'
    data = {'event_id': str(uuid4()), 'event_type': 'DIAGNOSIS_REQUESTED',
            'task_id': task, 'case_id': task, 'round_no': 0,
            'symptom': '기사 소견: 기판과 외관 확인 필요',
            'attachments': [{'photo_id': 'pcb-1', 'target': 'PCB', 'mock_readable': True,
                             'mock_confidence': 0.2 if name in ('additional','limit') else 0.9}]}
    log_event('demo.started', scenario=name, demo_run=run_id, mode='mock')
    try:
        if name == 'v2-blocked':
            raw = json.loads((Path(__file__).resolve().parents[1] / 'examples/backend-v2.json').read_text())
            raw['payload']['attachments'][0]['target'] = 'GENERAL'
            try:
                process_backend_command(processor, BackendCommand.model_validate(raw))
            except ContractPending:
                result = {'status': 'BLOCKED_EXPECTED', 'photo_request_count': 0,
                          'tool_call_count': 0, 'retry_count': 0}
            else:
                raise RuntimeError('GENERAL input should remain blocked')
        else:
            if name == 'success':
                data['attachments'].append({'photo_id':'mechanical-1','target':'MECHANICAL'})
            result = processor.process(DiagnosisEvent.model_validate(data))
            if name in ('additional', 'limit'):
                rounds = 1 if name == 'additional' else 3
                for n in range(1, rounds+1):
                    expect(result['status'] == 'WAITING_INPUT', 'Expected a new-input request')
                    data = {**data, 'event_id': str(uuid4()), 'event_type': 'ADDITIONAL_INPUT_SUBMITTED',
                            'round_no': n, 'attachments': data['attachments'] + [{
                                'photo_id': f'pcb-new-{n}', 'target': 'PCB', 'mock_readable': True,
                                'mock_confidence': 0.9 if name == 'additional' else 0.2}]}
                    result = processor.process(DiagnosisEvent.model_validate(data))
                expect(result['status'] == ('SUCCEEDED' if name == 'additional' else 'FAILED'), 'Unexpected final status')
                if name == 'limit':
                    expect(result['error_code'] == 'PHOTO_LIMIT' and result['photo_request_count'] == 3, 'Expected third-round limit')
            elif name == 'duplicate':
                repeated = processor.process(DiagnosisEvent.model_validate(data))
                expect(repeated == result and len(processor.outgoing()) == 1, 'Duplicate executed twice')
            elif name == 'tool-error':
                expect(result['status'] == 'FAILED' and result['retry_count'] == 2 and
                       result['photo_request_count'] == 0, 'Tool retries should not request new photos')
            else:
                expect(result['status'] == 'SUCCEEDED', 'Expected success')
        summary = {key: result[key] for key in ('status','photo_request_count','tool_call_count','retry_count')}
        log_event('demo.finished', scenario=name, demo_run=run_id, mode='mock', **summary)
        return {'scenario': name, 'task_id': task, **summary}
    finally:
        processor.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=('all', *SCENARIOS), default='all')
    parser.add_argument('--log-level', choices=('DEBUG','INFO','WARNING','ERROR'), default='INFO')
    parser.add_argument('--log-file', help='JSONL 로그 파일. 예: .local/demo.jsonl')
    args = parser.parse_args()
    configure_logging(args.log_level, args.log_file)
    run_id = uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix='repair-agent-demo-') as directory:
        summaries = [run_scenario(s, Path(directory), run_id)
                     for s in (SCENARIOS if args.scenario == 'all' else (args.scenario,))]
    print(json.dumps({'mode':'mock', 'demo_run':run_id, 'results':summaries}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
