"""전용 로컬 Spring/Kafka/실제 MCP·모델·LLM 통합 검증. 접수 데이터를 생성한다."""
import argparse
import json
import hashlib
import os
import sqlite3
import time
from pathlib import Path
from threading import Thread
from uuid import uuid4
import httpx
from dotenv import load_dotenv
from . import config as C
from .journal import VALIDATOR


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', choices=['pcb', 'mixed', 'mechanical', 'retry-limit', 'dent'], default='mixed')
    args = parser.parse_args()
    load_dotenv(C.ROOT / '.local/e2e.env')
    pcb = C.ROOT.parent / 'MCP_Error_check/pcb_agent/results/test_before/l_light_05_missing_hole_01_4_600.jpg'
    mech = C.HERE / 'samples/mech_sample.jpg'
    if args.scenario == 'retry-limit':
        import struct, zlib
        blank_photos = []
        def chunk(kind, data):
            return struct.pack('>I', len(data)) + kind + data + struct.pack('>I', zlib.crc32(kind + data))
        # 추가 사진 내용이 바뀌어도 작업 전체의 재입력 3회 제한이 유지되는지 검증한다.
        (C.ROOT / '.local').mkdir(exist_ok=True)
        for index in range(4):
            blank = C.ROOT / f'.local/blank-round-{index}.png'
            width = 640 + index
            blank.write_bytes(b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', width, 640, 8, 2, 0, 0, 0))
                              + chunk(b'IDAT', zlib.compress((b'\x00' + b'\xff' * (width * 3)) * 640)) + chunk(b'IEND', b''))
            blank_photos.append(blank)
        assert len({hashlib.sha256(p.read_bytes()).hexdigest() for p in blank_photos}) == 4
        photos = [(blank_photos[0], 'PCB')]
    elif args.scenario == 'dent':
        photos = [(Path(os.environ['DENT_TEST_IMAGE']).resolve(), 'MOTOR')]
    elif args.scenario == 'pcb':
        photos = [(pcb, 'PCB')]
    elif args.scenario == 'mechanical':
        photos = [(mech, 'MOTOR')]
    else:
        photos = [(pcb, 'PCB'), (C.HERE / 'samples/pcb_sample.jpg', 'PCB'), (mech, 'MOTOR')]
    client = httpx.Client(base_url=C.SPRING_URL, timeout=30)
    token = client.get('/api/auth/csrf').json()['token']
    response = client.post('/api/auth/login', data={'username': 'tech1', 'password': os.environ['DEV_PASSWORD']},
                           headers={'X-CSRF-TOKEN': token})
    response.raise_for_status()
    client.headers['X-CSRF-TOKEN'] = client.get('/api/auth/csrf').json()['token']
    data = {'productId': '1', 'symptoms': '전원이 켜지지 않아요. PCB 또는 외형 손상이 의심됩니다.',
            'errorCodes': [], 'firmware': 'unknown', 'userInput': {}, 'photoTargets': [t for _, t in photos]}
    response = client.post('/api/repairs', files=[('data', ('', json.dumps(data, ensure_ascii=False), 'application/json'))]
                           + [('files', (p.name, p.read_bytes(), 'image/png' if p.suffix == '.png' else 'image/jpeg')) for p, _ in photos])
    response.raise_for_status()
    created = response.json(); repair = created['repairId']; task = created['taskId']
    print(json.dumps({'stage': 'created', 'scenario': args.scenario, 'repair': repair, 'task': task}), flush=True)
    events, progress_status, stream_errors = [], [], []
    terminal = 'DIAGNOSIS_FAILED' if args.scenario == 'retry-limit' else 'DIAGNOSIS_COMPLETED'
    def collect():
        try:
            with client.stream('GET', f'/api/repairs/{repair}/events', timeout=120,
                               headers={'Accept': 'text/event-stream'}) as response:
                response.raise_for_status()
                for line in response.iter_lines():
                    if line.startswith('event:'):
                        name = line[6:].strip(); events.append(name)
                        if name == 'DIAGNOSIS_PROGRESS':
                            progress_status.append(client.get(f'/api/repairs/{repair}/diagnosis').json()['status'])
                        if name == terminal:
                            return
        except Exception as exc:
            stream_errors.append(type(exc).__name__)
    listener = Thread(target=collect, daemon=True); listener.start()
    rounds = 0
    while True:
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            response = client.get(f'/api/repairs/{repair}/diagnosis'); response.raise_for_status(); state = response.json()
            if state['status'] in ('SUCCEEDED', 'FAILED', 'WAITING_INPUT'):
                break
            time.sleep(.5)
        else:
            raise AssertionError('Spring terminal/waiting status timeout')
        if state['status'] != 'WAITING_INPUT':
            break
        assert args.scenario == 'retry-limit', state
        rounds += 1
        assert rounds <= 3
        detail = client.get(f'/api/repairs/{repair}').json()
        request = next(r for r in detail['photoRequests'] if r['submitted_at'] is None)
        p, target = blank_photos[rounds], 'PCB'
        response = client.post(f'/api/repairs/{repair}/attachments', params={'requestId': request['id'], 'target': target},
                               files={'file': (p.name, p.read_bytes(), 'image/png')})
        response.raise_for_status(); added = response.json()['attachmentId']
        body = {'requestId': request['id'], 'submissionKey': str(uuid4()), 'attachmentIds': [added]}
        response = client.post(f'/api/repairs/{repair}/diagnosis/resume', json=body); response.raise_for_status()
        duplicate = client.post(f'/api/repairs/{repair}/diagnosis/resume', json=body); duplicate.raise_for_status()
        assert duplicate.json() == response.json()
        print(json.dumps({'stage': 'resumed', 'round': rounds, 'repair': repair}), flush=True)
    expected = 'FAILED' if args.scenario == 'retry-limit' else 'SUCCEEDED'
    assert state['status'] == expected, state
    assert rounds == (3 if args.scenario == 'retry-limit' else 0)
    listener.join(timeout=10)
    assert not listener.is_alive() and not stream_errors, stream_errors
    assert terminal in events and 'RUNNING' in progress_status, (events, progress_status)
    files = sorted((C.HERE / 'reports').glob(f'task-{task}_revision-*.json'))
    assert len(files) == rounds + 1
    reports = [json.loads(p.read_text()) for p in files]
    assert reports[-1]['result']['eventType'] == terminal
    connection = sqlite3.connect(C.STATE_PATH)
    records = [json.loads(row[0]) for row in connection.execute('SELECT body FROM events') if json.loads(row[0])['taskId'] == task]
    for e in records:
        VALIDATOR.validate(e)
    tools = [e for e in records if e['eventType'] == 'TOOL_EXECUTED']
    errors = [e['payload']['output'] for e in tools if e['payload']['output'].get('error')]
    assert not errors, errors
    if args.scenario == 'dent':
        assert any(e['payload']['tool'] == 'interpret_damage' for e in tools)
    summary = {'scenario': args.scenario, 'status': expected, 'task_id': task, 'repair_id': repair,
               'additional_requests': rounds, 'initial_photos': len(photos), 'sse_events': events,
               'different_additional_photos': args.scenario == 'retry-limit',
               'progress_received_while_running': True, 'tool_calls': [e['payload']['tool'] for e in tools],
               'tool_errors': errors, 'report_files': [str(p.resolve()) for p in files],
               'scope': 'real Spring/Kafka/Agent/MCP/RAG/LLM; local dev users and seeded product data'}
    (C.HERE / f'reports/e2e-{args.scenario}-summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    client.close(); connection.close()


if __name__ == '__main__':
    main()
