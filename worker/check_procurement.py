"""실제 진단 완료 건의 재고 조회·승인을 검증하고 시험 주문을 취소한다(로컬 dev 전용)."""
from contextlib import contextmanager
import argparse
import json
import os
from pathlib import Path
import httpx
from dotenv import load_dotenv
from . import config as C


@contextmanager
def login(username):
    client = httpx.Client(base_url=C.SPRING_URL, timeout=30)
    token = client.get('/api/auth/csrf').json()['token']
    result = client.post('/api/auth/login', data={'username': username, 'password': os.environ['DEV_PASSWORD']},
                         headers={'X-CSRF-TOKEN': token})
    result.raise_for_status()
    client.headers['X-CSRF-TOKEN'] = client.get('/api/auth/csrf').json()['token']
    try:
        yield client
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repair-id', required=True)
    args = parser.parse_args()
    if C.SPRING_URL not in ('http://localhost:8080', 'http://127.0.0.1:8080'):
        raise ValueError('Only dedicated local dev server is supported')
    load_dotenv(C.ROOT / '.local/e2e.env')
    with login('tech1') as tech, login('quality') as quality:
        response = tech.get(f'/api/repairs/{args.repair_id}/procurement'); response.raise_for_status()
        procurement = response.json()
        assert procurement['status'] == 'SUCCEEDED', procurement
        results = []
        for order in procurement['orders']:
            order_id = order['id']
            detail = tech.get(f'/api/orders/{order_id}').json()
            assert detail['status'] == 'PENDING', detail
            approved = quality.post(f'/api/orders/{order_id}/approve', json={'reason': '로컬 통합 검증 승인'})
            approved.raise_for_status()
            assert approved.json()['status'] == 'APPROVED'
            cancelled = tech.post(f'/api/orders/{order_id}/cancel', json={'reason': '로컬 검증 종료; 예약 해제'})
            cancelled.raise_for_status()
            assert cancelled.json()['status'] == 'CANCELLED'
            results.append({'order_id': order_id, 'sources': [i['source'] for i in detail['items']],
                            'approved': 'APPROVED', 'cleanup': 'CANCELLED'})
    output = {'repair_id': args.repair_id, 'scope': 'real diagnosis recommendation → Spring → business MCP → approval; local seeded inventory',
              'procurement': 'SUCCEEDED', 'results': results}
    path = C.HERE / f'reports/procurement-{args.repair_id}-summary.json'
    path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
