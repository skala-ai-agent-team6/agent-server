"""실행 한도 장애 시연. 합성 도구/그래프 + 로컬 입출력이며 실제 모델·Kafka는 사용하지 않는다."""
import asyncio
import copy
import json
import uuid
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

from langgraph.graph import START, StateGraph
from . import config as C, harness, main, report
from .journal import Journal
from .send_test import COMMAND
from .verify_live import LocalOutput


async def run():
    summaries = []
    scenarios = {
        'TOOL_CALL_LIMIT': {'MAX_TOOL_CALLS': 1},
        'TOOL_REPEAT_LIMIT': {},
        'NODE_EXECUTION_LIMIT': {'MAX_NODE_EXECUTIONS': 2},
        'GRAPH_STEP_LIMIT': {'RECURSION_LIMIT': 2},
        'RUN_TIMEOUT': {'RUN_TIMEOUT': .02},
        'TOOL_TIMEOUT': {'TOOL_TIMEOUT': .02},
    }
    for code, settings in scenarios.items():
        @harness.guarded()
        async def synthetic_tool(value):
            if code in ('RUN_TIMEOUT', 'TOOL_TIMEOUT'):
                await asyncio.Event().wait()
            return value

        @harness.observed
        async def synthetic_node(state, config):
            return {}

        async def invoke(*args, **kwargs):
            if code == 'NODE_EXECUTION_LIMIT':
                for _ in range(3):
                    await synthetic_node({}, {})
            else:
                for i in range(4):
                    await synthetic_tool('same' if code == 'TOOL_REPEAT_LIMIT' else i)
            raise AssertionError('Expected an execution limit')

        graph = SimpleNamespace(ainvoke=invoke)
        if code == 'GRAPH_STEP_LIMIT':
            builder = StateGraph(dict)
            builder.add_node('loop', synthetic_node)
            builder.add_edge(START, 'loop')
            builder.add_edge('loop', 'loop')
            graph = builder.compile()
        cmd = copy.deepcopy(COMMAND)
        task = str(uuid.uuid4().int % 100000000 + 1)
        cmd.update(eventId=str(uuid.uuid4()), taskId=task, repairId=task,
                   executionId=str(uuid.uuid4()), correlationId=str(uuid.uuid4()))
        cmd['payload']['task']['id'] = cmd['payload']['repair']['id'] = task
        output = LocalOutput()
        journal = Journal(C.ROOT/'.local/limit-check.sqlite3')
        try:
            with ExitStack() as stack:
                for key, value in settings.items():
                    stack.enter_context(patch.object(C, key, value))
                stack.enter_context(patch.object(main, 'GRAPH', graph))
                result = await main.handle(cmd, output, journal)
        finally:
            journal.close()
        assert result == {'eventType': 'DIAGNOSIS_FAILED', 'payload': {'errorCode': code}}
        path = report.OUT/f'task-{task}_revision-1_고장리포트.md'
        assert code in path.read_text()
        saved = json.loads(path.with_suffix('.json').read_text())
        assert saved['result'] == result
        assert not any(e['eventType'] == 'PHOTO_REQUESTED' for e in output.events)
        summaries.append({'code': code, 'report': str(path), 'metrics': saved['execution']})
    path = report.OUT/'limit-check-summary.json'
    path.write_text(json.dumps({'mode': 'synthetic-tools-local-events-no-real-model-or-Kafka',
                               'scenarios': summaries}, ensure_ascii=False, indent=2))
    print(f'6 limit failure reports verified: {path}')
    return summaries


if __name__ == '__main__':
    asyncio.run(run())
