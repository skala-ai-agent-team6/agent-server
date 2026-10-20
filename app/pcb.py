"""팀 PCB stdio MCP 연결. 기계 분석·RAG는 아직 모의 도구다."""
import asyncio
import json
import math
import os
import sys
from datetime import timedelta
from pathlib import Path
from app.adapters import MockTools
from app.observability import context, log_event


class PCBToolError(ValueError):
    """재시도로 해결되지 않는 파일·계약·모델 응답 오류."""


class PCBClient:
    def __init__(self, server_script, python=sys.executable, timeout=60):
        self.script = Path(server_script).expanduser().resolve(strict=True)
        self.python, self.timeout = python, timeout
        if timeout <= 0:
            raise ValueError('timeout must be positive')

    def call(self, image_path):
        return asyncio.run(self._bounded_call(str(image_path)))

    async def _bounded_call(self, image_path):
        from mcp.shared.exceptions import McpError
        try:
            return await asyncio.wait_for(self._call(image_path), timeout=self.timeout)
        except ExceptionGroup as exc:
            # stdio/AnyIO 정리 과정에서 감싼 원래 오류를 복원한다.
            def leaves(group):
                for error in group.exceptions:
                    if isinstance(error, BaseExceptionGroup):
                        yield from leaves(error)
                    else:
                        yield error
            errors = list(leaves(exc))
            if any(isinstance(e, TimeoutError) or isinstance(e, McpError) and e.error.code == 408 for e in errors):
                raise TimeoutError('PCB MCP session timed out') from exc
            if all(isinstance(e, (ConnectionError, OSError)) for e in errors):
                raise ConnectionError('MCP transport failed') from exc
            raise PCBToolError('MCP session failed; inspect server stderr') from exc
        except McpError as exc:
            if exc.error.code == 408:
                raise TimeoutError('PCB MCP session timed out') from exc
            raise PCBToolError('MCP protocol error') from exc
        except TimeoutError:
            raise
        except OSError as exc:
            raise PCBToolError('Cannot start PCB MCP process') from exc

    async def _call(self, image_path):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
        runtime = Path('.local/runtime').resolve()
        runtime.mkdir(parents=True, exist_ok=True)
        params = StdioServerParameters(
            command=self.python, args=[str(self.script)], cwd=str(self.script.parent),
            env={'YOLO_CONFIG_DIR': str(runtime / 'yolo'),
                 'MPLCONFIGDIR': str(runtime / 'matplotlib'),
                 'PYTHONUNBUFFERED': '1'})
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=self.timeout)) as session:
                await session.initialize()
                listing = await session.list_tools()
                names = [tool.name for tool in listing.tools]
                if 'analyze_pcb_image' not in names:
                    raise PCBToolError('PCB MCP does not advertise analyze_pcb_image')
                response = await session.call_tool('analyze_pcb_image', {'image_path': image_path, 'conf': 0.25})
                if response.isError:
                    raise PCBToolError('PCB MCP returned a tool error')
                value = response.structuredContent
                if value is None:
                    parts = [part.text for part in response.content if part.type == 'text']
                    try:
                        value = json.loads(''.join(parts))
                    except (ValueError, TypeError) as exc:
                        raise PCBToolError('PCB MCP returned invalid JSON') from exc
                if not isinstance(value, dict) or value.get('error'):
                    raise PCBToolError('PCB MCP returned an invalid result or image error')
                return value


class PCBTools(MockTools):
    mode = 'mixed'

    def __init__(self, client, photos):
        self.client = client
        # photo_id를 서버 관리 경로로 매핑. 요청 본문의 임의 경로를 열지 않는다.
        self.photos = {key: Path(path).expanduser().resolve() for key, path in photos.items()}

    def analyze(self, domain, request):
        if domain != 'PCB':
            return super().analyze(domain, request)
        photo = [a for a in request['attachments'] if a['target'] == 'PCB'][-1]
        path = self.photos.get(photo['photo_id'])
        if path is None or not path.is_file():
            raise PCBToolError('PCB photo_id is not mapped to a readable local file')
        log_event('mcp.call_started', **context(request), server='pcb-detector',
                  transport='stdio', tool='analyze_pcb_image', mode_detail='live')
        raw = self.client.call(path)
        defects = raw.get('defects')
        if not isinstance(defects, list) or not isinstance(raw.get('is_defective'), bool):
            raise PCBToolError('Invalid PCB result schema')
        counts = {}
        allowed = {'missing_hole', 'mouse_bite', 'open_circuit', 'short', 'spur', 'spurious_copper'}
        for item in defects:
            if not isinstance(item, dict):
                raise PCBToolError('Invalid detection')
            score, box = item.get('confidence'), item.get('box_xyxy')
            if (item.get('type') not in allowed or type(score) not in (float, int)
                    or not math.isfinite(score) or not 0 <= score <= 1
                    or not isinstance(box, list) or len(box) != 4
                    or any(type(n) not in (float, int) or not math.isfinite(n) for n in box)):
                raise PCBToolError('Invalid detection class, confidence or bounding box')
            counts[item['type']] = counts.get(item['type'], 0) + 1
        if raw['is_defective'] != bool(defects) or raw.get('defect_counts') != counts:
            raise PCBToolError('Inconsistent detection summary')
        # 검출 확률은 고장 원인 확률이 아니다. 미검출은 정상으로 확정하지 않는다.
        confidence = min((item['confidence'] for item in defects), default=0.0)
        finding = ('PCB 결함 검출: ' + ', '.join(f'{k} {v}개' for k, v in counts.items())
                   if defects else '결함 미검출. 정상 여부·고장 원인은 추가 확인 필요')
        log_event('mcp.call_completed', **context(request), server='pcb-detector',
                  tool='analyze_pcb_image', defect_count=len(defects), defect_counts=counts,
                  detection_score=confidence, score_kind='minimum_box_confidence')
        return {'domain': 'PCB', 'sufficient': bool(defects), 'confidence': confidence,
                'finding': finding, 'evidence_ids': [photo['photo_id']],
                'annotated_image': raw.get('annotated_image'), 'defects': defects,
                'defect_counts': counts, 'mode': 'live', 'mcp_server': 'pcb-detector',
                'tool_version': 'analyze_pcb_image-v1', 'model_version': 'team-best.pt',
                'score_kind': 'minimum_box_confidence', 'diagnosis_confirmed': False}


def configured_tools():
    mode = os.getenv('AGENT_TOOL_MODE', 'mock')
    if mode == 'mock':
        return MockTools()
    if mode != 'pcb-live':
        raise ValueError('AGENT_TOOL_MODE must be mock or pcb-live')
    manifest = json.loads(Path(os.environ['PCB_PHOTO_MANIFEST']).read_text())
    if not isinstance(manifest, dict) or any(not isinstance(v, str) for v in manifest.values()):
        raise ValueError('PCB_PHOTO_MANIFEST must be a photo_id to absolute path object')
    return PCBTools(PCBClient(os.getenv('PCB_MCP_SCRIPT', '.local/vendor/MCP_Error_check/pcb_agent/mcp_server.py'),
                              python=os.getenv('PCB_MCP_PYTHON', sys.executable),
                              timeout=float(os.getenv('PCB_MCP_TIMEOUT', '35'))), manifest)
