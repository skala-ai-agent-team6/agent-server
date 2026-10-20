"""Spring inventory.lookup → 팀 업무 MCP get_part_availability 변환. 발주는 Spring 승인 흐름 유지."""
import hmac
import os

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from typing import Literal
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .enrichment import call


class Arguments(BaseModel):
    model_config = ConfigDict(extra="forbid")
    partIds: list[str] = Field(max_length=100)


class Params(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: Literal["inventory.lookup"]
    arguments: Arguments


class RPCRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    jsonrpc: Literal["2.0"]
    id: str
    method: Literal["tools/call"]
    params: Params


async def lookup(part_ids):
    stocks, factories = [], []
    for part in dict.fromkeys(part_ids):
        if not part.isascii() or not part.isdecimal() or int(part) < 1:
            raise ValueError("Invalid part ID")
        raw = await call(os.getenv("BUSINESS_MCP_URL", "http://127.0.0.1:8000/mcp"),
                         "get_part_availability", {"part_id": int(part)})
        if str(raw["part"]["id"]) != part:
            raise ValueError("Part mismatch")
        for row in raw["warehouses"]:
            numbers = [row[k] for k in ("on_hand", "reserved", "available")]
            if any(type(n) is not int or n < 0 for n in numbers) or numbers[2] != numbers[0]-numbers[1]:
                raise ValueError("Invalid inventory")
            stocks.append({"partId": part, "warehouseId": str(row["warehouse_id"]),
                           "onHand": numbers[0], "reserved": numbers[1], "available": numbers[2]})
        factories.extend({"partId": part, "factoryId": str(row["factory_id"]), "region": row["region"],
                          "contact": row["phone"], "lead_days": row["lead_days"]} for row in raw["suppliers"])
    return {"stocks": stocks, "factories": factories}


async def rpc(request: Request):
    token = os.getenv("BUSINESS_GATEWAY_TOKEN", "")
    if not token or not hmac.compare_digest(request.headers.get("Authorization", ""), "Bearer "+token):
        return JSONResponse({"error": "Service authentication required"}, status_code=401)
    try:
        data = RPCRequest.model_validate(await request.json())
    except (ValueError, ValidationError):
        return JSONResponse({"error": "Invalid RPC request"}, status_code=400)
    try:
        result = await lookup(data.params.arguments.partIds)
        body = {"result": {"isError": False, "structuredContent": result}}
    except Exception:
        body = {"error": {"code": -32000, "message": "Business MCP lookup failed"}}
    return JSONResponse({"jsonrpc": "2.0", "id": data.id, **body})


app = Starlette(routes=[Route("/rpc", rpc, methods=["POST"])])
