"""Task 2: scoped POST HTTP/JSON-RPC proxy (not a full MCP SSE/session gateway)."""

import json

import httpx
from fastapi import Request, Response

from .auth import authenticate
from .config import Settings
from .errors import GatewayError, rpc_error

MAX_BODY = 64 * 1024


async def proxy(request: Request, client: httpx.AsyncClient, settings: Settings) -> Response:
    raw = bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw) > MAX_BODY:
            return rpc_error(None, -32600, "Request too large", 413)
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return rpc_error(None, -32700, "Parse error", 400)
    if not isinstance(payload, dict):
        return rpc_error(None, -32600, "Invalid Request", 400)
    request_id = payload.get("id")
    if (
        payload.get("jsonrpc") != "2.0"
        or not isinstance(payload.get("method"), str)
        or ("id" in payload and type(request_id) not in (str, int))
    ):
        return rpc_error(None, -32600, "Invalid Request", 400)
    try:
        identity = authenticate(request.headers.get("authorization"), settings)
    except GatewayError:
        return rpc_error(request_id, -32001, "Unauthorized", 401)
    params = payload.get("params", {})
    if not isinstance(params, dict):
        return rpc_error(request_id, -32602, "Invalid params")
    method = payload["method"]
    notification = "id" not in payload
    # Tool invocations must have request IDs in MCP. Never execute them as notifications.
    if notification and method in ("tools/call", "tools/list"):
        return Response(status_code=202)
    if method == "tools/call":
        name = params.get("name")
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(params.get("arguments", {}), dict)
        ):
            return rpc_error(request_id, -32602, "Invalid params")
        if name.startswith("admin_") and identity.role != "admin":
            return rpc_error(request_id, -32001, "Unauthorized Tool Call")
    try:
        response = await client.post(
            settings.mcp_upstream_url,
            content=bytes(raw),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=3.0,
        )
    except httpx.RequestError:
        if notification:
            return Response(status_code=202)
        return rpc_error(request_id, -32603, "Downstream unavailable", 502)
    if notification:
        return Response(status_code=202)
    if response.status_code >= 400:
        return rpc_error(request_id, -32603, "Downstream unavailable", 502)
    # Successful responses (including tools/list) are forwarded byte-for-byte.
    return Response(
        response.content,
        status_code=response.status_code,
        media_type=response.headers.get("content-type", "application/json"),
    )
