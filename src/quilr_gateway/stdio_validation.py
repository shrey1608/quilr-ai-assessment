"""Wire preflight for SDK 1.x, which otherwise logs malformed JSON as a notification.

The SDK still owns stdio framing, initialization, dispatch and cancellation.
This adapter supplies standard JSON-RPC errors before invalid messages reach it.
"""

import asyncio
import json

from mcp import types
from pydantic import ValidationError


def reject_constant(value: str):
    raise ValueError("Non-JSON numeric constant")


class ProtocolOutput:
    def __init__(self, output):
        self.output = output
        self.lock = asyncio.Lock()

    async def write(self, text: str):
        async with self.lock:
            await self.output.write(text)
            await self.output.flush()

    async def flush(self):
        pass  # write() already flushes while holding the lock

    async def error(self, request_id, code: int, message: str):
        await self.write(
            json.dumps(
                {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}
            )
            + "\n"
        )


class ValidatedInput:
    def __init__(self, source, output: ProtocolOutput):
        self.source, self.output = source, output

    async def __aiter__(self):
        async for line in self.source:
            try:
                payload = json.loads(line, parse_constant=reject_constant)
            except ValueError:
                await self.output.error(None, -32700, "Parse error")
                continue
            try:
                types.JSONRPCMessage.model_validate(payload, strict=True)
            except ValidationError:
                await self.output.error(None, -32600, "Invalid Request")
                continue
            if "method" in payload and "id" in payload:
                if payload["method"] not in {"initialize", "ping", "tools/list", "tools/call"}:
                    await self.output.error(payload["id"], -32601, "Method not found")
                    continue
                try:
                    types.ClientRequest.model_validate(payload, strict=True)
                except ValidationError:
                    await self.output.error(payload["id"], -32602, "Invalid params")
                    continue
            yield line
