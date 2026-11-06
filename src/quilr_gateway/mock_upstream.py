"""Local, deterministic mocks. Never expose this unauthenticated app publicly."""

import asyncio
import json

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Assessment mock upstream")


@app.post("/mcp")
async def mcp(request: Request):
    payload = await request.json()
    request_id = payload.get("id")
    method = payload.get("method")
    if "id" not in payload:
        return Response(status_code=202)
    if method == "initialize":
        result = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock", "version": "1.0"},
        }
    elif method == "tools/list":
        result = {
            "tools": [
                {"name": name, "description": "Mock tool", "inputSchema": {"type": "object"}}
                for name in ["get_status", "admin_reset_key"]
            ]
        }
    elif method == "tools/call":
        name = payload["params"]["name"]
        if name not in {"get_status", "admin_reset_key"}:
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32602, "message": "Unknown tool"},
                }
            )
        result = {"content": [{"type": "text", "text": f"Executed {name}"}]}
    else:
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": "Method not found"},
            }
        )
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


@app.post("/{provider}/chat/completions")
async def completion(provider: str, request: Request):
    body = await request.json()
    prompt = body["messages"][-1]["content"]
    if provider == "primary":
        if "[primary-429]" in prompt:
            return JSONResponse({"error": "mock rate limit"}, status_code=429)
        if "[primary-timeout]" in prompt:
            await asyncio.sleep(3.2)
    if "[all-429]" in prompt:
        return JSONResponse({"error": "mock rate limit"}, status_code=429)
    text = "Hello! Email demo@example.com, SSN 123-45-6789, card 4111 1111 1111 1111. Goodbye."
    # ASCII demo tokenizer: one character per token, honors output cap.
    text = text[: body["max_tokens"]]
    usage = {"total_tokens": sum(len(m["content"]) for m in body["messages"]) + len(text)}
    if body.get("stream"):

        async def events():
            for offset in range(0, len(text), 3):
                data = {"choices": [{"index": 0, "delta": {"content": text[offset : offset + 3]}}]}
                yield "data: " + json.dumps(data) + "\n\n"
                await asyncio.sleep(0.01)
            yield "data: " + json.dumps({"choices": [], "usage": usage}) + "\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(events(), media_type="text/event-stream")
    return {
        "model": body["model"],
        "choices": [{"message": {"role": "assistant", "content": text}}],
        "usage": usage,
    }
