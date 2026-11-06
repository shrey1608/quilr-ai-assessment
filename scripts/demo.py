"""One-command demonstration using real local HTTP processes and the SDK client.

No .env or provider keys required. Temporary SQLite and random signing secret.
"""

import asyncio
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from quilr_gateway.auth import issue_token
from quilr_gateway.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def main():
    processes = []
    with tempfile.TemporaryDirectory(prefix="quilr-demo-") as temp:
        primary_port, gateway_port = free_port(), free_port()
        settings = Settings(
            _env_file=None,
            jwt_secret=secrets.token_urlsafe(48),
            database_path=str(Path(temp) / "gateway.sqlite3"),
            mcp_upstream_url=f"http://127.0.0.1:{primary_port}/mcp",
            primary_url=f"http://127.0.0.1:{primary_port}/primary/chat/completions",
            secondary_url=f"http://127.0.0.1:{primary_port}/secondary/chat/completions",
        )
        env = os.environ | {
            "JWT_SECRET": settings.jwt_secret.get_secret_value(),
            "DATABASE_PATH": settings.database_path,
            "MCP_UPSTREAM_URL": settings.mcp_upstream_url,
            "PRIMARY_URL": settings.primary_url,
            "SECONDARY_URL": settings.secondary_url,
            "PRIMARY_MODEL": "mock-primary",
            "SECONDARY_MODEL": "mock-secondary",
            "TOKENS_PER_MINUTE": "50000",
            "PROVIDER_TIMEOUT_SECONDS": "3.0",
            "JWT_ISSUER": settings.jwt_issuer,
            "JWT_AUDIENCE": settings.jwt_audience,
        }
        with (Path(temp) / "servers.log").open("w+") as log:
            try:
                for app, port, factory in [
                    ("quilr_gateway.mock_upstream:app", primary_port, []),
                    ("quilr_gateway.app:create_app", gateway_port, ["--factory"]),
                ]:
                    processes.append(
                        await asyncio.to_thread(
                            subprocess.Popen,
                            [
                                sys.executable,
                                "-m",
                                "uvicorn",
                                app,
                                "--host",
                                "127.0.0.1",
                                "--port",
                                str(port),
                                *factory,
                            ],
                            cwd=ROOT,
                            env=env,
                            stdout=log,
                            stderr=log,
                        )
                    )
                async with httpx.AsyncClient(
                    base_url=f"http://127.0.0.1:{gateway_port}", timeout=10, trust_env=False
                ) as client:
                    for _ in range(100):
                        try:
                            ready = await client.get("/health")
                            mock = await client.get(f"http://127.0.0.1:{primary_port}/openapi.json")
                            if ready.status_code == mock.status_code == 200:
                                break
                        except httpx.RequestError:
                            pass
                        await asyncio.sleep(0.1)
                    else:
                        raise RuntimeError("Local services did not start")
                    async with (
                        stdio_client(
                            StdioServerParameters(
                                command=sys.executable, args=["-m", "quilr_gateway.mcp_server"]
                            ),
                            errlog=log,
                        ) as (read, write),
                        ClientSession(read, write) as session,
                    ):
                        await session.initialize()
                        result = await session.call_tool(
                            "get_customer_record", {"customer_id": "CUST-00001"}
                        )
                        assert result.structuredContent["customer_id"] == "CUST-00001"
                        print("PASS Task 1: official MCP client -> stdio server")
                    viewer = {"Authorization": "Bearer " + issue_token(settings, "viewer", "demo")}
                    admin = {"Authorization": "Bearer " + issue_token(settings, "admin", "demo")}
                    call = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "admin_reset_key"},
                    }
                    denied = await client.post("/mcp", headers=viewer, json=call)
                    allowed = await client.post("/mcp", headers=admin, json=call)
                    assert denied.json()["error"]["code"] == -32001
                    assert "result" in allowed.json()
                    print("PASS Task 2: viewer blocked, admin forwarded")
                    body = {"messages": [{"role": "user", "content": "Hello"}], "max_tokens": 256}
                    started = time.monotonic()
                    first_delta = None
                    text = ""
                    async with client.stream(
                        "POST", "/v1/chat/completions/stream", headers=viewer, json=body
                    ) as response:
                        async for line in response.aiter_lines():
                            if line == "data: [DONE]":
                                break
                            if line.startswith("data: "):
                                delta = json.loads(line[6:])["delta"]
                                first_delta = first_delta or time.monotonic() - started
                                text += delta
                    elapsed = time.monotonic() - started
                    assert text.count("[REDACTED]") == 3, text
                    assert first_delta is not None and first_delta < elapsed
                    print(f"PASS Task 3: {text}")
                    print(f"  First safe delta: {first_delta:.3f}s; total: {elapsed:.3f}s")
                    for mode in ["[primary-429]", "[primary-timeout]"]:
                        body["messages"][0]["content"] = mode
                        result = await client.post(
                            "/v1/chat/completions", headers=viewer, json=body
                        )
                        assert result.json()["model"] == "mock-secondary", result.text
                        print(f"PASS Task 4: {mode} -> secondary provider")
                    print("All four tasks demonstrated successfully.")
            except BaseException:
                log.flush()
                log.seek(0)
                sys.stderr.write(log.read())
                raise
            finally:
                for process in processes:
                    process.terminate()
                for process in processes:
                    try:
                        await asyncio.to_thread(process.wait, timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        await asyncio.to_thread(process.wait)


if __name__ == "__main__":
    asyncio.run(main())
