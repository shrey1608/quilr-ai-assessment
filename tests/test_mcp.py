import asyncio
import json
import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.shared.exceptions import McpError
from pydantic import ValidationError

from quilr_gateway.mcp_server import CustomerInput, RefundInput


@pytest.mark.parametrize(
    "value",
    [
        "CUST-1234",
        "CUST-123456",
        "CUST-１２３４５",
        "cust-12345",
        "CUST-12345\n",
        12345,
        True,
        None,
    ],
)
def test_customer_strict(value):
    with pytest.raises(ValidationError):
        CustomerInput.model_validate({"customer_id": value})


@pytest.mark.parametrize("amount", [0, -1, "1.5", True, float("inf"), float("nan")])
def test_refund_strict(amount):
    with pytest.raises(ValidationError):
        RefundInput(customer_id="CUST-00001", amount=amount, reason="Duplicate order")


@pytest.mark.parametrize("reason", ["short", " " * 20, "    short    ", 1234567890])
def test_reason(reason):
    with pytest.raises(ValidationError):
        RefundInput(customer_id="CUST-00001", amount=1.5, reason=reason)


async def test_official_sdk_client_roundtrip(tmp_path):
    with (tmp_path / "stderr.log").open("w") as logs:
        async with (
            stdio_client(
                StdioServerParameters(
                    command=sys.executable, args=["-m", "quilr_gateway.mcp_server"]
                ),
                errlog=logs,
            ) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            listed = await session.list_tools()
            assert {t.name for t in listed.tools} == {"get_customer_record", "trigger_refund"}
            result = await session.call_tool(
                "trigger_refund",
                {"customer_id": "CUST-00001", "amount": 12.5, "reason": "Duplicate payment"},
            )
            assert result.structuredContent["status"] == "simulated"
            assert not result.isError
            with pytest.raises(McpError) as caught:
                await session.call_tool("get_customer_record", {"customer_id": "bad"})
            assert caught.value.error.code == -32602
            with pytest.raises(McpError) as caught:
                await session.call_tool("unknown", {})
            assert caught.value.error.code == -32602
            missing = await session.call_tool("get_customer_record", {"customer_id": "CUST-99999"})
            assert missing.isError
    assert "Starting customer MCP server" in (tmp_path / "stderr.log").read_text()


async def test_raw_stdout_is_only_jsonrpc():
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "quilr_gateway.mcp_server",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def send(payload):
        process.stdin.write((json.dumps(payload) + "\n").encode())
        await process.stdin.drain()

    async def receive():
        line = await asyncio.wait_for(process.stdout.readline(), 5)
        data = json.loads(line)
        assert data["jsonrpc"] == "2.0"
        return data

    try:
        await send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1"},
                },
            }
        )
        assert (await receive())["id"] == 1
        await send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        await send(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "trigger_refund",
                    "arguments": {
                        "customer_id": "CUST-00001",
                        "amount": True,
                        "reason": "Duplicate payment",
                    },
                },
            }
        )
        assert (await receive())["error"]["code"] == -32602
        process.stdin.write(b"{broken-json\n")
        await process.stdin.drain()
        error = await receive()
        assert error["error"]["code"] == -32700
        assert error["id"] is None
        await send({"jsonrpc": "2.0", "id": 3, "method": "no_such_method"})
        assert (await receive())["error"]["code"] == -32601
        await send({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": 42}})
        assert (await receive())["error"]["code"] == -32602
        await send([])
        assert (await receive())["error"]["code"] == -32600
        process.stdin.close()
        await asyncio.wait_for(process.wait(), 5)
        for line in (await process.stdout.read()).splitlines():
            assert json.loads(line)["jsonrpc"] == "2.0"
        assert b"Starting customer" in await process.stderr.read()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
