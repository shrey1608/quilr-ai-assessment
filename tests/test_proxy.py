import json

import httpx
import jwt
import pytest

from quilr_gateway.app import create_app
from quilr_gateway.auth import issue_token


@pytest.mark.parametrize(
    "role,name,blocked",
    [
        ("viewer", "admin_reset_key", True),
        ("admin", "admin_reset_key", False),
        ("viewer", "get_status", False),
    ],
)
async def test_tool_authorization(settings, role, name, blocked):
    calls = []

    async def downstream(request):
        calls.append(request)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": "abc", "result": {"ok": True}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(downstream)) as upstream:
        app = create_app(settings, upstream)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client,
        ):
            response = await client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": "abc",
                    "method": "tools/call",
                    "params": {"name": name},
                },
                headers={"Authorization": "Bearer " + issue_token(settings, role, "t")},
            )
            assert response.json()["id"] == "abc"
            if blocked:
                assert response.json()["error"] == {
                    "code": -32001,
                    "message": "Unauthorized Tool Call",
                }
                assert not calls
            else:
                assert len(calls) == 1
                assert "authorization" not in calls[0].headers


async def test_list_transparent_and_invalid_auth(settings):
    seen = []
    raw = b'{"jsonrpc":"2.0", "id":0, "result":{"tools":[{"name":"admin_secret"}]}}'

    async def downstream(request):
        seen.append(request.content)
        return httpx.Response(200, content=raw, headers={"content-type": "application/json"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(downstream)) as upstream:
        app = create_app(settings, upstream)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client,
        ):
            payload = {"jsonrpc": "2.0", "id": 0, "method": "tools/list"}
            token = issue_token(settings, "viewer", "t")
            response = await client.post(
                "/mcp", json=payload, headers={"Authorization": "Bearer " + token}
            )
            assert response.content == raw
            assert json.loads(seen[0]) == payload
            forged = jwt.encode({"role": "admin"}, "wrong-secret-" + "x" * 32, algorithm="HS256")
            for bad in [
                "",
                "Bearer " + forged,
                "Bearer " + issue_token(settings, "admin", "t", ttl=-1),
            ]:
                response = await client.post("/mcp", json=payload, headers={"Authorization": bad})
                assert response.status_code == 401
            assert len(seen) == 1


@pytest.mark.parametrize(
    "body,code",
    [
        (b"{", -32700),
        (
            b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
            b'"params":{"name":"admin_reset_key","name":"get_status"}}',
            -32600,
        ),
        (
            b'{"jsonrpc":"2.0","id":1,"method":"tools/call","method":"tools/list",'
            b'"params":{"name":"admin_reset_key"}}',
            -32600,
        ),
        (
            b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
            b'"params":{"name":"get_status","arguments":{"amount":NaN}}}',
            -32700,
        ),
        (b"[]", -32600),
        (b"null", -32600),
        (b'{"jsonrpc":"1.0","method":"tools/list","id":1}', -32600),
        (b'{"jsonrpc":"2.0","method":"tools/list","id":true}', -32600),
        (b'{"jsonrpc":"2.0","method":"tools/call","id":1,"params":[]}', -32602),
        (b'{"jsonrpc":"2.0","method":"tools/call","id":1,"params":{"name":5}}', -32602),
    ],
)
async def test_malformed_requests_never_forward(settings, body, code):
    async def downstream(request):
        pytest.fail("Invalid request reached upstream")

    async with httpx.AsyncClient(transport=httpx.MockTransport(downstream)) as upstream:
        app = create_app(settings, upstream)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client,
        ):
            response = await client.post(
                "/mcp",
                content=body,
                headers={"Authorization": "Bearer " + issue_token(settings, "viewer", "t")},
            )
            assert response.json()["error"]["code"] == code
