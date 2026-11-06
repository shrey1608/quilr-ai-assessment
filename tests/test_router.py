import asyncio
import json

import httpx
import pytest

from quilr_gateway.app import create_app
from quilr_gateway.auth import issue_token
from quilr_gateway.errors import GatewayError
from quilr_gateway.providers import CompletionRequest, ProviderRouter

BODY = CompletionRequest(messages=[{"role": "user", "content": "Hello"}], max_tokens=100)
SUCCESS = {"choices": [{"message": {"content": "Hello"}}], "usage": {"total_tokens": 12}}


@pytest.mark.parametrize("failure", ["429", "timeout"])
async def test_primary_failure_falls_back_and_cancels(settings, failure):
    settings.provider_timeout_seconds = 0.02
    seen = []
    cancelled = []

    async def provider(request):
        seen.append(request.url.path)
        if request.url.path == "/primary":
            if failure == "429":
                return httpx.Response(429, json={"secret": "internal trace"})
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.append(True)
        return httpx.Response(200, json=SUCCESS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        result = await ProviderRouter(client, settings).complete(BODY)
    assert result["model"] == "mock-secondary"
    assert seen == ["/primary", "/secondary"]
    assert cancelled == ([True] if failure == "timeout" else [])


async def test_no_fallback_for_other_status_and_no_error_leak(settings):
    seen = []

    async def provider(request):
        seen.append(request.url.path)
        return httpx.Response(500, text="super-secret upstream stacktrace")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        with pytest.raises(GatewayError) as caught:
            await ProviderRouter(client, settings).complete(BODY)
    assert seen == ["/primary"]
    assert "super-secret" not in json.dumps(caught.value.payload())


async def test_complete_and_stream_end_to_end(settings):
    async def provider(request):
        if not json.loads(request.content).get("stream"):
            return httpx.Response(200, json=SUCCESS)
        text = "Hi! alice@example.com SSN 123-45-6789 card 4111 1111 1111 1111."
        frames = [
            "data: " + json.dumps({"choices": [{"delta": {"content": c}}]}) + "\n\n" for c in text
        ]
        frames += ['data: {"choices":[],"usage":{"total_tokens":40}}\n\n', "data: [DONE]\n\n"]
        return httpx.Response(
            200, text="".join(frames), headers={"content-type": "text/event-stream"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
        app = create_app(settings, upstream)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client,
        ):
            headers = {"Authorization": "Bearer " + issue_token(settings, "viewer", "t")}
            response = await client.post(
                "/v1/chat/completions", json=BODY.model_dump(), headers=headers
            )
            assert response.json()["usage"]["total_tokens"] == 12
            response = await client.post(
                "/v1/chat/completions/stream", json=BODY.model_dump(), headers=headers
            )
            assert "[DONE]" in response.text
            deltas = [
                json.loads(line[6:])["delta"]
                for line in response.text.splitlines()
                if line.startswith("data: {")
            ]
            assert "".join(deltas) == "Hi! [REDACTED] SSN [REDACTED] card [REDACTED]."


async def test_failed_stream_does_not_flush_partial_pii(settings):
    async def provider(request):
        return httpx.Response(
            200,
            text='data: {"choices":[{"delta":{"content":"Hi ali"}}]}\n\n',
            headers={"content-type": "text/event-stream"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
        app = create_app(settings, upstream)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client,
        ):
            response = await client.post(
                "/v1/chat/completions/stream",
                json=BODY.model_dump(),
                headers={"Authorization": "Bearer " + issue_token(settings, "viewer", "t")},
            )
            assert "event: error" in response.text
            assert '"delta": "Hi "' in response.text
            assert '"delta": "ali' not in response.text
            assert "[DONE]" not in response.text


async def test_absolute_timeout_includes_slow_response_body(settings):
    settings.provider_timeout_seconds = 0.02
    closed = []

    class SlowBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"choices":'
            await asyncio.sleep(10)
            yield b"[]}"

        async def aclose(self):
            closed.append(True)

    async def provider(request):
        if request.url.path == "/primary":
            return httpx.Response(200, stream=SlowBody())
        return httpx.Response(200, json=SUCCESS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        result = await ProviderRouter(client, settings).complete(BODY)
    assert result["model"] == "mock-secondary"
    assert closed == [True]


@pytest.mark.parametrize("failure", ["429", "timeout"])
async def test_stream_fallback_before_first_event(settings, failure):
    settings.provider_timeout_seconds = 0.02
    seen = []

    async def provider(request):
        seen.append(request.url.path)
        if request.url.path == "/primary":
            if failure == "429":
                return httpx.Response(429)
            await asyncio.sleep(10)
        return httpx.Response(
            200,
            text='data: {"choices":[]}\n\ndata: [DONE]\n\n',
            headers={"content-type": "text/event-stream"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        events = [event async for event in ProviderRouter(client, settings).stream(BODY)]
    assert events == [{"choices": []}, None]
    assert seen == ["/primary", "/secondary"]


async def test_stream_never_retries_after_first_event(settings):
    seen = []
    closed = []

    class Interrupted(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"choices":[{"delta":{"content":"Hello "}}]}\n\n'
            raise httpx.ReadTimeout("private upstream detail")

        async def aclose(self):
            closed.append(True)

    async def provider(request):
        seen.append(request.url.path)
        return httpx.Response(
            200, stream=Interrupted(), headers={"content-type": "text/event-stream"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        stream = ProviderRouter(client, settings).stream(BODY)
        assert (await anext(stream))["choices"][0]["delta"]["content"] == "Hello "
        with pytest.raises(GatewayError) as caught:
            await anext(stream)
        assert "private" not in json.dumps(caught.value.payload())
    assert seen == ["/primary"]
    assert closed == [True]


async def test_local_budget_rejection_does_not_contact_providers(settings):
    settings.tokens_per_minute = 10

    async def provider(request):
        pytest.fail("Over-budget request reached provider")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
        app = create_app(settings, upstream)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client,
        ):
            response = await client.post(
                "/v1/chat/completions",
                json=BODY.model_dump(),
                headers={"Authorization": "Bearer " + issue_token(settings, "viewer", "t")},
            )
            assert response.status_code == 429
            assert response.headers["retry-after"] == "60"
            assert response.json()["error"]["code"] == "token_limit_exceeded"


async def test_both_provider_errors_are_sanitized(settings):
    async def provider(request):
        return httpx.Response(429, text="SECRET upstream stacktrace")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
        app = create_app(settings, upstream)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client,
        ):
            response = await client.post(
                "/v1/chat/completions",
                json=BODY.model_dump(),
                headers={"Authorization": "Bearer " + issue_token(settings, "viewer", "t")},
            )
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "providers_unavailable"
            assert "SECRET" not in response.text
