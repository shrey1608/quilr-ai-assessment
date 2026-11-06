"""HTTP entrypoint for tasks 2–4. Run with uvicorn quilr_gateway.app:create_app --factory."""

import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import StreamingResponse

from .auth import authenticate
from .config import Settings
from .errors import GatewayError
from .limiter import TokenLimiter
from .providers import CompletionRequest, ProviderRouter, total_usage
from .redaction import StreamingRedactor
from .rpc_proxy import proxy

logger = logging.getLogger(__name__)


def encode_sse(payload: dict, event: str | None = None) -> str:
    prefix = f"event: {event}\n" if event else ""
    return prefix + "data: " + json.dumps(payload) + "\n\n"


def create_app(
    settings: Settings | None = None, client: httpx.AsyncClient | None = None
) -> FastAPI:
    settings = settings or Settings()
    limiter = TokenLimiter(settings.database_path, settings.tokens_per_minute)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await limiter.initialize()
        app.state.client = client or httpx.AsyncClient(follow_redirects=False, trust_env=False)
        app.state.router = ProviderRouter(app.state.client, settings)
        yield
        if client is None:
            await app.state.client.aclose()

    app = FastAPI(title="QuilrAI Assessment Gateway", lifespan=lifespan)

    @app.exception_handler(GatewayError)
    async def gateway_error(request: Request, exc: GatewayError):
        return exc.response()

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        return GatewayError(422, "invalid_request", "Invalid completion request").response()

    @app.exception_handler(Exception)
    async def internal_error(request: Request, exc: Exception):
        # Log only exception type, never prompts, bearer tokens, or upstream text.
        logger.error("Gateway failure: %s", type(exc).__name__)
        return GatewayError(500, "internal_error", "Gateway request failed").response()

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/mcp")
    async def mcp(request: Request):
        return await proxy(request, app.state.client, settings)

    async def reserve(request: Request, body: CompletionRequest) -> str:
        identity = authenticate(request.headers.get("authorization"), settings)
        try:
            return await limiter.reserve(identity.tenant, body.reservation_tokens())
        except sqlite3.Error:
            raise GatewayError(
                503, "budget_unavailable", "Token budget service unavailable"
            ) from None

    @app.post("/v1/chat/completions")
    async def completion(request: Request, body: CompletionRequest):
        reservation = await reserve(request, body)
        usage = None
        try:
            result = await app.state.router.complete(body)
            usage = total_usage(result)
            return result
        finally:
            await limiter.settle_safely(reservation, usage)

    @app.post("/v1/chat/completions/stream")
    async def completion_stream(request: Request, body: CompletionRequest):
        reservation = await reserve(request, body)

        async def generate():
            guard = StreamingRedactor()
            usage = None
            source = app.state.router.stream(body)
            try:
                # Includes all stream work, shorter than the SQLite reservation lease.
                async with asyncio.timeout(60):
                    async for event in source:
                        if event is None:
                            tail = guard.finish()
                            if tail:
                                yield encode_sse({"delta": tail})
                            yield "data: [DONE]\n\n"
                            return
                        if total_usage(event) is not None:
                            usage = total_usage(event)
                        choices = event.get("choices", [])
                        if not isinstance(choices, list):
                            raise ValueError("Invalid choices")
                        for choice in choices:
                            # This endpoint supports exactly one text completion.
                            if choice.get("index", 0) != 0:
                                raise ValueError("Multiple choices unsupported")
                            delta = choice.get("delta", {})
                            if not isinstance(delta, dict):
                                raise ValueError("Invalid delta")
                            content = delta.get("content")
                            if content is None:
                                continue
                            if not isinstance(content, str):
                                raise ValueError("Invalid text delta")
                            safe = guard.feed(content)
                            if safe:
                                yield encode_sse({"delta": safe})
            except GatewayError as exc:
                usage = None
                yield encode_sse(exc.payload(), "error")
            except TimeoutError:
                usage = None
                yield encode_sse(
                    GatewayError(504, "stream_timeout", "Stream timed out").payload(), "error"
                )
            except Exception:
                usage = None
                yield encode_sse(
                    GatewayError(502, "stream_error", "Stream interrupted").payload(), "error"
                )
            finally:
                await source.aclose()
                await limiter.settle_safely(reservation, usage)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app
