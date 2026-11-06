"""Provider integration with bounded responses, SSE framing, and explicit deadlines."""

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
from pydantic import BaseModel, ConfigDict, Field

from .config import Settings
from .errors import GatewayError


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    role: str = Field(pattern=r"^(system|user|assistant)$")
    content: str = Field(min_length=1, max_length=16_000)


class CompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    messages: list[Message] = Field(min_length=1, max_length=32)
    max_tokens: int = Field(default=256, ge=1, le=8192)

    def reservation_tokens(self) -> int:
        # Conservative byte-level upper bound, with chat-template overhead.
        # Exact usage is reconciled from the provider after a successful response.
        return (
            sum(len(m.content.encode("utf-8")) + 32 for m in self.messages) + 32 + self.max_tokens
        )


@dataclass(frozen=True)
class Endpoint:
    url: str
    model: str
    api_key: str


async def sse_events(response: httpx.Response) -> AsyncIterator[dict | None]:
    """Parse SSE across arbitrary byte/UTF-8 boundaries; cap each frame at 64 KiB."""
    line = bytearray()
    data: list[str] = []
    size = 0
    async for chunk in response.aiter_bytes():
        for byte in chunk:
            size += 1
            if size > 65_536:
                raise ValueError("Oversized SSE frame")
            if byte != 10:
                line.append(byte)
                continue
            decoded = line.decode("utf-8").rstrip("\r")
            line.clear()
            if decoded == "":
                if data:
                    body = "\n".join(data)
                    data.clear()
                    if body == "[DONE]":
                        yield None
                        return
                    event = json.loads(body)
                    if not isinstance(event, dict) or "error" in event:
                        raise ValueError("Invalid SSE event")
                    yield event
                size = 0
            elif decoded.startswith("data:"):
                data.append(decoded[5:].removeprefix(" "))
    # EOF without [DONE] is an interrupted response, never a successful completion.
    raise ValueError("Incomplete SSE stream")


def total_usage(payload: dict) -> int | None:
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    value = usage.get("total_tokens")
    return value if type(value) is int and value >= 0 else None


class ProviderRouter:
    def __init__(self, client: httpx.AsyncClient, settings: Settings):
        self.client = client
        self.timeout = settings.provider_timeout_seconds
        self.endpoints = [
            Endpoint(
                settings.primary_url,
                settings.primary_model,
                settings.primary_api_key.get_secret_value(),
            ),
            Endpoint(
                settings.secondary_url,
                settings.secondary_model,
                settings.secondary_api_key.get_secret_value(),
            ),
        ]

    def _request(self, endpoint: Endpoint, body: CompletionRequest, stream: bool) -> httpx.Request:
        payload = body.model_dump() | {"model": endpoint.model, "stream": stream}
        if stream:
            payload["stream_options"] = {"include_usage": True}
        headers = {"Accept": "text/event-stream" if stream else "application/json"}
        if endpoint.api_key:
            headers["Authorization"] = f"Bearer {endpoint.api_key}"
        return self.client.build_request(
            "POST", endpoint.url, json=payload, headers=headers, timeout=self.timeout
        )

    async def complete(self, body: CompletionRequest) -> dict:
        for index, endpoint in enumerate(self.endpoints):
            response = None
            try:
                # Absolute deadline includes headers AND body; no orphaned timeout race task.
                async with asyncio.timeout(self.timeout):
                    response = await self.client.send(
                        self._request(endpoint, body, False), stream=True
                    )
                    if response.status_code == 429:
                        if index == 0:
                            continue
                        raise GatewayError(
                            503, "providers_unavailable", "Providers are unavailable"
                        )
                    if response.status_code != 200:
                        raise GatewayError(502, "upstream_error", "Provider request failed")
                    raw = bytearray()
                    async for chunk in response.aiter_bytes():
                        raw.extend(chunk)
                        if len(raw) > 1_048_576:
                            raise ValueError("Oversized provider response")
                    payload = json.loads(raw)
                    if (
                        not isinstance(payload, dict)
                        or "error" in payload
                        or not isinstance(payload.get("choices"), list)
                        or not payload["choices"]
                    ):
                        raise ValueError("Invalid provider response")
                    # Return only the contract's safe fields, never arbitrary upstream metadata.
                    content = payload["choices"][0]["message"]["content"]
                    if not isinstance(content, str):
                        raise ValueError("Invalid provider content")
                    result = {
                        "model": endpoint.model,
                        "choices": [
                            {"index": 0, "message": {"role": "assistant", "content": content}}
                        ],
                    }
                    usage = total_usage(payload)
                    if usage is not None:
                        result["usage"] = {"total_tokens": usage}
                    return result
            except (TimeoutError, httpx.TimeoutException):
                if index == 1:
                    raise GatewayError(504, "provider_timeout", "Providers timed out") from None
            except httpx.RequestError:
                raise GatewayError(502, "upstream_error", "Provider connection failed") from None
            except (ValueError, KeyError, TypeError, IndexError):
                raise GatewayError(
                    502, "invalid_upstream_response", "Invalid provider response"
                ) from None
            finally:
                if response is not None:
                    await response.aclose()
        raise GatewayError(503, "providers_unavailable", "Providers are unavailable")

    async def stream(self, body: CompletionRequest) -> AsyncIterator[dict | None]:
        for index, endpoint in enumerate(self.endpoints):
            response = None
            started = False
            try:
                async with asyncio.timeout(self.timeout):
                    response = await self.client.send(
                        self._request(endpoint, body, True), stream=True
                    )
                    if response.status_code == 429:
                        if index == 0:
                            continue
                        raise GatewayError(
                            503, "providers_unavailable", "Providers are unavailable"
                        )
                    if response.status_code != 200:
                        raise GatewayError(502, "upstream_error", "Provider request failed")
                    if "text/event-stream" not in response.headers.get("content-type", ""):
                        raise ValueError("Expected SSE")
                    events = sse_events(response)
                    first = await anext(events)
                started = True
                yield first
                async for event in events:
                    yield event
                return
            except (TimeoutError, httpx.TimeoutException):
                if started or index == 1:
                    raise GatewayError(
                        504, "provider_timeout", "Provider stream timed out"
                    ) from None
            except httpx.RequestError:
                raise GatewayError(502, "upstream_error", "Provider connection failed") from None
            except (ValueError, UnicodeError, StopAsyncIteration):
                raise GatewayError(
                    502, "invalid_upstream_response", "Invalid provider stream"
                ) from None
            finally:
                if response is not None:
                    await response.aclose()
        raise GatewayError(503, "providers_unavailable", "Providers are unavailable")
