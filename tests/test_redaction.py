import random

import httpx
import pytest

from quilr_gateway.providers import sse_events
from quilr_gateway.redaction import StreamingRedactor

CASES = [
    ("Email alice.smith+tag@example.co.uk!", "Email [REDACTED]!"),
    ("SSN 123-45-6789.", "SSN [REDACTED]."),
    ("Card 4111 1111 1111 1111.", "Card [REDACTED]."),
    ("Card 4111-1111-1111-1111", "Card [REDACTED]"),
    ("Card 4111111111111111", "Card [REDACTED]"),
    ("Hello 🌍! Normal prose 123 today.", "Hello 🌍! Normal prose 123 today."),
    ("123 user@example.com", "123 [REDACTED]"),
    ("abc123@example.com", "[REDACTED]"),
    ("one@a.com two@b.org", "[REDACTED] [REDACTED]"),
]


@pytest.mark.parametrize("source,expected", CASES)
def test_every_possible_two_chunk_split(source, expected):
    for offset in range(len(source) + 1):
        guard = StreamingRedactor()
        result = guard.feed(source[:offset]) + guard.feed(source[offset:]) + guard.finish()
        assert result == expected, (offset, result)


def test_random_chunks_and_bounded_memory():
    source = " ".join(s for s, _ in CASES)
    expected = " ".join(s for _, s in CASES)
    rng = random.Random(7)
    for _ in range(100):
        guard = StreamingRedactor()
        output = ""
        remaining = source
        while remaining:
            size = rng.randint(1, 17)
            output += guard.feed(remaining[:size])
            remaining = remaining[size:]
            assert len(guard.buffer) <= 320
        assert output + guard.finish() == expected
    guard = StreamingRedactor()
    output = ""
    for _ in range(10_000):
        output += guard.feed("a")
        assert len(guard.buffer) <= 320
    assert output + guard.feed("@example.com ") + guard.finish() == "[REDACTED] "


def test_responsive_safe_prefix_and_partial_candidate():
    guard = StreamingRedactor()
    assert guard.feed("Hello! Contact ali") == "Hello! Contact "
    assert guard.feed("ce@exam") == ""
    assert guard.feed("ple.com ") == "[REDACTED] "


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, data):
        self.data = data

    async def __aiter__(self):
        for byte in self.data:
            yield bytes([byte])


async def test_sse_byte_boundaries_multiline_comments_utf8():
    raw = ': comment\r\ndata: {"text":\r\ndata: "🌍"}\r\n\r\ndata: [DONE]\n\n'.encode()
    response = httpx.Response(200, stream=ByteStream(raw))
    assert [event async for event in sse_events(response)] == [{"text": "🌍"}, None]


@pytest.mark.parametrize("raw", [b"data: " + b"x" * 65536, b"data: {}\n\n", b"data: nope\n\n"])
async def test_invalid_or_unterminated_sse(raw):
    response = httpx.Response(200, content=raw)
    with pytest.raises(ValueError):
        _ = [event async for event in sse_events(response)]
