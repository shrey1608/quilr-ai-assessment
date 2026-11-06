import asyncio

import aiosqlite
import pytest

from quilr_gateway.errors import GatewayError
from quilr_gateway.limiter import TokenLimiter


async def test_atomic_concurrent_reservations_across_instances(tmp_path):
    path = str(tmp_path / "rate.sqlite3")
    first, second = TokenLimiter(path, 100), TokenLimiter(path, 100)
    await first.initialize()

    async def attempt(index):
        try:
            return await (first if index % 2 else second).reserve("tenant", 10)
        except GatewayError as exc:
            assert exc.status == 429
            return None

    results = await asyncio.gather(*(attempt(i) for i in range(30)))
    assert len([r for r in results if r]) == 10
    assert await first.reserve("other-tenant", 100)


async def test_reconcile_restart_and_exact_sliding_boundary(tmp_path):
    now = [100.0]
    path = str(tmp_path / "rate.sqlite3")
    limiter = TokenLimiter(path, 100, clock=lambda: now[0])
    await limiter.initialize()
    reservation = await limiter.reserve("t", 100)
    now[0] = 102
    await limiter.settle(reservation, 40)
    reopened = TokenLimiter(path, 100, clock=lambda: now[0])
    await reopened.initialize()
    await reopened.reserve("t", 60)
    with pytest.raises(GatewayError):
        await reopened.reserve("t", 1)
    now[0] = 162
    assert await reopened.reserve("t", 40)  # settled 40 expires, pending 60 stays counted
    with pytest.raises(GatewayError):
        await reopened.reserve("t", 1)


async def test_crash_lease_and_unknown_usage(tmp_path):
    now = [0.0]
    limiter = TokenLimiter(str(tmp_path / "rate.sqlite3"), 100, clock=lambda: now[0])
    await limiter.initialize()
    reservation = await limiter.reserve("t", 100)
    now[0] = 60
    with pytest.raises(GatewayError):
        await limiter.reserve("t", 1)
    await limiter.settle(reservation, None)
    now[0] = 119.99
    with pytest.raises(GatewayError):
        await limiter.reserve("t", 1)
    now[0] = 120
    await limiter.reserve("t", 100)
    now[0] = 240
    assert await limiter.reserve("t", 100)


async def test_cancellation_and_idempotent_settlement(tmp_path):
    limiter = TokenLimiter(str(tmp_path / "rate.sqlite3"), 100)
    await limiter.initialize()
    reservation = await limiter.reserve("t", 100)
    await limiter.settle_safely(reservation, 20)
    await limiter.settle_safely(reservation, 0)
    async with aiosqlite.connect(limiter.path) as db:
        row = await (await db.execute("SELECT tokens, pending FROM token_usage")).fetchone()
        assert row == (20, 0)
