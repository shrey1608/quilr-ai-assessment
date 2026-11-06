"""SQLite token reservations for a per-tenant sliding window."""

import asyncio
import math
import time
from pathlib import Path
from uuid import uuid4

import aiosqlite

from .errors import GatewayError


class TokenLimiter:
    def __init__(self, path: str, limit: int = 50_000, window: float = 60, clock=time.time):
        self.path, self.limit, self.window, self.clock = path, limit, window, clock

    async def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""CREATE TABLE IF NOT EXISTS token_usage (
                id TEXT PRIMARY KEY, tenant TEXT NOT NULL, tokens INTEGER NOT NULL,
                timestamp REAL NOT NULL, pending INTEGER NOT NULL, lease_until REAL NOT NULL
            )""")
            await db.execute("CREATE INDEX IF NOT EXISTS usage_tenant ON token_usage(tenant)")
            await db.commit()

    async def reserve(self, tenant: str, tokens: int) -> str:
        if tokens <= 0 or tokens > self.limit:
            raise GatewayError(429, "token_limit_exceeded", "Request exceeds token budget", 60)
        async with aiosqlite.connect(self.path, timeout=5) as db:
            # Serialize admission across processes sharing this database.
            await db.execute("BEGIN IMMEDIATE")
            now = self.clock()
            await db.execute(
                "DELETE FROM token_usage WHERE (pending=0 AND timestamp<=?) "
                "OR (pending=1 AND lease_until<=?)",
                (now - self.window, now),
            )
            rows = await (
                await db.execute(
                    "SELECT tokens, timestamp, pending, lease_until "
                    "FROM token_usage WHERE tenant=?",
                    (tenant,),
                )
            ).fetchall()
            used = sum(row[0] for row in rows)
            if used + tokens > self.limit:
                deficit = used + tokens - self.limit
                retry_at = now + self.window
                for count, expiry in sorted(
                    ((r[0], r[3] if r[2] else r[1] + self.window) for r in rows),
                    key=lambda row: row[1],
                ):
                    deficit -= count
                    retry_at = expiry
                    if deficit <= 0:
                        break
                await db.commit()
                raise GatewayError(
                    429,
                    "rate_limit_exceeded",
                    "Tenant token budget exhausted",
                    max(1, math.ceil(retry_at - now)),
                )
            reservation = str(uuid4())
            # All provider operations have a total deadline <120s. A crashed worker's
            # reservation expires after this lease; live requests stay counted at rollover.
            await db.execute(
                "INSERT INTO token_usage VALUES (?, ?, ?, ?, 1, ?)",
                (reservation, tenant, tokens, now, now + 120),
            )
            await db.commit()
            return reservation

    async def settle(self, reservation: str, actual_tokens: int | None) -> None:
        # None retains the full reservation when usage is unknown (errors/disconnects).
        async with aiosqlite.connect(self.path, timeout=5) as db:
            await db.execute(
                "UPDATE token_usage SET tokens=COALESCE(?, tokens), timestamp=?, pending=0 "
                "WHERE id=? AND pending=1",
                (actual_tokens, self.clock(), reservation),
            )
            await db.commit()

    async def settle_safely(self, reservation: str, actual_tokens: int | None) -> None:
        # Complete the bounded DB operation even if the HTTP client disconnects.
        task = asyncio.create_task(self.settle(reservation, actual_tokens))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise
