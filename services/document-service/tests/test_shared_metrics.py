from __future__ import annotations

import pytest

from app.services.shared_metrics import SharedMetrics


class StubRedis:
    def __init__(self, fail: bool = False) -> None:
        self.data: dict[str, str] = {}
        self.strings: dict[str, str] = {}
        self.fail = fail

    async def hincrby(self, key: str, field: str, amount: int) -> int:
        if self.fail:
            raise ConnectionError("redis down")
        self.data[field] = str(int(self.data.get(field, "0")) + amount)
        return int(self.data[field])

    async def hgetall(self, key: str) -> dict[str, str]:
        if self.fail:
            raise ConnectionError("redis down")
        return dict(self.data)

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None:
        if self.fail:
            raise ConnectionError("redis down")
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    async def get(self, key: str) -> str | None:
        if self.fail:
            raise ConnectionError("redis down")
        return self.strings.get(key)


@pytest.mark.asyncio
async def test_it_accumulates_and_reads_counters() -> None:
    metrics = SharedMetrics(StubRedis())

    await metrics.incr("validated")
    await metrics.incr("validated", 2)
    await metrics.incr("processed", 3)

    assert await metrics.snapshot() == {"validated": 3, "processed": 3}


@pytest.mark.asyncio
async def test_it_is_a_noop_without_a_redis_client() -> None:
    metrics = SharedMetrics(None)

    await metrics.incr("validated")

    assert await metrics.snapshot() == {}


@pytest.mark.asyncio
async def test_observability_failures_never_break_processing() -> None:
    metrics = SharedMetrics(StubRedis(fail=True))

    await metrics.incr("validated")

    assert await metrics.snapshot() == {}


@pytest.mark.asyncio
async def test_it_shares_the_breaker_snapshot() -> None:
    metrics = SharedMetrics(StubRedis())
    snapshot = {"state": "open", "consecutive_failures": 5, "failure_threshold": 5}

    await metrics.set_breaker(snapshot)

    assert await metrics.breaker_snapshot() == snapshot


@pytest.mark.asyncio
async def test_claim_document_is_idempotent() -> None:
    metrics = SharedMetrics(StubRedis())

    assert await metrics.claim_document("doc-1") is True
    assert await metrics.claim_document("doc-1") is False
    assert await metrics.claim_document("doc-2") is True
