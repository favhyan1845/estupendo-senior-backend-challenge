from __future__ import annotations

import random

import pytest

from app.services.errors import PermanentExternalError, TransientExternalError
from app.services.retry import RetryPolicy, call_with_retry


@pytest.mark.asyncio
async def test_retries_with_exponential_backoff_until_success() -> None:
    delays: list[float] = []
    attempts = {"count": 0}

    async def operation() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise TransientExternalError("boom")
        return "ok"

    async def sleep(delay: float) -> None:
        delays.append(delay)

    result = await call_with_retry(
        operation,
        RetryPolicy(max_attempts=3, base_delay=0.1, max_delay=2, multiplier=2, jitter=0),
        sleep=sleep,
    )

    assert result == "ok"
    assert attempts["count"] == 3
    assert delays == [pytest.approx(0.1), pytest.approx(0.2)]


@pytest.mark.asyncio
async def test_raises_the_last_error_after_exhausting_attempts() -> None:
    attempts = {"count": 0}

    async def operation() -> None:
        attempts["count"] += 1
        raise TransientExternalError("still down")

    async def sleep(_: float) -> None:
        return None

    with pytest.raises(TransientExternalError):
        await call_with_retry(
            operation,
            RetryPolicy(max_attempts=4, base_delay=0.01, max_delay=1, jitter=0),
            sleep=sleep,
        )

    assert attempts["count"] == 4


@pytest.mark.asyncio
async def test_does_not_retry_permanent_errors() -> None:
    attempts = {"count": 0}

    async def operation() -> None:
        attempts["count"] += 1
        raise PermanentExternalError("invalid document", status_code=400)

    async def sleep(_: float) -> None:
        return None

    with pytest.raises(PermanentExternalError):
        await call_with_retry(
            operation,
            RetryPolicy(max_attempts=5, base_delay=0.01, max_delay=1, jitter=0),
            sleep=sleep,
        )

    assert attempts["count"] == 1


def test_delays_are_capped_and_jittered() -> None:
    policy = RetryPolicy(base_delay=1, max_delay=2, multiplier=2, jitter=0)
    assert policy.delay_for(1) == 1
    assert policy.delay_for(2) == 2
    assert policy.delay_for(10) == 2

    jittered = RetryPolicy(base_delay=1, max_delay=10, multiplier=2, jitter=0.2)
    rng = random.Random(42)
    delay = jittered.delay_for(1, rng=rng)

    assert 0.8 <= delay <= 1.2
