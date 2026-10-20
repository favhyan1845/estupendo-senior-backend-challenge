from __future__ import annotations

import pytest

from app.services.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState
from tests.conftest import FakeClock


@pytest.mark.asyncio
async def test_opens_after_five_consecutive_failures_and_rejects_calls() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, clock=clock)

    for _ in range(4):
        await breaker.record_failure()

    assert breaker.state is CircuitState.CLOSED

    await breaker.record_failure()
    assert breaker.state is CircuitState.OPEN

    with pytest.raises(CircuitOpenError) as error:
        await breaker.acquire()

    assert error.value.retry_in_seconds == pytest.approx(30.0)

    snapshot = await breaker.snapshot()
    assert snapshot.state == "open"
    assert snapshot.total_rejected == 1


@pytest.mark.asyncio
async def test_stays_open_for_the_whole_recovery_window() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, clock=clock)

    for _ in range(5):
        await breaker.record_failure()

    clock.advance(29.9)

    with pytest.raises(CircuitOpenError):
        await breaker.acquire()


@pytest.mark.asyncio
async def test_half_open_allows_a_single_probe_and_closes_on_success() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, clock=clock)

    for _ in range(5):
        await breaker.record_failure()

    clock.advance(30.1)
    await breaker.acquire()

    assert breaker.state is CircuitState.HALF_OPEN

    with pytest.raises(CircuitOpenError):
        await breaker.acquire()

    await breaker.record_success()

    assert breaker.state is CircuitState.CLOSED
    assert (await breaker.snapshot()).consecutive_failures == 0


@pytest.mark.asyncio
async def test_failed_probe_reopens_the_circuit_for_another_window() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, clock=clock)

    for _ in range(5):
        await breaker.record_failure()

    clock.advance(30.1)
    await breaker.acquire()
    await breaker.record_failure()

    assert breaker.state is CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        await breaker.acquire()

    clock.advance(30.1)

    await breaker.acquire()
    assert breaker.state is CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_success_resets_the_consecutive_failure_counter() -> None:
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, clock=FakeClock())

    for _ in range(4):
        await breaker.record_failure()

    await breaker.record_success()

    for _ in range(4):
        await breaker.record_failure()

    assert breaker.state is CircuitState.CLOSED

    await breaker.record_failure()

    assert breaker.state is CircuitState.OPEN
