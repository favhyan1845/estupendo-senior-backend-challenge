from __future__ import annotations

from typing import Any

import pytest

from app.services.circuit_breaker import CircuitBreaker, CircuitOpenError
from app.services.errors import PermanentExternalError, TransientExternalError
from app.services.resilient_validator import ResilientValidator
from app.services.retry import RetryPolicy


class ScriptedClient:
    def __init__(self, outcomes: list[Any]) -> None:
        self._outcomes = outcomes
        self.calls = 0

    async def validate(self, document: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        outcome = self._outcomes.pop(0) if self._outcomes else {"valid": True}

        if isinstance(outcome, BaseException):
            raise outcome

        return outcome


async def no_sleep(_: float) -> None:
    return None


def policy(max_attempts: int = 2) -> RetryPolicy:
    return RetryPolicy(max_attempts=max_attempts, base_delay=0.001, max_delay=0.002, jitter=0)


@pytest.mark.asyncio
async def test_opens_the_circuit_only_after_five_exhausted_calls() -> None:
    client = ScriptedClient([TransientExternalError("down")] * 100)
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
    validator = ResilientValidator(client, breaker, policy(max_attempts=2), sleep=no_sleep)

    for _ in range(5):
        with pytest.raises(TransientExternalError):
            await validator.validate({"document_id": "doc-1"})

    # 5 calls x 2 attempts, then OPEN.
    assert client.calls == 10
    assert breaker.state.value == "open"

    with pytest.raises(CircuitOpenError):
        await validator.validate({"document_id": "doc-1"})

    assert client.calls == 10, "OPEN circuit must fail fast without calling the provider"


@pytest.mark.asyncio
async def test_retries_absorb_isolated_failures_and_keep_the_circuit_closed() -> None:
    client = ScriptedClient(
        [
            TransientExternalError("blip"),
            {"document_id": "doc-1", "valid": True},
        ]
    )
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=30)
    validator = ResilientValidator(client, breaker, policy(max_attempts=3), sleep=no_sleep)

    result = await validator.validate({"document_id": "doc-1"})

    assert result["valid"] is True
    assert client.calls == 2
    assert breaker.state.value == "closed"


@pytest.mark.asyncio
async def test_permanent_errors_do_not_open_the_circuit() -> None:
    client = ScriptedClient([PermanentExternalError("bad document", status_code=400)] * 10)
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
    validator = ResilientValidator(client, breaker, policy(max_attempts=3), sleep=no_sleep)

    for _ in range(10):
        with pytest.raises(PermanentExternalError):
            await validator.validate({"document_id": "doc-1"})

    assert client.calls == 10, "4xx must not be retried"
    assert breaker.state.value == "closed"
    assert (await breaker.snapshot()).total_failures == 0
