"""Exponential backoff with jitter.

Delays follow ``base * multiplier^(n-1)`` capped at ``max_delay`` and are
full-jittered by a configurable ratio (default ±20%) to avoid synchronised
retry storms across workers ("thundering herd").
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Awaitable, Callable, Sequence, TypeVar

from app.services.errors import TransientExternalError

T = TypeVar("T")

SleepFn = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 2.0
    multiplier: float = 2.0
    jitter: float = 0.2

    def delay_for(self, retry_number: int, rng: random.Random | None = None) -> float:
        """Delay before retry ``retry_number`` (1-based, i.e. after 1st failure)."""
        if retry_number < 1:
            raise ValueError("retry_number must be >= 1")

        delay = min(
            self.base_delay * (self.multiplier ** (retry_number - 1)),
            self.max_delay,
        )

        if self.jitter <= 0:
            return delay

        source = rng or random
        return delay * source.uniform(1 - self.jitter, 1 + self.jitter)


async def call_with_retry(
    operation: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    retry_on: Sequence[type[BaseException]] = (TransientExternalError,),
    sleep: SleepFn = asyncio.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, float, BaseException], Awaitable[None] | None] | None = None,
) -> T:
    """Run ``operation`` retrying transient failures with exponential backoff."""
    last_error: BaseException | None = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await operation()
        except BaseException as exc:  # noqa: BLE001 - re-raised unless retryable
            if not isinstance(exc, tuple(retry_on)):
                raise

            last_error = exc
            if attempt >= policy.max_attempts:
                break

            delay = policy.delay_for(attempt, rng)
            if on_retry is not None:
                result = on_retry(attempt, delay, exc)
                if asyncio.iscoroutine(result):
                    await result
            await sleep(delay)

    assert last_error is not None
    raise last_error
