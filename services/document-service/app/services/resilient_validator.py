"""Composition of retry + circuit breaker around the external client.

The order is deliberate:
  1. the breaker gates the call (fail fast when OPEN);
  2. retries absorb isolated transient failures;
  3. the breaker records the *final* outcome of the retry sequence, so five
     consecutive exhausted calls (not five individual attempts) open it.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from app.services.circuit_breaker import CircuitBreaker
from app.services.errors import PermanentExternalError, TransientExternalError
from app.services.external_client import ExternalValidationClient
from app.services.retry import RetryPolicy, SleepFn, call_with_retry

logger = logging.getLogger(__name__)


class ResilientValidator:
    def __init__(
        self,
        client: ExternalValidationClient,
        breaker: CircuitBreaker,
        policy: RetryPolicy,
        *,
        sleep: SleepFn = asyncio.sleep,
        on_retry: Callable[[int, float, BaseException], Awaitable[None] | None] | None = None,
    ) -> None:
        self._client = client
        self._breaker = breaker
        self._policy = policy
        self._sleep = sleep
        self._on_retry = on_retry

    async def validate(self, document: dict[str, Any]) -> dict[str, Any]:
        await self._breaker.acquire()

        try:
            result = await call_with_retry(
                lambda: self._client.validate(document),
                self._policy,
                retry_on=(TransientExternalError,),
                sleep=self._sleep,
                on_retry=self._on_retry,
            )
        except TransientExternalError:
            # Retries exhausted: the dependency is considered unhealthy.
            await self._breaker.record_failure()
            raise
        except PermanentExternalError:
            # The dependency answered; the document is the problem.
            await self._breaker.record_success()
            raise
        except BaseException:
            # Cancellation / unexpected programming error: never swallow it and
            # do not mark the dependency as failed.
            raise
        else:
            await self._breaker.record_success()
            return result
