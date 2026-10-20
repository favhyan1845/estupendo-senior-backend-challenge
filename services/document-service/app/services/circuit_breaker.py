"""Async circuit breaker (CLOSED -> OPEN -> HALF_OPEN) with injectable clock.

Design notes
------------
* 5 consecutive failures open the circuit for 30 s (configurable).
* While OPEN, callers fail fast with ``CircuitOpenError``; the processor uses
  that signal to divert the document to the contingency path (DLQ) instead of
  hammering a broken dependency.
* After the recovery window, exactly one probe is allowed (HALF_OPEN). Success
  closes the circuit; a failed probe re-opens it for another full window.
* All transitions happen under an ``asyncio.Lock`` so concurrent tasks cannot
  observe or produce inconsistent states. ``clock`` is injected to make the
  time-based behaviour deterministic in tests.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable

from app.schemas.documents import BreakerSnapshot


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    def __init__(self, retry_in_seconds: float) -> None:
        self.retry_in_seconds = max(0.0, retry_in_seconds)
        super().__init__(f"Circuit breaker is open; retrying in {self.retry_in_seconds:.2f}s")


@dataclass
class _State:
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    opened_at_monotonic: float | None = None
    opened_at_wall: datetime | None = None
    half_open_in_flight: int = 0
    total_failures: int = 0
    total_successes: int = 0
    total_rejected: int = 0


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be > 0")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self._clock = clock
        self._wall_clock = wall_clock
        self._lock = asyncio.Lock()
        self._state = _State()

    @property
    def state(self) -> CircuitState:
        return self._state.state

    async def acquire(self) -> None:
        """Reserve permission to call the dependency or fail fast."""
        async with self._lock:
            self._promote_to_half_open_if_due()

            if self._state.state is CircuitState.CLOSED:
                return

            if self._state.state is CircuitState.OPEN:
                self._state.total_rejected += 1
                raise CircuitOpenError(self._retry_in_seconds())

            if self._state.half_open_in_flight >= self.half_open_max_calls:
                self._state.total_rejected += 1
                raise CircuitOpenError(self.recovery_timeout)

            self._state.half_open_in_flight += 1

    async def record_success(self) -> None:
        async with self._lock:
            self._state.total_successes += 1
            self._state.state = CircuitState.CLOSED
            self._state.consecutive_failures = 0
            self._state.opened_at_monotonic = None
            self._state.opened_at_wall = None
            self._state.half_open_in_flight = 0

    async def record_failure(self) -> None:
        async with self._lock:
            self._state.total_failures += 1

            if self._state.state is CircuitState.HALF_OPEN:
                self._trip()
                return

            if self._state.state is CircuitState.OPEN:
                return

            self._state.consecutive_failures += 1
            if self._state.consecutive_failures >= self.failure_threshold:
                self._trip()

    async def snapshot(self) -> BreakerSnapshot:
        async with self._lock:
            self._promote_to_half_open_if_due()
            state = self._state.state

            return BreakerSnapshot(
                state=state.value,
                consecutive_failures=self._state.consecutive_failures,
                failure_threshold=self.failure_threshold,
                opened_at=self._state.opened_at_wall if state is CircuitState.OPEN else None,
                retry_in_seconds=self._retry_in_seconds() if state is CircuitState.OPEN else 0.0,
                total_failures=self._state.total_failures,
                total_successes=self._state.total_successes,
                total_rejected=self._state.total_rejected,
            )

    def _promote_to_half_open_if_due(self) -> None:
        if self._state.state is not CircuitState.OPEN:
            return

        if self._retry_in_seconds() <= 0:
            self._state.state = CircuitState.HALF_OPEN
            self._state.half_open_in_flight = 0

    def _retry_in_seconds(self) -> float:
        if self._state.opened_at_monotonic is None:
            return 0.0

        elapsed = self._clock() - self._state.opened_at_monotonic

        return max(0.0, self.recovery_timeout - elapsed)

    def _trip(self) -> None:
        self._state.state = CircuitState.OPEN
        self._state.opened_at_monotonic = self._clock()
        self._state.opened_at_wall = self._wall_clock()
        self._state.half_open_in_flight = 0
