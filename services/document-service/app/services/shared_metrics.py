"""Counters shared by API and worker processes.

The API and the worker are separate containers, so in-process counters cannot
describe the whole pipeline. The worker increments Redis hashes and the API
merges them in ``/stats``; if Redis is unavailable the API falls back to its
local counters (and reports queue/DLQ depth as ``null``).
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class SharedMetrics:
    def __init__(
        self,
        client: Any | None,
        key: str = "documents:metrics",
        breaker_key: str = "documents:breaker",
        seen_prefix: str = "documents:seen:",
        dedup_ttl_seconds: int = 86_400,
    ) -> None:
        self._client = client
        self._key = key
        self._breaker_key = breaker_key
        self._seen_prefix = seen_prefix
        self._dedup_ttl_seconds = dedup_ttl_seconds

    async def incr(self, field: str, amount: int = 1) -> None:
        if self._client is None:
            return

        try:
            await self._client.hincrby(self._key, field, amount)
        except Exception:  # observability must never break processing
            logger.debug("could not increment shared metric %s", field, exc_info=True)

    async def snapshot(self) -> dict[str, int]:
        if self._client is None:
            return {}

        try:
            raw = await self._client.hgetall(self._key)
        except Exception:
            logger.debug("could not read shared metrics", exc_info=True)
            return {}

        parsed: dict[str, int] = {}

        for key, value in (raw or {}).items():
            try:
                parsed[str(key)] = int(value)
            except (TypeError, ValueError):
                continue

        return parsed

    async def claim_document(self, document_id: str) -> bool:
        """Idempotency guard: True only for the first delivery of a document.

        Redis Streams + spool replay give at-least-once delivery: a publish
        timeout can leave the batch both in Redis and in the spool. Claiming
        ``document_id`` with SET NX makes the second delivery a cheap no-op.
        Without Redis we fail open (process the document) to preserve
        availability.
        """
        if self._client is None:
            return True

        try:
            return bool(
                await self._client.set(
                    f"{self._seen_prefix}{document_id}",
                    "1",
                    nx=True,
                    ex=self._dedup_ttl_seconds,
                )
            )
        except Exception:
            logger.debug("could not claim document idempotency key", exc_info=True)
            return True

    async def set_breaker(self, payload: dict[str, Any]) -> None:
        if self._client is None:
            return

        try:
            await self._client.set(
                self._breaker_key,
                json.dumps(payload, default=str),
                ex=3600,
            )
        except Exception:
            logger.debug("could not publish breaker snapshot", exc_info=True)

    async def breaker_snapshot(self) -> dict[str, Any] | None:
        if self._client is None:
            return None

        try:
            raw = await self._client.get(self._breaker_key)
        except Exception:
            logger.debug("could not read breaker snapshot", exc_info=True)
            return None

        if not raw:
            return None

        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return None

        return parsed if isinstance(parsed, dict) else None
