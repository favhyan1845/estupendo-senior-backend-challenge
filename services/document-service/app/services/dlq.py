"""Dead Letter Queue with a disk fallback.

Every document that cannot be validated after the retry policy is exhausted,
or that arrives while the circuit is OPEN, is parked in the DLQ with its batch
id, reason and attempt count. Operators can replay the stream later
(``XREAD``/``XCLAIM``) once the external provider is healthy.

If Redis itself is unavailable, the DLQ entry is appended to a JSONL file so
the information is never lost, even during a full broker outage.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anyio
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class DeadLetterStore:
    def __init__(
        self,
        client: redis.Redis | None,
        stream: str = "documents:dlq",
        max_length: int = 1_000_000,
        fallback_path: str | Path = "/data/spool/dead-letter.jsonl",
    ) -> None:
        self._client = client
        self._stream = stream
        self._max_length = max_length
        self._fallback_path = Path(fallback_path)

    async def add(
        self,
        document: dict[str, Any],
        *,
        batch_id: str,
        reason: str,
        attempts: int = 0,
    ) -> str:
        payload = {
            "batch_id": batch_id,
            "reason": reason,
            "attempts": str(attempts),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "document": json.dumps(document, separators=(",", ":"), default=str),
        }

        if self._client is not None:
            try:
                await self._client.xadd(
                    self._stream,
                    payload,
                    maxlen=self._max_length,
                    approximate=True,
                )
                return "redis"
            except Exception:
                logger.exception("DLQ stream unavailable; falling back to disk")

        await anyio.to_thread.run_sync(self._append_fallback_sync, payload)

        return "file"

    async def depth(self) -> int | None:
        if self._client is None:
            return None

        try:
            return int(await self._client.xlen(self._stream))
        except Exception:
            return None

    def _append_fallback_sync(self, payload: dict[str, str]) -> None:
        self._fallback_path.parent.mkdir(parents=True, exist_ok=True)

        with self._fallback_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
