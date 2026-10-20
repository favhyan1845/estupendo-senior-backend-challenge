"""Durable batch producer used by ``POST /documents/process``.

Happy path: one MULTI/EXEC pipeline to Redis Streams, bounded by a short
timeout (~150 ms). The endpoint must answer in <200 ms, so the producer never
waits for the downstream validation - it only guarantees the batch was
*accepted durably*.

Degraded path: if Redis is down/full/slow, the batch is appended to the disk
spool (fsync) and the API still returns 202. The background replayer moves it
to Redis when the broker recovers.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Sequence
from uuid import uuid4

from app.schemas.documents import DocumentIn
from app.services.metrics import Metrics
from app.services.queue import DocumentQueue
from app.services.spool import DiskSpool, SpoolEntry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishReceipt:
    batch_id: str
    accepted: int
    channel: Literal["redis", "spool", "memory"]
    received_at: datetime


class DurableBatchProducer:
    def __init__(
        self,
        queue: DocumentQueue,
        spool: DiskSpool,
        *,
        publish_timeout_seconds: float,
        metrics: Metrics,
        broker_channel: Literal["redis", "memory"] = "redis",
    ) -> None:
        self._queue = queue
        self._spool = spool
        self._timeout = publish_timeout_seconds
        self._metrics = metrics
        self._broker_channel = broker_channel

    async def publish(
        self,
        documents: Sequence[DocumentIn],
        batch_id: str | None = None,
    ) -> PublishReceipt:
        batch_id = batch_id or uuid4().hex
        received_at = datetime.now(timezone.utc)
        payload: list[dict[str, Any]] = [doc.model_dump(mode="json") for doc in documents]

        try:
            await asyncio.wait_for(
                self._queue.enqueue(batch_id, payload),
                timeout=self._timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # Redis down, full (noeviction), or too slow
            logger.warning(
                "queue publish failed, spooling batch to disk: %s",
                exc,
                extra={"batch_id": batch_id, "channel": "spool", "reason": type(exc).__name__},
            )
            await self._spool.append(
                SpoolEntry(
                    batch_id=batch_id,
                    documents=payload,
                    created_at=received_at.isoformat(),
                )
            )
            self._metrics.record_acceptance(len(payload))
            self._metrics.record_publish("spool", len(payload))

            return PublishReceipt(
                batch_id=batch_id,
                accepted=len(payload),
                channel="spool",
                received_at=received_at,
            )

        self._metrics.record_acceptance(len(payload))
        self._metrics.record_publish(self._broker_channel, len(payload))

        return PublishReceipt(
            batch_id=batch_id,
            accepted=len(payload),
            channel=self._broker_channel,
            received_at=received_at,
        )
