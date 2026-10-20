"""Background task that drains the disk spool back into Redis."""

from __future__ import annotations

import asyncio
import logging

from app.services.metrics import Metrics
from app.services.queue import DocumentQueue
from app.services.spool import DiskSpool, SpoolEntry

logger = logging.getLogger(__name__)


class SpoolReplayer:
    def __init__(
        self,
        spool: DiskSpool,
        queue: DocumentQueue,
        *,
        publish_timeout_seconds: float,
        interval_seconds: float,
        metrics: Metrics,
    ) -> None:
        self._spool = spool
        self._queue = queue
        self._timeout = publish_timeout_seconds
        self._interval = interval_seconds
        self._metrics = metrics

    async def replay_once(self) -> int:
        async def forward(entry: SpoolEntry) -> bool:
            try:
                await asyncio.wait_for(
                    self._queue.enqueue(entry.batch_id, entry.documents),
                    timeout=self._timeout,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("spool replay failed, keeping batch for next round: %s", exc)
                return False

            self._metrics.replayed_batches += 1

            return True

        forwarded = await self._spool.drain(forward)

        if forwarded:
            logger.info("spool replay forwarded %d batches", forwarded)

        return forwarded

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()

        while not stop.is_set():
            try:
                await self.replay_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("spool replayer iteration failed")

            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                continue
