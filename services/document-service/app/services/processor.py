"""Worker-side document processor.

Every stream entry is one document. The processor:
  1. validates it through the resilient validator (breaker + retries);
  2. ACKs the entry on *every* terminal path so a poison document cannot be
     redelivered forever;
  3. routes failures to the DLQ with an explicit reason, which is what makes
     the OPEN circuit a contingency path instead of data loss;
  4. bounds concurrency with a semaphore so a slow provider cannot exhaust
     memory with in-flight coroutines.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.services.circuit_breaker import CircuitOpenError
from app.services.dlq import DeadLetterStore
from app.services.errors import PermanentExternalError, TransientExternalError
from app.services.metrics import Metrics
from app.services.queue import DocumentQueue, QueueMessage
from app.services.resilient_validator import ResilientValidator

logger = logging.getLogger(__name__)


class DocumentProcessor:
    def __init__(
        self,
        *,
        validator: ResilientValidator,
        queue: DocumentQueue,
        dlq: DeadLetterStore,
        metrics: Metrics,
        max_inflight: int = 100,
        max_attempts: int = 3,
    ) -> None:
        self._validator = validator
        self._queue = queue
        self._dlq = dlq
        self._metrics = metrics
        self._max_attempts = max_attempts
        self._semaphore = asyncio.Semaphore(max_inflight)

    async def process(self, message: QueueMessage) -> None:
        try:
            await self._process_document(message.batch_id, message.document)
        finally:
            # ACK even on unexpected errors: the document is already parked in
            # the DLQ (or was a duplicate); redelivery would only add noise.
            await self._queue.ack(message)

        self._metrics.processed_documents += 1

    async def process_many(self, messages: list[QueueMessage]) -> None:
        await asyncio.gather(*(self.process(message) for message in messages))

    async def _process_document(self, batch_id: str, document: dict[str, Any]) -> None:
        if not isinstance(document, dict) or not document.get("document_id"):
            await self._dead_letter(
                batch_id,
                document if isinstance(document, dict) else {"raw": str(document)},
                reason="invalid_payload",
                attempts=0,
            )

            return

        async with self._semaphore:
            try:
                result = await self._validator.validate(document)
            except CircuitOpenError:
                # Contingency path: circuit OPEN, do not call the provider.
                await self._dead_letter(batch_id, document, reason="circuit_open", attempts=0)
            except TransientExternalError as exc:
                await self._dead_letter(
                    batch_id,
                    document,
                    reason=f"transient_exhausted:{exc.status_code or 'timeout'}",
                    attempts=self._max_attempts,
                )
            except PermanentExternalError as exc:
                await self._dead_letter(
                    batch_id,
                    document,
                    reason=f"permanently_rejected:{exc.status_code}",
                    attempts=1,
                )
            except Exception:
                logger.exception("unexpected validation error")
                await self._dead_letter(batch_id, document, reason="unexpected_error", attempts=0)
            else:
                if result.get("valid", True):
                    self._metrics.validated_documents += 1
                else:
                    self._metrics.rejected_documents += 1

    async def _dead_letter(
        self,
        batch_id: str,
        document: dict[str, Any],
        *,
        reason: str,
        attempts: int,
    ) -> None:
        channel = await self._dlq.add(
            document,
            batch_id=batch_id,
            reason=reason,
            attempts=attempts,
        )
        self._metrics.record_dead_letter(channel)

        logger.warning(
            "document dead-lettered",
            extra={
                "batch_id": batch_id,
                "document_id": document.get("document_id"),
                "reason": reason,
                "attempt": attempts,
                "channel": channel,
            },
        )
