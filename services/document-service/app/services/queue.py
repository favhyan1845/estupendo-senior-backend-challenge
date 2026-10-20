"""Queue abstractions.

Production uses Redis Streams + consumer groups:
  * one stream entry per **batch** (up to 1,000 documents): the request path
    performs a single XADD, so the ACK unit is the batch; a crash mid-batch
    redelivers it and the per-document idempotency key removes duplicates;
  * XACK gives at-least-once delivery and pending-entry recovery;
  * ``XINFO GROUPS.lag`` is the operational backlog (batches, not documents).

``InMemoryDocumentQueue`` mirrors the same contract for local runs and tests.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol

import redis.asyncio as redis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueueMessage:
    id: str
    batch_id: str
    documents: list[dict[str, Any]]


class DocumentQueue(Protocol):
    async def ensure_ready(self) -> None: ...

    async def enqueue(self, batch_id: str, documents: list[dict[str, Any]]) -> int: ...

    async def read(self, consumer: str, count: int, block_ms: int) -> list[QueueMessage]: ...

    async def ack(self, message: QueueMessage) -> int: ...

    async def depth(self) -> int | None: ...


class RedisDocumentQueue:
    def __init__(
        self,
        client: redis.Redis,
        stream: str = "documents:inbox",
        group: str = "document-workers",
        max_length: int = 500_000,
    ) -> None:
        self._client = client
        self._stream = stream
        self._group = group
        self._max_length = max_length

    async def ensure_ready(self) -> None:
        try:
            await self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def enqueue(self, batch_id: str, documents: list[dict[str, Any]]) -> int:
        if not documents:
            return 0

        # One stream entry per batch: the request does a single XADD (~ms)
        # instead of 1,000, which keeps the 202 path well under 200 ms even
        # when workers are hammering Redis.
        await self._client.xadd(
            self._stream,
            {
                "batch_id": batch_id,
                "documents": json.dumps(documents, separators=(",", ":"), default=str),
            },
            maxlen=self._max_length,
            approximate=True,
        )

        return len(documents)

    async def read(self, consumer: str, count: int, block_ms: int) -> list[QueueMessage]:
        response = await self._client.xreadgroup(
            self._group,
            consumer,
            {self._stream: ">"},
            count=count,
            block=block_ms,
        )

        messages: list[QueueMessage] = []

        for _stream, entries in response or []:
            for entry_id, fields in entries:
                try:
                    batch_id = str(fields.get("batch_id", ""))

                    if "documents" in fields:
                        documents = json.loads(fields["documents"])
                    else:
                        # Backwards compatibility with single-document entries.
                        documents = [json.loads(fields["document"])]

                    if not isinstance(documents, list) or not documents:
                        raise ValueError("empty batch")

                    messages.append(
                        QueueMessage(
                            id=entry_id,
                            batch_id=batch_id,
                            documents=documents,
                        )
                    )
                except (KeyError, ValueError, TypeError):
                    # Poison entry: ack it later so it does not loop forever.
                    logger.error("discarding malformed stream entry", extra={"reason": "malformed_entry"})
                    await self._client.xack(self._stream, self._group, entry_id)

        return messages

    async def ack(self, message: QueueMessage) -> int:
        return int(await self._client.xack(self._stream, self._group, message.id))

    async def depth(self) -> int | None:
        """Backlog of the consumer group (``lag``), not cumulative stream length.

        XLEN keeps growing with acked entries, so it is a poor operational
        metric. ``XINFO GROUPS.lag`` is the number of entries not yet delivered
        to the group, which is what autoscaling/alerts should watch.
        """
        try:
            groups = await self._client.xinfo_groups(self._stream)
        except Exception:
            try:
                return int(await self._client.xlen(self._stream))
            except Exception:
                return None

        for group in groups:
            if group.get("name") != self._group:
                continue

            lag = group.get("lag")
            if lag is not None:
                return int(lag)

            stream_length = int(group.get("stream-length") or await self._client.xlen(self._stream))
            entries_read = group.get("entries-read")

            if entries_read is None:
                return stream_length - int(group.get("pending") or 0)

            return max(0, stream_length - int(entries_read))

        return None


class InMemoryDocumentQueue:
    """Same contract backed by an ``asyncio.Queue`` (dev/test only)."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[QueueMessage] = asyncio.Queue()
        self._counter = 0
        self.acked: list[QueueMessage] = []

    async def ensure_ready(self) -> None:
        return None

    async def enqueue(self, batch_id: str, documents: list[dict[str, Any]]) -> int:
        if not documents:
            return 0

        self._counter += 1
        await self._queue.put(
            QueueMessage(
                id=f"mem-{self._counter}",
                batch_id=batch_id,
                documents=list(documents),
            )
        )

        return len(documents)

    async def read(self, consumer: str, count: int, block_ms: int) -> list[QueueMessage]:
        messages: list[QueueMessage] = []

        while len(messages) < count:
            try:
                timeout = block_ms / 1000 if block_ms else 0.01
                messages.append(await asyncio.wait_for(self._queue.get(), timeout=timeout))
            except asyncio.TimeoutError:
                break

        return messages

    async def ack(self, message: QueueMessage) -> int:
        self.acked.append(message)

        return 1

    async def depth(self) -> int | None:
        return self._queue.qsize()
