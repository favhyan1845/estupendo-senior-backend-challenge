"""Queue abstractions.

Production uses Redis Streams + consumer groups:
  * every document is its own stream entry, so a malformed/poison document
    cannot block its 999 siblings;
  * XACK gives at-least-once delivery and pending-entry recovery;
  * the producer writes each batch in a single MULTI/EXEC pipeline, which is
    one network round trip and all-or-nothing (no partial batches that later
    get duplicated by the spool replay).

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
    document: dict[str, Any]


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
        pipe = self._client.pipeline(transaction=True)

        for document in documents:
            pipe.xadd(
                self._stream,
                {
                    "batch_id": batch_id,
                    "document": json.dumps(document, separators=(",", ":"), default=str),
                },
                maxlen=self._max_length,
                approximate=True,
            )

        await pipe.execute()

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
                    messages.append(
                        QueueMessage(
                            id=entry_id,
                            batch_id=str(fields.get("batch_id", "")),
                            document=json.loads(fields["document"]),
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
        return int(await self._client.xlen(self._stream))


class InMemoryDocumentQueue:
    """Same contract backed by an ``asyncio.Queue`` (dev/test only)."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[QueueMessage] = asyncio.Queue()
        self._counter = 0
        self.acked: list[QueueMessage] = []

    async def ensure_ready(self) -> None:
        return None

    async def enqueue(self, batch_id: str, documents: list[dict[str, Any]]) -> int:
        for document in documents:
            self._counter += 1
            await self._queue.put(
                QueueMessage(id=f"mem-{self._counter}", batch_id=batch_id, document=document)
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
