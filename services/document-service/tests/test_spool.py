from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.services.metrics import Metrics
from app.services.producer import DurableBatchProducer
from app.services.queue import InMemoryDocumentQueue
from app.services.replayer import SpoolReplayer
from app.services.spool import DiskSpool, SpoolEntry
from tests.conftest import make_document


class UnavailableQueue:
    """Simulates Redis being down or full (noeviction)."""

    def __init__(self) -> None:
        self.attempts = 0

    async def ensure_ready(self) -> None:
        return None

    async def enqueue(self, batch_id: str, documents: list[dict[str, Any]]) -> int:
        self.attempts += 1
        raise ConnectionError("redis is unavailable")

    async def read(self, consumer: str, count: int, block_ms: int) -> list[Any]:
        return []

    async def ack(self, message: Any) -> int:
        return 0

    async def depth(self) -> int | None:
        return None


@pytest.mark.asyncio
async def test_spool_appends_with_fsync_and_drains(tmp_path: Path) -> None:
    spool = DiskSpool(tmp_path / "spool.jsonl")

    await spool.append(
        SpoolEntry(batch_id="b1", documents=[{"document_id": "d1"}], created_at="2026-10-20T00:00:00Z")
    )
    await spool.append(
        SpoolEntry(batch_id="b2", documents=[{"document_id": "d2"}], created_at="2026-10-20T00:00:01Z")
    )

    assert await spool.pending_batches() == 2

    forwarded: list[SpoolEntry] = []

    async def forward(entry: SpoolEntry) -> bool:
        forwarded.append(entry)
        return True

    assert await spool.drain(forward) == 2
    assert [entry.batch_id for entry in forwarded] == ["b1", "b2"]
    assert await spool.pending_batches() == 0


@pytest.mark.asyncio
async def test_spool_keeps_batches_that_cannot_be_forwarded(tmp_path: Path) -> None:
    spool = DiskSpool(tmp_path / "spool.jsonl")
    await spool.append(
        SpoolEntry(batch_id="b1", documents=[{"document_id": "d1"}], created_at="2026-10-20T00:00:00Z")
    )

    async def forward(_: SpoolEntry) -> bool:
        return False

    assert await spool.drain(forward) == 0
    assert await spool.pending_batches() == 1


@pytest.mark.asyncio
async def test_producer_spools_when_redis_is_unavailable(tmp_path: Path, settings: Settings) -> None:
    queue = UnavailableQueue()
    spool = DiskSpool(tmp_path / "spool.jsonl")
    metrics = Metrics()
    producer = DurableBatchProducer(
        queue,
        spool,
        publish_timeout_seconds=0.05,
        metrics=metrics,
    )

    receipt = await producer.publish(
        [_document(1), _document(2)],
        batch_id="batch-degraded",
    )

    assert receipt.channel == "spool"
    assert receipt.accepted == 2
    assert await spool.pending_batches() == 1
    assert metrics.published_to_spool == 2
    assert metrics.published_to_redis == 0
    assert queue.attempts == 1


@pytest.mark.asyncio
async def test_replayer_moves_spooled_batches_to_redis_once_it_recovers(tmp_path: Path) -> None:
    spool = DiskSpool(tmp_path / "spool.jsonl")
    await spool.append(
        SpoolEntry(batch_id="batch-1", documents=[{"document_id": "d1"}, {"document_id": "d2"}], created_at="now")
    )

    redis_backend = InMemoryDocumentQueue()
    replayer = SpoolReplayer(
        spool,
        redis_backend,
        publish_timeout_seconds=0.05,
        interval_seconds=60,
        metrics=Metrics(),
    )

    assert await replayer.replay_once() == 1
    assert await spool.pending_batches() == 0
    assert await redis_backend.depth() == 2


@pytest.mark.asyncio
async def test_replayer_keeps_entries_when_redis_is_still_down(tmp_path: Path) -> None:
    spool = DiskSpool(tmp_path / "spool.jsonl")
    await spool.append(
        SpoolEntry(batch_id="batch-1", documents=[{"document_id": "d1"}], created_at="now")
    )

    replayer = SpoolReplayer(
        spool,
        UnavailableQueue(),
        publish_timeout_seconds=0.05,
        interval_seconds=60,
        metrics=Metrics(),
    )

    assert await replayer.replay_once() == 0
    assert await spool.pending_batches() == 1


def _document(index: int):
    from app.schemas.documents import DocumentIn

    return DocumentIn(**make_document(index))
