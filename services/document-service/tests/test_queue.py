from __future__ import annotations

import json
from typing import Any

import pytest

from app.services.queue import RedisDocumentQueue


class StubRedis:
    def __init__(self, groups: list[dict[str, Any]] | Exception, length: int = 1000) -> None:
        self._groups = groups
        self._length = length

    async def xinfo_groups(self, stream: str) -> list[dict[str, Any]]:
        if isinstance(self._groups, Exception):
            raise self._groups
        return self._groups

    async def xlen(self, stream: str) -> int:
        return self._length


@pytest.mark.asyncio
async def test_depth_reports_the_consumer_group_lag_instead_of_xlen() -> None:
    redis = StubRedis(
        [
            {
                "name": "document-workers",
                "lag": 0,
                "pending": 0,
                "stream-length": 1000,
                "entries-read": 1000,
            }
        ]
    )
    queue = RedisDocumentQueue(redis, group="document-workers")

    # XLEN would still report 1000 even when everything is processed.
    assert await queue.depth() == 0


@pytest.mark.asyncio
async def test_depth_falls_back_to_the_stream_length_when_xinfo_fails() -> None:
    queue = RedisDocumentQueue(StubRedis(ConnectionError("redis down"), length=42))

    assert await queue.depth() == 42


@pytest.mark.asyncio
async def test_depth_falls_back_when_the_group_does_not_exist_yet() -> None:
    queue = RedisDocumentQueue(StubRedis([{"name": "other-group", "lag": 7}], length=99))

    assert await queue.depth() is None


class XaddStub:
    def __init__(self) -> None:
        self.entries: list[dict[str, str]] = []

    async def xadd(
        self,
        stream: str,
        fields: dict[str, str],
        maxlen: int | None = None,
        approximate: bool = False,
    ) -> str:
        self.entries.append(fields)
        return "1-0"

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int,
        block: int,
    ):
        return []


@pytest.mark.asyncio
async def test_enqueue_writes_one_stream_entry_per_batch() -> None:
    redis = XaddStub()
    queue = RedisDocumentQueue(redis, group="document-workers")
    documents = [{"document_id": f"doc-{i}"} for i in range(1000)]

    accepted = await queue.enqueue("batch-1", documents)

    assert accepted == 1000
    assert len(redis.entries) == 1, "1 XADD per batch keeps the request path fast"
    assert redis.entries[0]["batch_id"] == "batch-1"
    assert len(json.loads(redis.entries[0]["documents"])) == 1000
