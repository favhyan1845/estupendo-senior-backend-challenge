"""Durable disk spool used when Redis is unavailable, full or too slow.

Writes are append-only JSONL + ``fsync``: once ``append`` returns, the batch
survives an API restart or a container reschedule (the file lives on a mounted
volume). A background replayer forwards the spool to Redis as soon as the
broker recovers, then atomically truncates the spool.

The spool is the last line of defence for the "broker is down during a traffic
spike" scenario: the API keeps answering 202 because the batch is already
durable on disk (or on EFS/S3 in the AWS design).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import anyio

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SpoolEntry:
    batch_id: str
    documents: list[dict[str, Any]]
    created_at: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "batch_id": self.batch_id,
                "documents": self.documents,
                "created_at": self.created_at,
            },
            separators=(",", ":"),
            default=str,
        )

    @staticmethod
    def from_json(line: str) -> "SpoolEntry":
        payload = json.loads(line)
        return SpoolEntry(
            batch_id=str(payload["batch_id"]),
            documents=list(payload["documents"]),
            created_at=str(payload.get("created_at") or datetime.now(timezone.utc).isoformat()),
        )


class DiskSpool:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._processing_path = self._path.with_suffix(self._path.suffix + ".processing")
        self._lock = asyncio.Lock()

    @property
    def path(self) -> Path:
        return self._path

    async def append(self, entry: SpoolEntry) -> None:
        async with self._lock:
            await anyio.to_thread.run_sync(self._append_sync, entry)

    async def pending_batches(self) -> int:
        async with self._lock:
            return await anyio.to_thread.run_sync(self._count_sync)

    async def drain(self, forward: Callable[[SpoolEntry], Awaitable[bool]]) -> int:
        """Forward every spooled batch, keeping the ones that still fail."""
        async with self._lock:
            if not self._path.exists():
                return 0

            await anyio.to_thread.run_sync(self._rotate_sync)
            lines = await anyio.to_thread.run_sync(self._read_processing_sync)

            entries: list[SpoolEntry] = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(SpoolEntry.from_json(line))
                except (ValueError, KeyError, TypeError):
                    logger.error("discarding unreadable spool line", extra={"reason": "unreadable_spool"})

            forwarded = 0
            failed: list[SpoolEntry] = []

            try:
                for index, entry in enumerate(entries):
                    try:
                        delivered = await forward(entry)
                    except asyncio.CancelledError:
                        # Put the in-flight entry and everything not yet tried
                        # back into the spool before propagating shutdown.
                        failed.extend(entries[index:])
                        raise
                    except Exception:  # pragma: no cover - defensive
                        logger.exception("spool forwarder crashed")
                        delivered = False

                    if delivered:
                        forwarded += 1
                    else:
                        failed.append(entry)
            finally:
                await anyio.to_thread.run_sync(self._rewrite_failed_sync, failed)

            return forwarded

    # ------------------------------------------------------------------ sync
    def _append_sync(self, entry: SpoolEntry) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)

        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(entry.to_json() + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def _count_sync(self) -> int:
        if not self._path.exists():
            return 0

        with self._path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())

    def _rotate_sync(self) -> None:
        os.replace(self._path, self._processing_path)

    def _read_processing_sync(self) -> list[str]:
        if not self._processing_path.exists():
            return []

        with self._processing_path.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()

        self._processing_path.unlink(missing_ok=True)

        return lines

    def _rewrite_failed_sync(self, failed: list[SpoolEntry]) -> None:
        if not failed:
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp = self._path.with_suffix(self._path.suffix + ".tmp")

        with temp.open("w", encoding="utf-8") as handle:
            for entry in failed:
                handle.write(entry.to_json() + "\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temp, self._path)
