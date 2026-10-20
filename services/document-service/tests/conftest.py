from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.main import create_app
from app.runtime import Runtime
from app.services.circuit_breaker import CircuitBreaker
from app.services.dlq import DeadLetterStore
from app.services.metrics import Metrics
from app.services.producer import DurableBatchProducer
from app.services.queue import InMemoryDocumentQueue
from app.services.replayer import SpoolReplayer
from app.services.spool import DiskSpool


class FakeClock:
    """Deterministic monotonic clock for breaker tests."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class StubValidator:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.calls = 0
        self._result = result or {"valid": True}

    async def validate(self, document: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        return {"document_id": document.get("document_id"), **self._result}


class StubProcessor:
    def __init__(self) -> None:
        self.messages: list[Any] = []

    async def process_many(self, messages: list[Any]) -> None:
        self.messages.extend(messages)


class StubExternalClient:
    async def aclose(self) -> None:
        return None


def build_runtime(
    settings: Settings,
    *,
    validator: Any | None = None,
    processor: Any | None = None,
    breaker: CircuitBreaker | None = None,
) -> Runtime:
    queue = InMemoryDocumentQueue()
    spool = DiskSpool(settings.spool_path)
    metrics = Metrics()
    dlq = DeadLetterStore(
        None,
        fallback_path=str(Path(settings.spool_path).with_name("dead-letter.jsonl")),
    )
    producer = DurableBatchProducer(
        queue,
        spool,
        publish_timeout_seconds=settings.publish_timeout_seconds,
        metrics=metrics,
        broker_channel="memory",
    )
    replayer = SpoolReplayer(
        spool,
        queue,
        publish_timeout_seconds=settings.spool_publish_timeout_seconds,
        interval_seconds=settings.spool_flush_interval_seconds,
        metrics=metrics,
    )

    return Runtime(
        settings=settings,
        queue=queue,
        spool=spool,
        dlq=dlq,
        breaker=breaker or CircuitBreaker(),
        metrics=metrics,
        producer=producer,
        replayer=replayer,
        validator=validator or StubValidator(),
        processor=processor or StubProcessor(),
        external_client=StubExternalClient(),
    )


def make_document(index: int = 1) -> dict[str, Any]:
    return {
        "document_id": f"doc-{index:05d}",
        "document_type": "invoice",
        "issuer_tax_id": "900123456-7",
        "amount_cents": 125_000 + index,
        "currency": "COP",
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "payload_hash": f"hash-{index}",
    }


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        redis_enabled=False,
        spool_path=str(tmp_path / "spool" / "pending.jsonl"),
        external_base_url="http://external.test",
        retry_base_delay_seconds=0.001,
        retry_max_delay_seconds=0.002,
        retry_jitter_ratio=0.0,
        worker_block_ms=5,
    )


@pytest.fixture
def runtime(settings: Settings) -> Runtime:
    return build_runtime(settings)


@pytest.fixture
def app(settings: Settings, runtime: Runtime):
    return create_app(settings=settings, runtime=runtime)


@pytest.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        yield http


@pytest.fixture
def read_dlq(tmp_path: Path):
    def _read() -> list[dict[str, Any]]:
        path = tmp_path / "spool" / "dead-letter.jsonl"

        if not path.exists():
            return []

        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    return _read
