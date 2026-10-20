from __future__ import annotations

from typing import Any

import pytest

from app.services.circuit_breaker import CircuitBreaker, CircuitOpenError
from app.services.dlq import DeadLetterStore
from app.services.errors import PermanentExternalError, TransientExternalError
from app.services.metrics import Metrics
from app.services.processor import DocumentProcessor
from app.services.queue import InMemoryDocumentQueue, QueueMessage
from app.services.resilient_validator import ResilientValidator
from app.services.retry import RetryPolicy
from tests.conftest import make_document


class ScriptedClient:
    def __init__(self, outcomes: list[Any] | None = None) -> None:
        self._outcomes = outcomes or []
        self.calls = 0

    async def validate(self, document: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        outcome = self._outcomes.pop(0) if self._outcomes else {"valid": True}

        if isinstance(outcome, BaseException):
            raise outcome

        return outcome


async def no_sleep(_: float) -> None:
    return None


def build_processor(
    tmp_path,
    client: ScriptedClient,
    *,
    breaker: CircuitBreaker | None = None,
    max_attempts: int = 2,
) -> tuple[DocumentProcessor, InMemoryDocumentQueue, Metrics]:
    queue = InMemoryDocumentQueue()
    metrics = Metrics()
    dlq = DeadLetterStore(None, fallback_path=tmp_path / "spool" / "dead-letter.jsonl")
    breaker = breaker or CircuitBreaker(failure_threshold=5, recovery_timeout=30)
    validator = ResilientValidator(
        client,
        breaker,
        RetryPolicy(max_attempts=max_attempts, base_delay=0.001, max_delay=0.002, jitter=0),
        sleep=no_sleep,
    )
    processor = DocumentProcessor(
        validator=validator,
        queue=queue,
        dlq=dlq,
        metrics=metrics,
        max_inflight=10,
        max_attempts=max_attempts,
    )

    return processor, queue, metrics


def message(document: dict[str, Any] | None = None) -> QueueMessage:
    return QueueMessage(id="1-0", batch_id="batch-1", document=document or make_document(1))


@pytest.mark.asyncio
async def test_successful_validation_is_acked_and_counted(tmp_path, read_dlq) -> None:
    processor, queue, metrics = build_processor(tmp_path, ScriptedClient())

    await processor.process(message())

    assert metrics.validated_documents == 1
    assert metrics.dead_lettered_documents == 0
    assert len(queue.acked) == 1
    assert read_dlq() == []


@pytest.mark.asyncio
async def test_transient_failures_exhausting_retries_go_to_the_dlq(tmp_path, read_dlq) -> None:
    client = ScriptedClient([TransientExternalError("down")] * 10)
    processor, queue, metrics = build_processor(tmp_path, client, max_attempts=2)

    await processor.process(message())

    entries = read_dlq()
    assert len(entries) == 1
    assert entries[0]["reason"].startswith("transient_exhausted")
    assert entries[0]["attempts"] == "2"
    assert client.calls == 2
    assert len(queue.acked) == 1, "the stream entry must be ACKed to avoid a poison loop"
    assert metrics.dead_lettered_documents == 1


@pytest.mark.asyncio
async def test_open_circuit_diverts_documents_without_calling_the_provider(tmp_path, read_dlq) -> None:
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)

    for _ in range(5):
        await breaker.record_failure()

    client = ScriptedClient()
    processor, queue, metrics = build_processor(tmp_path, client, breaker=breaker)

    await processor.process(message())

    entries = read_dlq()
    assert len(entries) == 1
    assert entries[0]["reason"] == "circuit_open"
    assert client.calls == 0
    assert len(queue.acked) == 1
    assert metrics.dead_lettered_documents == 1


@pytest.mark.asyncio
async def test_permanent_rejections_are_dead_lettered_once(tmp_path, read_dlq) -> None:
    client = ScriptedClient([PermanentExternalError("bad document", status_code=422)])
    processor, _, _ = build_processor(tmp_path, client, max_attempts=3)

    await processor.process(message())

    entries = read_dlq()
    assert len(entries) == 1
    assert entries[0]["reason"] == "permanently_rejected:422"
    assert client.calls == 1


@pytest.mark.asyncio
async def test_invalid_payloads_are_dead_lettered_without_calling_the_provider(tmp_path, read_dlq) -> None:
    client = ScriptedClient()
    processor, _, _ = build_processor(tmp_path, client)

    await processor.process(message({"batch_id": "missing-document-id"}))

    entries = read_dlq()
    assert len(entries) == 1
    assert entries[0]["reason"] == "invalid_payload"
    assert client.calls == 0


@pytest.mark.asyncio
async def test_no_document_is_lost_under_a_thirty_percent_failure_rate(tmp_path, read_dlq) -> None:
    outcomes: list[Any] = []

    for index in range(1, 101):
        outcomes.append(
            TransientExternalError("simulated 30% outage") if index % 10 < 3 else {"valid": True}
        )

    client = ScriptedClient(outcomes)
    processor, queue, metrics = build_processor(tmp_path, client, max_attempts=3)

    messages = [
        QueueMessage(id=f"{index}-0", batch_id="batch-load", document=make_document(index))
        for index in range(1, 101)
    ]

    await processor.process_many(messages)

    assert len(queue.acked) == 100, "every stream entry must be acknowledged"
    assert metrics.validated_documents + metrics.dead_lettered_documents == 100
    assert metrics.dead_lettered_documents > 0
    assert len(read_dlq()) == metrics.dead_lettered_documents
