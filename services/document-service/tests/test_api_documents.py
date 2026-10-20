from __future__ import annotations

import time

import pytest

from tests.conftest import make_document


def batch_payload(count: int) -> dict:
    return {
        "batch_id": "batch-test-001",
        "documents": [make_document(index) for index in range(1, count + 1)],
    }


@pytest.mark.asyncio
async def test_accepts_1000_documents_in_under_200_milliseconds(client, runtime) -> None:
    payload = batch_payload(1000)

    started = time.perf_counter()
    response = await client.post("/api/v1/documents/process", json=payload)
    elapsed_ms = (time.perf_counter() - started) * 1000

    assert response.status_code == 202

    body = response.json()
    assert body["batch_id"] == "batch-test-001"
    assert body["accepted"] == 1000
    assert body["status"] == "accepted"
    assert body["channel"] == "redis"
    assert body["processing_time_ms"] < 200
    assert response.headers["X-Queue-Channel"] == "redis"
    assert elapsed_ms < 200, f"Endpoint took {elapsed_ms:.1f} ms"

    assert await runtime.queue.depth() == 1000


@pytest.mark.asyncio
async def test_rejects_more_than_1000_documents(client, runtime) -> None:
    response = await client.post("/api/v1/documents/process", json=batch_payload(1001))

    assert response.status_code == 422
    assert await runtime.queue.depth() == 0


@pytest.mark.asyncio
async def test_rejects_an_empty_batch(client) -> None:
    response = await client.post("/api/v1/documents/process", json={"documents": []})

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_rejects_invalid_documents(client) -> None:
    invalid = make_document(1)
    invalid["currency"] = "cop"
    invalid["unknown_field"] = True

    response = await client.post(
        "/api/v1/documents/process",
        json={"documents": [invalid]},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_generates_a_batch_id_when_the_client_does_not_send_one(client) -> None:
    response = await client.post(
        "/api/v1/documents/process",
        json={"documents": [make_document(1)]},
    )

    assert response.status_code == 202
    assert len(response.json()["batch_id"]) == 32


@pytest.mark.asyncio
async def test_stats_expose_breaker_state_and_queue_depth(client) -> None:
    await client.post("/api/v1/documents/process", json=batch_payload(3))

    response = await client.get("/api/v1/documents/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["accepted_batches"] == 1
    assert body["accepted_documents"] == 3
    assert body["published_to_redis"] == 3
    assert body["queue_depth"] == 3
    assert body["circuit_breaker"]["state"] == "closed"
    assert body["circuit_breaker"]["failure_threshold"] == 5


@pytest.mark.asyncio
async def test_health_endpoints(client) -> None:
    live = await client.get("/health/live")
    ready = await client.get("/health/ready")

    assert live.status_code == 200
    assert live.json() == {"status": "ok"}

    assert ready.status_code == 200
    body = ready.json()
    assert body["status"] == "ok"
    assert body["degraded_mode"] is False
    assert body["spool_pending_batches"] == 0
