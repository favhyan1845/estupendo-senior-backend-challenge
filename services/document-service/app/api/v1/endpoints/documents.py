"""Document ingestion endpoints."""

from __future__ import annotations

import logging
import time

from fastapi import APIRouter, HTTPException, Response, status

from app.api.deps import RuntimeDep, SettingsDep
from app.schemas.documents import BatchAccepted, DocumentBatchRequest, ServiceStats

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post(
    "/process",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BatchAccepted,
    summary="Accept a batch of documents for asynchronous validation",
    responses={
        202: {"description": "Batch durably accepted (Redis or disk spool)"},
        422: {"description": "Payload validation error (1..1000 documents)"},
    },
)
async def process_documents(
    payload: DocumentBatchRequest,
    response: Response,
    runtime: RuntimeDep,
    settings: SettingsDep,
) -> BatchAccepted:
    if len(payload.documents) > settings.max_documents_per_request:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"At most {settings.max_documents_per_request} documents per request are allowed",
        )

    started = time.perf_counter()

    # The only awaited work on the request path: one pipelined write (or one
    # fsync'd spool append). Validation happens in the worker.
    receipt = await runtime.producer.publish(payload.documents, payload.batch_id)

    elapsed_ms = (time.perf_counter() - started) * 1000

    response.headers["X-Batch-Id"] = receipt.batch_id
    response.headers["X-Processing-Time-Ms"] = f"{elapsed_ms:.2f}"
    response.headers["X-Queue-Channel"] = receipt.channel

    logger.info(
        "batch accepted",
        extra={
            "batch_id": receipt.batch_id,
            "channel": receipt.channel,
        },
    )

    return BatchAccepted(
        batch_id=receipt.batch_id,
        accepted=receipt.accepted,
        channel=receipt.channel,
        received_at=receipt.received_at,
        processing_time_ms=round(elapsed_ms, 2),
    )


@router.get("/stats", response_model=ServiceStats, summary="Runtime metrics, queue depth and breaker state")
async def stats(runtime: RuntimeDep) -> ServiceStats:
    breaker = await runtime.breaker.snapshot()

    try:
        queue_depth = await runtime.queue.depth()
    except Exception:
        queue_depth = None

    try:
        dlq_depth = await runtime.dlq.depth()
    except Exception:
        dlq_depth = None

    metrics = runtime.metrics

    return ServiceStats(
        accepted_batches=metrics.accepted_batches,
        accepted_documents=metrics.accepted_documents,
        published_to_broker=metrics.published_to_broker,
        published_to_spool=metrics.published_to_spool,
        validated_documents=metrics.validated_documents,
        rejected_documents=metrics.rejected_documents,
        dead_lettered_documents=metrics.dead_lettered_documents,
        processed_documents=metrics.processed_documents,
        spool_pending_batches=await runtime.spool.pending_batches(),
        dlq_depth=dlq_depth,
        queue_depth=queue_depth,
        circuit_breaker=breaker,
    )
