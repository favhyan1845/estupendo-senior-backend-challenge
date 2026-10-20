"""Composition root: builds every collaborator once per process."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import redis.asyncio as redis

from app.core.config import Settings
from app.services.circuit_breaker import CircuitBreaker
from app.services.dlq import DeadLetterStore
from app.services.external_client import ExternalValidationClient
from app.services.metrics import Metrics
from app.services.processor import DocumentProcessor
from app.services.producer import DurableBatchProducer
from app.services.queue import DocumentQueue, InMemoryDocumentQueue, RedisDocumentQueue
from app.services.replayer import SpoolReplayer
from app.services.resilient_validator import ResilientValidator
from app.services.retry import RetryPolicy
from app.services.spool import DiskSpool

logger = logging.getLogger(__name__)


@dataclass
class Runtime:
    settings: Settings
    queue: DocumentQueue
    spool: DiskSpool
    dlq: DeadLetterStore
    breaker: CircuitBreaker
    metrics: Metrics
    producer: DurableBatchProducer
    replayer: SpoolReplayer
    validator: ResilientValidator
    processor: DocumentProcessor
    external_client: ExternalValidationClient
    redis_client: redis.Redis | None = None

    async def close(self) -> None:
        await self.external_client.aclose()

        if self.redis_client is not None:
            await self.redis_client.aclose()


async def build_runtime(settings: Settings) -> Runtime:
    redis_client: redis.Redis | None = None
    queue: DocumentQueue

    if settings.redis_enabled:
        redis_client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=0.3,
            socket_timeout=0.5,
            health_check_interval=30,
        )
        queue = RedisDocumentQueue(
            redis_client,
            stream=settings.queue_stream,
            group=settings.consumer_group,
            max_length=settings.queue_max_length,
        )

        try:
            await queue.ensure_ready()
        except Exception:
            # The broker may be down at boot. The API must still start: every
            # publish will fail over to the disk spool, and the replayer will
            # create the consumer group as soon as Redis is reachable again.
            logger.exception("could not initialise the Redis consumer group at startup")
    else:
        queue = InMemoryDocumentQueue()

    external_client = ExternalValidationClient(
        base_url=settings.external_base_url,
        timeout=settings.external_timeout_seconds,
        max_connections=settings.external_max_connections,
        max_keepalive=settings.external_max_keepalive,
    )

    breaker = CircuitBreaker(
        failure_threshold=settings.circuit_failure_threshold,
        recovery_timeout=settings.circuit_recovery_seconds,
        half_open_max_calls=settings.circuit_half_open_max_calls,
    )

    retry_policy = RetryPolicy(
        max_attempts=settings.retry_max_attempts,
        base_delay=settings.retry_base_delay_seconds,
        max_delay=settings.retry_max_delay_seconds,
        multiplier=settings.retry_multiplier,
        jitter=settings.retry_jitter_ratio,
    )

    metrics = Metrics()

    validator = ResilientValidator(external_client, breaker, retry_policy)

    spool = DiskSpool(settings.spool_path)

    dlq = DeadLetterStore(
        redis_client,
        stream=settings.dlq_stream,
        max_length=settings.dlq_max_length,
        fallback_path=settings.spool_path.replace(".jsonl", "-dead-letter.jsonl"),
    )

    producer = DurableBatchProducer(
        queue,
        spool,
        publish_timeout_seconds=settings.publish_timeout_seconds,
        metrics=metrics,
        broker_channel="redis" if settings.redis_enabled else "memory",
    )

    replayer = SpoolReplayer(
        spool,
        queue,
        publish_timeout_seconds=settings.spool_publish_timeout_seconds,
        interval_seconds=settings.spool_flush_interval_seconds,
        metrics=metrics,
    )

    processor = DocumentProcessor(
        validator=validator,
        queue=queue,
        dlq=dlq,
        metrics=metrics,
        max_inflight=settings.worker_max_inflight,
        max_attempts=settings.retry_max_attempts,
    )

    return Runtime(
        settings=settings,
        queue=queue,
        spool=spool,
        dlq=dlq,
        breaker=breaker,
        metrics=metrics,
        producer=producer,
        replayer=replayer,
        validator=validator,
        processor=processor,
        external_client=external_client,
        redis_client=redis_client,
    )
