"""Background consumer for Redis Streams.

Run as a separate process/container:
    python -m app.workers.consumer

Scaling model: add replicas -> XREADGROUP distributes entries across the
consumer group. A crashed consumer leaves entries in the pending list (PEL),
which can be reclaimed with XAUTOCLAIM/XCLAIM.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import socket

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.runtime import Runtime, build_runtime

logger = logging.getLogger(__name__)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


async def consume_forever(
    runtime: Runtime,
    *,
    stop: asyncio.Event | None = None,
    consumer_name: str | None = None,
) -> None:
    settings = runtime.settings
    stop = stop or asyncio.Event()
    consumer = consumer_name or f"{settings.worker_consumer_name}-{socket.gethostname()}"

    await runtime.queue.ensure_ready()
    logger.info("consumer %s listening on %s", consumer, settings.queue_stream)

    while not stop.is_set():
        try:
            messages = await runtime.queue.read(
                consumer,
                count=settings.worker_batch_size,
                block_ms=settings.worker_block_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("queue read failed; backing off")
            await _sleep_or_stop(stop, 1.0)
            continue

        if not messages:
            continue

        try:
            await runtime.processor.process_many(messages)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("document batch processing failed")


async def _main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    runtime = await build_runtime(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    for signal_number in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signal_number, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows
            pass

    try:
        await consume_forever(runtime, stop=stop)
    finally:
        await runtime.close()
        logger.info("worker stopped")


if __name__ == "__main__":
    asyncio.run(_main())
