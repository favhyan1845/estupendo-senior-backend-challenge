"""Application factory.

The runtime is injectable so tests can build the app with in-memory
collaborators without touching Redis or the network.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.endpoints.health import router as health_router
from app.api.v1.router import router as api_router
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.runtime import Runtime, build_runtime
from app.workers.consumer import consume_forever

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None, runtime: Runtime | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owns_runtime = runtime is None
        active_runtime = runtime or await build_runtime(settings)
        app.state.runtime = active_runtime

        stop_event = asyncio.Event()
        app.state.stop_event = stop_event

        tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(active_runtime.replayer.run(stop_event), name="spool-replayer"),
        ]

        if settings.embedded_worker:
            tasks.append(
                asyncio.create_task(
                    consume_forever(active_runtime, stop=stop_event),
                    name="embedded-worker",
                )
            )

        logger.info("document-service started", extra={"channel": "startup"})

        try:
            yield
        finally:
            stop_event.set()

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

            if owns_runtime:
                await active_runtime.close()

            logger.info("document-service stopped", extra={"channel": "shutdown"})

    app = FastAPI(
        title="Document Validation Service",
        description=(
            "Asynchronous document validation gateway: accepts up to 1,000 documents "
            "per request, acknowledges in <200 ms, and validates them in the background "
            "with exponential backoff, a circuit breaker and a dead-letter contingency path."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    # Present before lifespan runs so ASGI transports that skip lifespan (tests)
    # can still resolve dependencies.
    if runtime is not None:
        app.state.runtime = runtime

    app.include_router(health_router)
    app.include_router(api_router)

    return app


app = create_app()
