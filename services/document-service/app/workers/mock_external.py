"""Mock of the unreliable third-party validation provider.

Default failure rate is 30% (503), with 0-40 ms of latency, so the retry and
circuit-breaker behaviour can be observed end to end in docker-compose.

Environment:
    MOCK_FAILURE_RATE  (default 0.3)
    MOCK_LATENCY_MS    (default 25, uniform 0..N)
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, status

app = FastAPI(title="Mock External Validator", version="1.0.0")

FAILURE_RATE = float(os.getenv("MOCK_FAILURE_RATE", "0.3"))
LATENCY_MS = float(os.getenv("MOCK_LATENCY_MS", "25"))


@app.get("/health")
async def health() -> dict[str, object]:
    return {"status": "ok", "failure_rate": FAILURE_RATE}


@app.post("/validate")
async def validate(document: dict) -> dict[str, object]:
    if LATENCY_MS > 0:
        await asyncio.sleep(random.uniform(0, LATENCY_MS) / 1000)

    if random.random() < FAILURE_RATE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="upstream dependency temporarily unavailable",
        )

    return {
        "document_id": document.get("document_id"),
        "valid": True,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "provider": "mock-external",
    }
