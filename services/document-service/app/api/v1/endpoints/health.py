"""Liveness and readiness probes.

Readiness stays 200 while the disk spool can absorb batches: losing Redis
degrades the service but does not make the API unable to accept work.
"""

from __future__ import annotations

from fastapi import APIRouter

from app.api.deps import RuntimeDep

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", summary="Process liveness")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready", summary="Readiness (Redis reachability + spool backlog)")
async def ready(runtime: RuntimeDep) -> dict[str, object]:
    redis_ok: bool | None = None

    if runtime.redis_client is not None:
        try:
            redis_ok = bool(await runtime.redis_client.ping())
        except Exception:
            redis_ok = False

    spool_pending = await runtime.spool.pending_batches()

    return {
        "status": "ok" if redis_ok in (True, None) else "degraded",
        "redis": redis_ok,
        "spool_pending_batches": spool_pending,
        "degraded_mode": redis_ok is False,
    }
