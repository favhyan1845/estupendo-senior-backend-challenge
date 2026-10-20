"""FastAPI dependencies."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request

from app.core.config import Settings, get_settings
from app.runtime import Runtime


def get_runtime(request: Request) -> Runtime:
    runtime = getattr(request.app.state, "runtime", None)

    if runtime is None:  # pragma: no cover - misconfiguration guard
        raise RuntimeError("Runtime is not initialised; check the application lifespan")

    return runtime


RuntimeDep = Annotated[Runtime, Depends(get_runtime)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
