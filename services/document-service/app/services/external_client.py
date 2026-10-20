"""HTTP adapter for the third-party document validation API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.services.errors import ExternalServiceError, PermanentExternalError, TransientExternalError

logger = logging.getLogger(__name__)


class ExternalValidationClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 2.0,
        client: httpx.AsyncClient | None = None,
        max_connections: int = 100,
        max_keepalive: int = 20,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive,
            ),
            headers={"User-Agent": "document-service/1.0"},
        )
        self._validate_path = "/validate"
        self.calls = 0
        self.transient_errors = 0
        self.permanent_errors = 0

    async def validate(self, document: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1

        try:
            response = await self._client.post(self._validate_path, json=document)
        except httpx.TimeoutException as exc:
            self.transient_errors += 1
            raise TransientExternalError(f"External validation timed out: {exc}") from exc
        except httpx.TransportError as exc:
            self.transient_errors += 1
            raise TransientExternalError(f"External validation unreachable: {exc}") from exc

        if response.status_code == 429 or response.status_code >= 500:
            self.transient_errors += 1
            raise TransientExternalError(
                f"External validation returned {response.status_code}",
                status_code=response.status_code,
            )

        if response.status_code >= 400:
            self.permanent_errors += 1
            raise PermanentExternalError(
                f"External validation rejected the document with {response.status_code}",
                status_code=response.status_code,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            self.transient_errors += 1
            raise TransientExternalError("External validation returned invalid JSON") from exc

        if not isinstance(payload, dict):
            raise TransientExternalError("External validation returned an unexpected payload")

        return payload

    async def ping(self) -> bool:
        try:
            response = await self._client.get("/health")
            return response.status_code < 500
        except httpx.HTTPError:
            return False

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @staticmethod
    def is_transient(error: BaseException) -> bool:
        return isinstance(error, ExternalServiceError) and error.retryable
