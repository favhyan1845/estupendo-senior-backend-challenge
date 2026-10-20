"""External-service error taxonomy.

The distinction matters: only *transient* errors are retried and only they
count as circuit-breaker failures. A 4xx caused by a malformed document says
nothing about the health of the remote service, so it must not open the
circuit.
"""


class ExternalServiceError(RuntimeError):
    """Base class for external validation errors."""

    retryable: bool = False

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class TransientExternalError(ExternalServiceError):
    """Timeouts, connection errors, 429 and 5xx responses."""

    retryable = True


class PermanentExternalError(ExternalServiceError):
    """The service answered, but the document/request is invalid (4xx)."""

    retryable = False
