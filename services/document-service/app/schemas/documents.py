"""Pydantic contracts for the documents API."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

MAX_DOCUMENTS_PER_REQUEST = 1000


class DocumentIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_id: str = Field(min_length=1, max_length=64)
    document_type: Literal["invoice", "credit_note", "debit_note"] = "invoice"
    issuer_tax_id: str = Field(min_length=5, max_length=20)
    amount_cents: int = Field(ge=0, le=10**15)
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    issued_at: datetime
    payload_hash: str | None = Field(default=None, max_length=128)


class DocumentBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str | None = Field(default=None, max_length=64)
    documents: list[DocumentIn] = Field(min_length=1, max_length=MAX_DOCUMENTS_PER_REQUEST)


class BatchAccepted(BaseModel):
    batch_id: str
    accepted: int
    status: Literal["accepted"] = "accepted"
    channel: Literal["redis", "spool", "memory"]
    received_at: datetime
    processing_time_ms: float


class ValidationResult(BaseModel):
    document_id: str
    valid: bool
    checked_at: datetime
    detail: str | None = None


class BreakerSnapshot(BaseModel):
    state: Literal["closed", "open", "half_open"]
    consecutive_failures: int
    failure_threshold: int
    opened_at: datetime | None = None
    retry_in_seconds: float = 0.0
    total_failures: int = 0
    total_successes: int = 0
    total_rejected: int = 0


class ServiceStats(BaseModel):
    accepted_batches: int
    accepted_documents: int
    published_to_broker: int
    published_to_spool: int
    validated_documents: int
    rejected_documents: int
    duplicate_documents: int
    dead_lettered_documents: int
    processed_documents: int
    spool_pending_batches: int
    dlq_depth: int | None = None
    queue_depth: int | None = None
    circuit_breaker: BreakerSnapshot
