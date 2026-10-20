"""Typed application settings.

Every knob is environment driven (prefix ``DOCS_``) so the same image runs as
API, worker or mock in docker-compose without code changes.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DOCS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "document-service"
    environment: str = "local"
    log_level: str = "INFO"

    # Payload limits
    max_documents_per_request: int = Field(default=1000, ge=1, le=10_000)

    # Redis / streams
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_enabled: bool = True
    queue_stream: str = "documents:inbox"
    consumer_group: str = "document-workers"
    queue_max_length: int = 500_000
    dlq_stream: str = "documents:dlq"
    dlq_max_length: int = 1_000_000
    publish_timeout_ms: int = 150
    embedded_worker: bool = False

    # External validation service
    external_base_url: str = "http://127.0.0.1:9000"
    external_timeout_seconds: float = Field(default=2.0, gt=0)
    external_max_connections: int = 100
    external_max_keepalive: int = 20

    # Retries (exponential backoff + jitter)
    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=0.1, gt=0)
    retry_max_delay_seconds: float = Field(default=2.0, gt=0)
    retry_jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    retry_multiplier: float = Field(default=2.0, ge=1)

    # Circuit breaker
    circuit_failure_threshold: int = Field(default=5, ge=1)
    circuit_recovery_seconds: float = Field(default=30.0, gt=0)
    circuit_half_open_max_calls: int = Field(default=1, ge=1)

    # Worker
    worker_batch_size: int = Field(default=50, ge=1, le=1000)
    worker_block_ms: int = Field(default=2000, ge=0)
    worker_max_inflight: int = Field(default=100, ge=1)
    worker_consumer_name: str = "worker-1"

    # Durable fallback when Redis is unavailable / full
    spool_path: str = "/data/spool/documents-pending.jsonl"
    spool_flush_interval_seconds: float = Field(default=2.0, gt=0)
    spool_publish_timeout_ms: int = 100

    @property
    def publish_timeout_seconds(self) -> float:
        return self.publish_timeout_ms / 1000

    @property
    def spool_publish_timeout_seconds(self) -> float:
        return self.spool_publish_timeout_ms / 1000


@lru_cache
def get_settings() -> Settings:
    return Settings()
