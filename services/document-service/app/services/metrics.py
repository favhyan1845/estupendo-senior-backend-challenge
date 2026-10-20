"""In-process counters exposed through ``GET /api/v1/documents/stats``.

Deliberately dependency-free: counters are plain ints updated by the event
loop, so observability never becomes a bottleneck in the hot path. In AWS the
same numbers are emitted as EMF/CloudWatch metrics.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Metrics:
    accepted_batches: int = 0
    accepted_documents: int = 0
    published_to_redis: int = 0
    published_to_spool: int = 0
    replayed_batches: int = 0
    processed_batches: int = 0
    validated_documents: int = 0
    rejected_documents: int = 0
    dead_lettered_documents: int = 0
    retries: int = 0
    dead_letter_channels: dict[str, int] = field(default_factory=dict)

    def record_acceptance(self, documents: int) -> None:
        self.accepted_batches += 1
        self.accepted_documents += documents

    def record_publish(self, channel: str, documents: int) -> None:
        if channel == "redis":
            self.published_to_redis += documents
        else:
            self.published_to_spool += documents

    def record_dead_letter(self, channel: str) -> None:
        self.dead_lettered_documents += 1
        self.dead_letter_channels[channel] = self.dead_letter_channels.get(channel, 0) + 1
