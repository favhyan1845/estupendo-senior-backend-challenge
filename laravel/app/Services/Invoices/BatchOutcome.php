<?php

namespace App\Services\Invoices;

final readonly class BatchOutcome
{
    public function __construct(
        public int $batchNumber,
        public int $processed,
        public int $taxTotalCents,
        public int $lastId,
        public bool $hasMore,
        public int $affectedRows,
        public float $durationMs,
        public int $peakMemoryBytes,
    ) {
    }

    public static function empty(int $lastId): self
    {
        return new self(
            batchNumber: 0,
            processed: 0,
            taxTotalCents: 0,
            lastId: $lastId,
            hasMore: false,
            affectedRows: 0,
            durationMs: 0.0,
            peakMemoryBytes: memory_get_peak_usage(true),
        );
    }
}
