<?php

namespace App\Events;

use Carbon\CarbonImmutable;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;

/**
 * Production-safe alternative to per-invoice events: one event per keyset
 * batch, carrying an aggregate payload (count + tax total) and the id range.
 */
class InvoiceBatchProcessed
{
    use Dispatchable, SerializesModels;

    public function __construct(
        public readonly int $batchNumber,
        public readonly int $firstId,
        public readonly int $lastId,
        public readonly int $count,
        public readonly int $taxTotalCents,
        public readonly CarbonImmutable $processedAt,
    ) {
    }
}
