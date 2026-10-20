<?php

namespace App\Events;

use Carbon\CarbonImmutable;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;

/**
 * Domain event emitted for every processed invoice when
 * invoices.events.mode = per_invoice (literal mode of the exercise).
 *
 * It intentionally carries scalars instead of an Invoice model: serialising
 * 100k models for queued listeners would defeat the memory ceiling.
 */
class InvoiceProcessed
{
    use Dispatchable, SerializesModels;

    public function __construct(
        public readonly int $invoiceId,
        public readonly string $invoiceNumber,
        public readonly int $taxCents,
        public readonly int $totalCents,
        public readonly string $currency,
        public readonly CarbonImmutable $processedAt,
    ) {
    }
}
