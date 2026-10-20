<?php

namespace App\Actions\Invoices;

final readonly class TaxResult
{
    public function __construct(
        public int $subtotalCents,
        public int $taxRateBps,
        public int $taxCents,
        public int $totalCents,
        public string $currency,
    ) {
    }
}
