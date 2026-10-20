<?php

namespace App\Services\Invoices;

/**
 * One keyset page of invoices. Rows stay as associative arrays on purpose:
 * hydrating 100k Eloquent models (or even 100k stdClass objects) is exactly
 * what pushes PHP over the 32 MB ceiling.
 */
final readonly class InvoiceBatch
{
    /**
     * @param  list<array{id: int, number: string, subtotal_cents: int, tax_rate_bps: int, currency: string}>  $rows
     */
    public function __construct(
        public array $rows,
        public int $lastId,
        public bool $hasMore,
        public int $number = 1,
    ) {
    }

    public function count(): int
    {
        return count($this->rows);
    }

    public function firstId(): ?int
    {
        return $this->rows[0]['id'] ?? null;
    }
}
