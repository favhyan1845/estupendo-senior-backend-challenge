<?php

namespace App\Listeners;

use App\Events\InvoiceBatchProcessed;
use App\Events\InvoiceProcessed;

/**
 * Lightweight, side-effect-free listener: it only accumulates counters in
 * memory (no I/O, no DB writes). That keeps per-invoice dispatch cheap enough
 * to be measurable while never retaining per-invoice payloads.
 */
final class CollectInvoiceProcessingMetrics
{
    private int $invoices = 0;

    private int $batches = 0;

    private int $taxCents = 0;

    private ?int $lastInvoiceId = null;

    public function handle(InvoiceProcessed|InvoiceBatchProcessed $event): void
    {
        if ($event instanceof InvoiceProcessed) {
            $this->invoices++;
            $this->taxCents += $event->taxCents;
            $this->lastInvoiceId = $event->invoiceId;

            return;
        }

        $this->batches++;
        $this->invoices += $event->count;
        $this->taxCents += $event->taxTotalCents;
        $this->lastInvoiceId = $event->lastId;
    }

    /**
     * @return array{invoices: int, batches: int, tax_total_cents: int, last_invoice_id: int|null}
     */
    public function snapshot(): array
    {
        return [
            'invoices' => $this->invoices,
            'batches' => $this->batches,
            'tax_total_cents' => $this->taxCents,
            'last_invoice_id' => $this->lastInvoiceId,
        ];
    }

    public function reset(): void
    {
        $this->invoices = 0;
        $this->batches = 0;
        $this->taxCents = 0;
        $this->lastInvoiceId = null;
    }
}
