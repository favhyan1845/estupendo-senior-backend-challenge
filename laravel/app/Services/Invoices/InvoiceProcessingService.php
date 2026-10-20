<?php

namespace App\Services\Invoices;

use App\Actions\Invoices\CalculateInvoiceTax;
use App\Actions\Invoices\PersistProcessedInvoices;
use App\Enums\InvoiceEventMode;
use App\Enums\InvoiceStatus;
use App\Enums\TransactionMode;
use App\Events\InvoiceBatchProcessed;
use App\Events\InvoiceProcessed;
use App\Support\MemoryGuard;
use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\DB;

/**
 * Orchestrates the 100k invoice run.
 *
 * Contract:
 *  - never loads more than `batchSize` rows at once (keyset pagination);
 *  - never hydrates Eloquent models;
 *  - writes each page with one bulk UPDATE;
 *  - fires domain events (per batch by default, per invoice on demand);
 *  - enforces the 32 MB ceiling after every page.
 *
 * The "single transaction" mode required by the statement is supported for
 * comparison (`--transaction=single`), but production defaults to short
 * per-batch transactions: a 100k-row transaction holds undo logs, gap locks
 * and a snapshot for the whole run, which is precisely what takes a primary
 * down under load.
 */
final class InvoiceProcessingService
{
    public function __construct(
        private readonly KeysetInvoiceReader $reader,
        private readonly CalculateInvoiceTax $calculateTax,
        private readonly PersistProcessedInvoices $persist,
        private readonly MemoryGuard $memoryGuard,
    ) {
    }

    public function process(ProcessingOptions $options): ProcessingReport
    {
        $report = ProcessingReport::start($options);

        $run = function () use ($options, $report): void {
            $afterId = $options->afterId;
            $processed = 0;
            $batchNumber = 0;

            while ($options->maxRows === 0 || $processed < $options->maxRows) {
                $batchSize = $options->maxRows === 0
                    ? $options->batchSize
                    : min($options->batchSize, $options->maxRows - $processed);

                $outcome = $this->processBatch(
                    afterId: $afterId,
                    batchSize: $batchSize,
                    eventMode: $options->eventMode,
                    batchNumber: ++$batchNumber,
                    wrapInTransaction: false,
                );

                $report->record($outcome);

                if ($outcome->processed === 0 || ! $outcome->hasMore) {
                    break;
                }

                $afterId = $outcome->lastId;
                $processed += $outcome->processed;
            }
        };

        if ($options->transactionMode === TransactionMode::Single) {
            DB::transaction($run);
        } else {
            $run();
        }

        $this->memoryGuard->assertWithinLimit('end of run');

        return $report->finish($this->memoryGuard);
    }

    /**
     * Processes exactly one keyset page. This is the unit of work used both by
     * the synchronous loop and by ProcessInvoiceBatchJob (queued fan-out).
     */
    public function processBatch(
        int $afterId,
        int $batchSize,
        InvoiceEventMode $eventMode = InvoiceEventMode::Batch,
        int $batchNumber = 1,
        bool $wrapInTransaction = true,
    ): BatchOutcome {
        $startedAt = hrtime(true);
        $batch = $this->reader->fetchBatch($afterId, $batchSize, $batchNumber);

        if ($batch === null) {
            return BatchOutcome::empty($afterId);
        }

        $computed = [];
        $taxTotalCents = 0;

        foreach ($batch->rows as $row) {
            $tax = ($this->calculateTax)($row['subtotal_cents'], $row['tax_rate_bps'], $row['currency']);
            $taxTotalCents += $tax->taxCents;

            $computed[] = [
                'id' => $row['id'],
                'number' => $row['number'],
                'tax_cents' => $tax->taxCents,
                'total_cents' => $tax->totalCents,
                'currency' => $tax->currency,
            ];
        }

        $processedAt = CarbonImmutable::now();

        $write = fn (): int => ($this->persist)($computed, $processedAt, InvoiceStatus::Pending);

        $affectedRows = $wrapInTransaction ? DB::transaction($write) : $write();

        $this->dispatchEvents($computed, $batch, $taxTotalCents, $processedAt, $eventMode);

        $this->memoryGuard->assertWithinLimit("batch #{$batch->number}");

        // Long-running loops: closures passed to DB::transaction()/event() capture
        // this page's row arrays and become GC candidates. PHP only collects them
        // when the root buffer fills, so without this call the retained pages grow
        // until the 32 MB ceiling is hit. Collecting per page is O(1) and keeps
        // usage flat for any number of invoices.
        gc_collect_cycles();

        return new BatchOutcome(
            batchNumber: $batch->number,
            processed: count($computed),
            taxTotalCents: $taxTotalCents,
            lastId: $batch->lastId,
            hasMore: $batch->hasMore,
            affectedRows: $affectedRows,
            durationMs: round((hrtime(true) - $startedAt) / 1_000_000, 2),
            peakMemoryBytes: $this->memoryGuard->peakBytes(),
        );
    }

    /**
     * @param  list<array{id: int, number: string, tax_cents: int, total_cents: int, currency: string}>  $computed
     */
    private function dispatchEvents(
        array $computed,
        InvoiceBatch $batch,
        int $taxTotalCents,
        CarbonImmutable $processedAt,
        InvoiceEventMode $mode,
    ): void {
        if ($mode === InvoiceEventMode::PerInvoice) {
            foreach ($computed as $invoice) {
                event(new InvoiceProcessed(
                    invoiceId: $invoice['id'],
                    invoiceNumber: $invoice['number'],
                    taxCents: $invoice['tax_cents'],
                    totalCents: $invoice['total_cents'],
                    currency: $invoice['currency'],
                    processedAt: $processedAt,
                ));
            }

            return;
        }

        event(new InvoiceBatchProcessed(
            batchNumber: $batch->number,
            firstId: $batch->firstId() ?? 0,
            lastId: $batch->lastId,
            count: count($computed),
            taxTotalCents: $taxTotalCents,
            processedAt: $processedAt,
        ));
    }
}
