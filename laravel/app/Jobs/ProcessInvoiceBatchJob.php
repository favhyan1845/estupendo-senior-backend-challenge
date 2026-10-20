<?php

namespace App\Jobs;

use App\Enums\InvoiceEventMode;
use App\Services\Invoices\InvoiceProcessingService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Throwable;

/**
 * Processes one keyset page, then re-dispatches itself for the next page.
 *
 * Why not one job for 100k rows? Because a single job is a single timeout,
 * a single retry unit and a single poison message. Chaining 200 small jobs
 * gives per-page retries with backoff, bounded execution time and the ability
 * to scale horizontally by adding workers (each page is independent thanks to
 * the `id > afterId AND status = 'pending'` guard).
 */
final class ProcessInvoiceBatchJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    public int $timeout = 120;

    /** @var array<int, int> */
    public array $backoff = [5, 30, 120];

    public bool $failOnTimeout = true;

    public function __construct(
        public readonly int $afterId = 0,
        public readonly int $batchSize = 500,
        public readonly InvoiceEventMode $eventMode = InvoiceEventMode::Batch,
        public readonly int $maxRows = 0,
        public readonly int $batchNumber = 1,
    ) {
    }

    public function handle(InvoiceProcessingService $service): void
    {
        $outcome = $service->processBatch(
            afterId: $this->afterId,
            batchSize: $this->batchSize,
            eventMode: $this->eventMode,
            batchNumber: $this->batchNumber,
        );

        if ($outcome->processed === 0 || ! $outcome->hasMore) {
            return;
        }

        if ($this->maxRows > 0 && $outcome->processed >= $this->maxRows) {
            return;
        }

        self::dispatch(
            afterId: $outcome->lastId,
            batchSize: $this->batchSize,
            eventMode: $this->eventMode,
            maxRows: $this->maxRows === 0 ? 0 : $this->maxRows - $outcome->processed,
            batchNumber: $this->batchNumber + 1,
        )
            ->onConnection((string) config('invoices.connection', 'redis'))
            ->onQueue((string) config('invoices.queue', 'invoices'));
    }

    public function failed(Throwable $exception): void
    {
        logger()->error('Invoice batch job failed', [
            'after_id' => $this->afterId,
            'batch_size' => $this->batchSize,
            'error' => $exception->getMessage(),
        ]);
    }
}
