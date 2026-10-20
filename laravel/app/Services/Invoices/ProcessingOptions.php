<?php

namespace App\Services\Invoices;

use App\Enums\InvoiceEventMode;
use App\Enums\TransactionMode;

final readonly class ProcessingOptions
{
    public function __construct(
        public int $batchSize = 500,
        public int $maxRows = 0,
        public int $afterId = 0,
        public InvoiceEventMode $eventMode = InvoiceEventMode::Batch,
        public TransactionMode $transactionMode = TransactionMode::Chunked,
        public bool $queue = false,
    ) {
    }

    public static function fromConfig(): self
    {
        return new self(
            batchSize: (int) config('invoices.batch_size', 500),
            eventMode: InvoiceEventMode::tryFrom((string) config('invoices.events.mode', 'batch')) ?? InvoiceEventMode::Batch,
            transactionMode: TransactionMode::tryFrom((string) config('invoices.transaction.mode', 'chunked')) ?? TransactionMode::Chunked,
        );
    }

    public function withOverrides(
        ?int $batchSize = null,
        ?int $maxRows = null,
        ?int $afterId = null,
        ?InvoiceEventMode $eventMode = null,
        ?TransactionMode $transactionMode = null,
        ?bool $queue = null,
    ): self {
        return new self(
            batchSize: $batchSize ?? $this->batchSize,
            maxRows: $maxRows ?? $this->maxRows,
            afterId: $afterId ?? $this->afterId,
            eventMode: $eventMode ?? $this->eventMode,
            transactionMode: $transactionMode ?? $this->transactionMode,
            queue: $queue ?? $this->queue,
        );
    }
}
