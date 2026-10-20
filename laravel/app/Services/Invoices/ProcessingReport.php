<?php

namespace App\Services\Invoices;

use App\Support\MemoryGuard;
use Carbon\CarbonImmutable;

/**
 * Mutable accumulator for a single run. Kept outside the hot loop as a plain
 * object (no arrays of per-invoice results) so reporting costs O(1) memory.
 */
final class ProcessingReport
{
    private int $processed = 0;

    private int $batches = 0;

    private int $taxCents = 0;

    private int $affectedRows = 0;

    private int $lastId = 0;

    private float $finishedAtMs = 0.0;

    private bool $finished = false;

    /** @var array<string, mixed> */
    private array $memory = [];

    private function __construct(
        public readonly ProcessingOptions $options,
        public readonly CarbonImmutable $startedAt,
        private readonly float $startedAtHr,
    ) {
    }

    public static function start(ProcessingOptions $options): self
    {
        return new self($options, CarbonImmutable::now(), hrtime(true) / 1_000_000);
    }

    public function record(BatchOutcome $outcome): void
    {
        $this->processed += $outcome->processed;
        $this->taxCents += $outcome->taxTotalCents;
        $this->affectedRows += $outcome->affectedRows;
        $this->lastId = max($this->lastId, $outcome->lastId);

        if ($outcome->processed > 0) {
            $this->batches++;
        }
    }

    public function finish(MemoryGuard $memoryGuard): self
    {
        $this->finishedAtMs = hrtime(true) / 1_000_000;
        $this->memory = $memoryGuard->snapshot();
        $this->finished = true;

        return $this;
    }

    public function processed(): int
    {
        return $this->processed;
    }

    public function batches(): int
    {
        return $this->batches;
    }

    public function taxCents(): int
    {
        return $this->taxCents;
    }

    public function affectedRows(): int
    {
        return $this->affectedRows;
    }

    public function lastId(): int
    {
        return $this->lastId;
    }

    public function durationMs(): float
    {
        $end = $this->finished ? $this->finishedAtMs : hrtime(true) / 1_000_000;

        return round($end - $this->startedAtHr, 2);
    }

    public function throughputPerSecond(): float
    {
        $seconds = max($this->durationMs() / 1000, 0.000001);

        return round($this->processed / $seconds, 2);
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'processed' => $this->processed,
            'batches' => $this->batches,
            'tax_total_cents' => $this->taxCents,
            'tax_total' => number_format($this->taxCents / 100, 2, '.', ''),
            'affected_rows' => $this->affectedRows,
            'last_id' => $this->lastId,
            'duration_ms' => $this->durationMs(),
            'invoices_per_second' => $this->throughputPerSecond(),
            'peak_memory_bytes' => $this->memory['peak_bytes'] ?? memory_get_peak_usage(),
            'peak_memory_megabytes' => $this->memory['peak_megabytes'] ?? round(memory_get_peak_usage() / 1048576, 2),
            'real_peak_memory_bytes' => $this->memory['real_peak_bytes'] ?? memory_get_peak_usage(true),
            'real_peak_memory_megabytes' => $this->memory['real_peak_megabytes'] ?? round(memory_get_peak_usage(true) / 1048576, 2),
            'memory_limit_bytes' => $this->memory['limit_bytes'] ?? null,
            'batch_size' => $this->options->batchSize,
            'event_mode' => $this->options->eventMode->value,
            'transaction_mode' => $this->options->transactionMode->value,
            'from_id' => $this->options->afterId,
            'max_rows' => $this->options->maxRows,
            'started_at' => $this->startedAt->toIso8601String(),
        ];
    }
}
