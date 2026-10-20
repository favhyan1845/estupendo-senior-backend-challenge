<?php

namespace App\Console\Commands;

use App\Enums\InvoiceEventMode;
use App\Enums\TransactionMode;
use App\Jobs\ProcessInvoiceBatchJob;
use App\Services\Invoices\InvoiceProcessingService;
use App\Services\Invoices\ProcessingOptions;
use Illuminate\Console\Command;
use Throwable;

final class ProcessInvoicesCommand extends Command
{
    protected $signature = 'invoices:process
                            {--batch=500 : Rows fetched per keyset page}
                            {--limit=0 : Maximum invoices to process (0 = all pending)}
                            {--after-id=0 : Keyset cursor to resume from}
                            {--events=batch : Event mode: batch|per_invoice}
                            {--transaction=chunked : Transaction mode: chunked|single}
                            {--queue : Fan out one ProcessInvoiceBatchJob per page instead of running in-process}
                            {--json : Emit the report as JSON}';

    protected $description = 'Apply taxes to pending invoices using streaming keyset pagination under a 32 MB memory ceiling.';

    public function handle(InvoiceProcessingService $service): int
    {
        $options = $this->resolveOptions();

        if ($options === null) {
            return self::INVALID;
        }

        if ($options->queue) {
            ProcessInvoiceBatchJob::dispatch(
                afterId: $options->afterId,
                batchSize: $options->batchSize,
                eventMode: $options->eventMode,
                maxRows: $options->maxRows,
            )
                ->onConnection((string) config('invoices.connection', 'redis'))
                ->onQueue((string) config('invoices.queue', 'invoices'));

            $this->components->info(sprintf(
                'Dispatched invoice batch job on connection [%s], queue [%s], starting after id %d.',
                config('invoices.connection'),
                config('invoices.queue'),
                $options->afterId,
            ));

            return self::SUCCESS;
        }

        try {
            $report = $service->process($options);
        } catch (Throwable $exception) {
            $this->components->error($exception->getMessage());
            report($exception);

            return self::FAILURE;
        }

        if ($this->option('json')) {
            $this->line((string) json_encode($report->toArray(), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

            return self::SUCCESS;
        }

        $data = $report->toArray();

        $this->newLine();
        $this->components->twoColumnDetail('<fg=green;options=bold>Invoices processed</>', number_format($data['processed']));
        $this->components->twoColumnDetail('Keyset batches', number_format($data['batches']).' of '.number_format($data['batch_size']).' rows');
        $this->components->twoColumnDetail('Tax total', $data['tax_total'].' '.($data['processed'] > 0 ? 'COP' : ''));
        $this->components->twoColumnDetail('Rows updated', number_format($data['affected_rows']));
        $this->components->twoColumnDetail('Duration', $data['duration_ms'].' ms ('.number_format($data['invoices_per_second']).' inv/s)');
        $this->components->twoColumnDetail('Peak PHP memory', $data['peak_memory_megabytes'].' MB (ceiling '.round(($data['memory_limit_bytes'] ?? 0) / 1048576, 2).' MB)');
        $this->components->twoColumnDetail('Peak allocator memory', $data['real_peak_memory_megabytes'].' MB');
        $this->components->twoColumnDetail('Event mode', $data['event_mode']);
        $this->components->twoColumnDetail('Transaction mode', $data['transaction_mode']);
        $this->newLine();

        if ($this->phpMemoryLimitBytes() === null) {
            $this->components->warn('PHP memory_limit is unlimited. Run with `php -d memory_limit=32M artisan invoices:process` to enforce the ceiling twice.');
        } elseif ($this->phpMemoryLimitBytes() > (int) config('invoices.memory_limit_bytes')) {
            $this->components->warn(sprintf(
                'PHP memory_limit is %s, above the 32 MB target. The pipeline guard still enforces %.0f MB; use `php -d memory_limit=32M` for a hard OS-level ceiling.',
                ini_get('memory_limit'),
                ((int) config('invoices.memory_limit_bytes')) / 1048576,
            ));
        }

        return self::SUCCESS;
    }

    private function phpMemoryLimitBytes(): ?int
    {
        $value = trim((string) ini_get('memory_limit'));

        if ($value === '' || $value === '-1') {
            return null;
        }

        $unit = strtolower($value[strlen($value) - 1]);
        $number = (int) $value;

        return match ($unit) {
            'k' => $number * 1024,
            'm' => $number * 1024 * 1024,
            'g' => $number * 1024 * 1024 * 1024,
            default => $number,
        };
    }

    private function resolveOptions(): ?ProcessingOptions
    {
        $eventMode = InvoiceEventMode::tryFrom((string) $this->option('events'));
        $transactionMode = TransactionMode::tryFrom((string) $this->option('transaction'));

        if ($eventMode === null) {
            $this->components->error('Invalid --events value. Use `batch` or `per_invoice`.');

            return null;
        }

        if ($transactionMode === null) {
            $this->components->error('Invalid --transaction value. Use `chunked` or `single`.');

            return null;
        }

        $batchSize = (int) $this->option('batch');

        if ($batchSize < 1 || $batchSize > 5_000) {
            $this->components->error('--batch must be between 1 and 5000.');

            return null;
        }

        return new ProcessingOptions(
            batchSize: $batchSize,
            maxRows: max(0, (int) $this->option('limit')),
            afterId: max(0, (int) $this->option('after-id')),
            eventMode: $eventMode,
            transactionMode: $transactionMode,
            queue: (bool) $this->option('queue'),
        );
    }
}
