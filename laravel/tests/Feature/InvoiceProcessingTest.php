<?php

namespace Tests\Feature;

use App\Enums\InvoiceEventMode;
use App\Enums\InvoiceStatus;
use App\Enums\TransactionMode;
use App\Events\InvoiceBatchProcessed;
use App\Events\InvoiceProcessed;
use App\Listeners\CollectInvoiceProcessingMetrics;
use App\Models\Invoice;
use App\Services\Invoices\InvoiceProcessingService;
use App\Services\Invoices\ProcessingOptions;
use Illuminate\Foundation\Testing\DatabaseTruncation;
use Illuminate\Support\Facades\Event;
use Symfony\Component\Process\Process;
use Tests\Concerns\SeedsInvoices;
use Tests\TestCase;

class InvoiceProcessingTest extends TestCase
{
    use DatabaseTruncation;
    use SeedsInvoices;

    public function test_it_processes_every_pending_invoice_with_keyset_pagination(): void
    {
        $this->seedInvoices(1_205);

        $report = app(InvoiceProcessingService::class)->process(new ProcessingOptions(batchSize: 500));
        $data = $report->toArray();

        $this->assertSame(1_205, $data['processed']);
        $this->assertSame(3, $data['batches']);
        $this->assertSame(1_205, $data['affected_rows']);
        $this->assertSame(InvoiceStatus::Processed, Invoice::query()->first()->status);
        $this->assertSame(0, Invoice::query()->where('status', 'pending')->count());

        $this->assertSame(
            Invoice::query()->count(),
            Invoice::query()->whereNotNull('processed_at')->count()
        );

        // 19% over 100.00 with half-up rounding, verified end to end.
        $invoice = Invoice::query()->find(1);
        $this->assertNotNull($invoice);
        $expectedTax = intdiv($invoice->subtotal_cents * $invoice->tax_rate_bps + 5_000, 10_000);
        $this->assertSame($expectedTax, $invoice->tax_cents);
        $this->assertSame($invoice->subtotal_cents + $expectedTax, $invoice->total_cents);
    }

    public function test_the_real_command_stays_under_32mb_in_a_isolated_php_process(): void
    {
        $this->seedInvoices(2_000, 'MEM');

        $env = [
            'APP_ENV' => 'testing',
            'DB_CONNECTION' => 'mysql',
            'DB_HOST' => (string) env('DB_HOST', '127.0.0.1'),
            'DB_PORT' => (string) env('DB_PORT', '3307'),
            'DB_DATABASE' => (string) env('DB_DATABASE', 'estupendo_test'),
            'DB_USERNAME' => (string) env('DB_USERNAME', 'root'),
            'DB_PASSWORD' => (string) env('DB_PASSWORD', ''),
            'INVOICES_MEMORY_LIMIT_MB' => '32',
            'CACHE_STORE' => 'array',
            'SESSION_DRIVER' => 'array',
            'QUEUE_CONNECTION' => 'sync',
        ];

        $process = new Process(
            [PHP_BINARY, '-d', 'memory_limit=32M', 'artisan', 'invoices:process', '--batch=250', '--json'],
            base_path(),
            $env,
        );

        // A real 32M memory_limit would abort with a fatal error otherwise.
        $process->setTimeout(120)->run();

        $this->assertTrue(
            $process->isSuccessful(),
            'The processing command failed: '.$process->getErrorOutput().$process->getOutput(),
        );

        $output = $process->getOutput();
        $payload = json_decode(substr($output, (int) strpos($output, '{')), true);

        $this->assertIsArray($payload, 'Expected a JSON report, got: '.$output);
        $this->assertSame(2_000, $payload['processed']);
        $this->assertLessThan(32 * 1024 * 1024, $payload['peak_memory_bytes']);
        $this->assertSame(32 * 1024 * 1024, $payload['memory_limit_bytes']);
    }

    public function test_it_is_idempotent_and_resumes_from_a_keyset_cursor(): void
    {
        $this->seedInvoices(600);

        $service = app(InvoiceProcessingService::class);

        $first = $service->process(new ProcessingOptions(batchSize: 200, maxRows: 200));
        $this->assertSame(200, $first->processed());

        $second = $service->process(new ProcessingOptions(batchSize: 200, afterId: $first->lastId()));
        $this->assertSame(400, $second->processed());

        $third = $service->process(new ProcessingOptions(batchSize: 200));
        $this->assertSame(0, $third->processed(), 'A second full run must not reprocess invoices.');

        $this->assertSame(0, Invoice::query()->where('status', 'pending')->count());
    }

    public function test_it_dispatches_a_batch_event_per_page_in_batch_mode(): void
    {
        $this->seedInvoices(120);
        Event::fake([InvoiceProcessed::class, InvoiceBatchProcessed::class]);

        app(InvoiceProcessingService::class)->process(new ProcessingOptions(batchSize: 50));

        Event::assertDispatchedTimes(InvoiceBatchProcessed::class, 3);
        Event::assertNotDispatched(InvoiceProcessed::class);
    }

    public function test_it_dispatches_one_event_per_invoice_in_per_invoice_mode(): void
    {
        $this->seedInvoices(120);
        Event::fake([InvoiceProcessed::class, InvoiceBatchProcessed::class]);

        app(InvoiceProcessingService::class)->process(new ProcessingOptions(
            batchSize: 50,
            eventMode: InvoiceEventMode::PerInvoice,
        ));

        Event::assertDispatchedTimes(InvoiceProcessed::class, 120);
        Event::assertNotDispatched(InvoiceBatchProcessed::class);
    }

    public function test_the_metrics_listener_counts_each_invoice_exactly_once(): void
    {
        $this->seedInvoices(210);

        $metrics = app(CollectInvoiceProcessingMetrics::class);
        $metrics->reset();

        app(InvoiceProcessingService::class)->process(new ProcessingOptions(batchSize: 100));

        $snapshot = $metrics->snapshot();

        $this->assertSame(210, $snapshot['invoices']);
        $this->assertSame(3, $snapshot['batches']);
        $this->assertSame(210, Invoice::query()->count());
    }

    public function test_single_transaction_mode_processes_the_whole_run_atomically(): void
    {
        $this->seedInvoices(300);

        $report = app(InvoiceProcessingService::class)->process(new ProcessingOptions(
            batchSize: 100,
            transactionMode: TransactionMode::Single,
        ));

        $this->assertSame(300, $report->processed());
        $this->assertSame(0, Invoice::query()->where('status', 'pending')->count());
    }
}
