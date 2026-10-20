<?php

namespace Tests\Feature;

use App\Enums\InvoiceStatus;
use App\Jobs\ProcessInvoiceBatchJob;
use App\Models\Invoice;
use Illuminate\Foundation\Testing\DatabaseTruncation;
use Illuminate\Support\Facades\Queue;
use Tests\Concerns\SeedsInvoices;
use Tests\TestCase;

class ProcessInvoicesCommandTest extends TestCase
{
    use DatabaseTruncation;
    use SeedsInvoices;

    public function test_it_processes_pending_invoices_and_reports_json(): void
    {
        $this->seedInvoices(75);

        $this->artisan('invoices:process', ['--batch' => 25, '--json' => true])
            ->expectsOutputToContain('"processed": 75')
            ->assertSuccessful();

        $this->assertSame(0, Invoice::query()->where('status', InvoiceStatus::Pending)->count());
    }

    public function test_it_supports_the_literal_single_transaction_mode(): void
    {
        $this->seedInvoices(40);

        $this->artisan('invoices:process', ['--batch' => 10, '--transaction' => 'single'])
            ->assertSuccessful();

        $this->assertSame(0, Invoice::query()->where('status', InvoiceStatus::Pending)->count());
    }

    public function test_it_rejects_an_invalid_event_mode(): void
    {
        $this->artisan('invoices:process', ['--events' => 'nope'])
            ->expectsOutputToContain('Invalid --events value')
            ->assertExitCode(2);
    }

    public function test_it_fans_out_batch_jobs_when_queue_option_is_used(): void
    {
        Queue::fake();

        $this->artisan('invoices:process', ['--queue' => true, '--batch' => 300])
            ->assertSuccessful();

        Queue::assertPushed(ProcessInvoiceBatchJob::class, function (ProcessInvoiceBatchJob $job) {
            return $job->batchSize === 300 && $job->afterId === 0;
        });
    }
}
