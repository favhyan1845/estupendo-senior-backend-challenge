<?php

namespace App\Providers;

use App\Events\InvoiceBatchProcessed;
use App\Events\InvoiceProcessed;
use App\Listeners\CollectInvoiceProcessingMetrics;
use App\Services\Invoices\KeysetInvoiceReader;
use App\Support\MemoryGuard;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     */
    public function register(): void
    {
        $this->app->singleton(MemoryGuard::class, fn () => MemoryGuard::fromConfig());
        $this->app->singleton(CollectInvoiceProcessingMetrics::class);
        $this->app->bind(KeysetInvoiceReader::class, fn () => KeysetInvoiceReader::fromConfig());
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        Event::listen(InvoiceProcessed::class, [CollectInvoiceProcessingMetrics::class, 'handle']);
        Event::listen(InvoiceBatchProcessed::class, [CollectInvoiceProcessingMetrics::class, 'handle']);
    }
}
