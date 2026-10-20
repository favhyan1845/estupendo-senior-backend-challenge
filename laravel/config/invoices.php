<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Invoices table
    |--------------------------------------------------------------------------
    */

    'table' => env('INVOICES_TABLE', 'invoices'),

    /*
    |--------------------------------------------------------------------------
    | Keyset batch size
    |--------------------------------------------------------------------------
    |
    | Rows fetched per keyset page. Rows are kept as plain arrays (no Eloquent
    | hydration) so memory stays constant regardless of the total row count.
    | 500 rows * ~0.4 KB ≈ 0.2 MB of heap per batch.
    |
    */

    'batch_size' => (int) env('INVOICES_BATCH_SIZE', 500),

    /*
    |--------------------------------------------------------------------------
    | Hard memory ceiling (bytes)
    |--------------------------------------------------------------------------
    |
    | The processing pipeline aborts as soon as the PHP peak usage crosses this
    | value. Run the command with `php -d memory_limit=32M` so PHP itself also
    | enforces the ceiling.
    |
    */

    'memory_limit_bytes' => (int) env('INVOICES_MEMORY_LIMIT_MB', 32) * 1024 * 1024,

    /*
    |--------------------------------------------------------------------------
    | Domain events
    |--------------------------------------------------------------------------
    |
    | batch        -> one InvoiceBatchProcessed event per batch (production safe)
    | per_invoice  -> one InvoiceProcessed event per invoice (literal spec mode)
    |
    */

    'events' => [
        'mode' => env('INVOICES_EVENT_MODE', 'batch'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Transaction strategy
    |--------------------------------------------------------------------------
    |
    | chunked -> one short transaction per keyset batch (recommended)
    | single  -> a single DB::transaction wrapping all 100k rows (demo/risk)
    |
    */

    'transaction' => [
        'mode' => env('INVOICES_TRANSACTION_MODE', 'chunked'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Streaming reader
    |--------------------------------------------------------------------------
    |
    | Reads run on their own PDO connection opened with
    | PDO::MYSQL_ATTR_USE_BUFFERED_QUERY = false, so MySQL never materialises
    | the whole result set in the PHP process.
    |
    */

    'reader' => [
        'connection' => env('INVOICES_READER_CONNECTION', env('DB_CONNECTION', 'mysql')),
        'fetch_rows' => (int) env('INVOICES_READER_BATCH', env('INVOICES_BATCH_SIZE', 500)),
    ],

    /*
    |--------------------------------------------------------------------------
    | Queue used by ProcessInvoiceBatchJob
    |--------------------------------------------------------------------------
    */

    'queue' => env('INVOICES_QUEUE', 'invoices'),

    'connection' => env('INVOICES_QUEUE_CONNECTION', env('QUEUE_CONNECTION', 'redis')),

];
