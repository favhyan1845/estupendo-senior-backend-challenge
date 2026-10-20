<?php

namespace App\Models;

use App\Enums\InvoiceStatus;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

/**
 * Eloquent representation of an invoice.
 *
 * IMPORTANT: the 100k-row pipeline deliberately does NOT hydrate this model
 * per row. It is used for single-record operations, tests and reporting only
 * (see App\Services\Invoices\KeysetInvoiceReader for the streaming path).
 */
class Invoice extends Model
{
    /** @use HasFactory<\Database\Factories\InvoiceFactory> */
    use HasFactory;

    protected $fillable = [
        'number',
        'customer_name',
        'customer_tax_id',
        'subtotal_cents',
        'tax_rate_bps',
        'tax_cents',
        'total_cents',
        'currency',
        'status',
        'processed_at',
    ];

    protected function casts(): array
    {
        return [
            'subtotal_cents' => 'integer',
            'tax_rate_bps' => 'integer',
            'tax_cents' => 'integer',
            'total_cents' => 'integer',
            'status' => InvoiceStatus::class,
            'processed_at' => 'immutable_datetime',
        ];
    }
}
