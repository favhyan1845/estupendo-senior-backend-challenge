<?php

namespace App\Enums;

enum InvoiceEventMode: string
{
    /** One InvoiceBatchProcessed event per batch - constant memory, production safe. */
    case Batch = 'batch';

    /** One InvoiceProcessed event per invoice - literal wording of the exercise. */
    case PerInvoice = 'per_invoice';
}
