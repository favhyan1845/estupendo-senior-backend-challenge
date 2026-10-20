<?php

namespace App\Enums;

enum InvoiceStatus: string
{
    case Pending = 'pending';
    case Processed = 'processed';
    case Failed = 'failed';
}
