<?php

namespace App\Enums;

enum TransactionMode: string
{
    /** One short transaction per keyset batch (recommended for production). */
    case Chunked = 'chunked';

    /** A single DB::transaction around the whole run (literal requirement, risky). */
    case Single = 'single';
}
