<?php

namespace Tests\Concerns;

use Illuminate\Support\Facades\DB;

trait SeedsInvoices
{
    /**
     * Bulk-inserts pending invoices directly through the query builder so the
     * fixture cost stays O(1 page) and the streaming reader (a separate PDO
     * connection) can see committed rows.
     */
    protected function seedInvoices(int $count, string $prefix = 'TST'): void
    {
        $rows = [];
        $now = now()->format('Y-m-d H:i:s');

        for ($i = 1; $i <= $count; $i++) {
            $rows[] = [
                'number' => sprintf('%s-%06d', $prefix, $i),
                'customer_name' => 'Test Customer '.$i,
                'customer_tax_id' => '900000'.$i.'-1',
                'subtotal_cents' => 10_000 + ($i * 37),
                'tax_rate_bps' => 1900,
                'tax_cents' => 0,
                'total_cents' => 0,
                'currency' => 'COP',
                'status' => 'pending',
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        foreach (array_chunk($rows, 500) as $chunk) {
            DB::table('invoices')->insert($chunk);
        }
    }
}
