<?php

namespace App\Actions\Invoices;

use App\Enums\InvoiceStatus;
use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\DB;
use InvalidArgumentException;

/**
 * Writes a whole keyset page with ONE UPDATE statement.
 *
 * Naive implementations do `$invoice->save()` per row: 100k round trips,
 * 100k row locks and (inside a single transaction) 100k undo-log entries.
 * Here a single `UPDATE ... CASE id WHEN ... END` touches only the processed
 * columns, keeps `WHERE status = 'pending'` for idempotency and returns the
 * affected row count so the caller can detect concurrent processing.
 */
final class PersistProcessedInvoices
{
    /**
     * @param  list<array{id: int, tax_cents: int, total_cents: int}>  $invoices
     */
    public function __invoke(
        array $invoices,
        CarbonImmutable $processedAt,
        InvoiceStatus $expectedStatus = InvoiceStatus::Pending,
    ): int {
        if ($invoices === []) {
            return 0;
        }

        $connection = DB::connection();
        $table = $connection->getQueryGrammar()->wrapTable((string) config('invoices.table', 'invoices'));
        $rowCount = count($invoices);

        $ids = array_column($invoices, 'id');
        $placeholders = implode(',', array_fill(0, $rowCount, '?'));
        $taxCases = implode(' ', array_fill(0, $rowCount, 'WHEN ? THEN ?'));
        $totalCases = implode(' ', array_fill(0, $rowCount, 'WHEN ? THEN ?'));

        $sql = "UPDATE {$table}
                   SET tax_cents = CASE id {$taxCases} ELSE tax_cents END,
                       total_cents = CASE id {$totalCases} ELSE total_cents END,
                       status = ?,
                       processed_at = ?,
                       updated_at = ?
                 WHERE status = ?
                   AND id IN ({$placeholders})";

        $bindings = [];

        // 1) tax_cents CASE id WHEN ? THEN ? ...
        foreach ($invoices as $invoice) {
            $bindings[] = $invoice['id'];
            $bindings[] = $invoice['tax_cents'];
        }

        // 2) total_cents CASE id WHEN ? THEN ? ...
        foreach ($invoices as $invoice) {
            $bindings[] = $invoice['id'];
            $bindings[] = $invoice['total_cents'];
        }

        // 3) SET status, processed_at, updated_at
        $timestamp = $processedAt->format('Y-m-d H:i:s');
        $bindings[] = InvoiceStatus::Processed->value;
        $bindings[] = $timestamp;
        $bindings[] = $timestamp;

        // 4) WHERE status = ?
        $bindings[] = $expectedStatus->value;

        // 5) AND id IN (?, ...)
        foreach ($ids as $id) {
            $bindings[] = $id;
        }

        // Sanity check: MySQL caps placeholders at 65,535 per statement.
        if (count($bindings) > 65_000) {
            throw new InvalidArgumentException('Batch too large for a single UPDATE statement.');
        }

        return $connection->update($sql, $bindings);
    }
}
