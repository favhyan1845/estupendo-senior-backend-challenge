<?php

namespace App\Services\Invoices;

use App\Enums\InvoiceStatus;
use App\Support\StreamingPdo;
use InvalidArgumentException;

/**
 * Keyset (a.k.a. seek) pagination reader.
 *
 * OFFSET pagination degrades linearly: MySQL must scan and discard every row
 * before the offset, so page 200 of 100k re-reads 99.5k rows. This reader
 * remembers the last id and asks for `id > lastId`, which the composite
 * (status, id) index serves as a range scan with constant cost per page.
 * Combined with the unbuffered PDO cursor, memory stays flat.
 */
final class KeysetInvoiceReader
{
    public function __construct(
        private readonly StreamingPdo $pdo,
        private readonly string $table = 'invoices',
        private readonly InvoiceStatus $status = InvoiceStatus::Pending,
    ) {
        if (preg_match('/^[A-Za-z0-9_]+$/', $table) !== 1) {
            throw new InvalidArgumentException("Invalid invoices table name [{$table}].");
        }
    }

    public static function fromConfig(): self
    {
        return new self(
            pdo: StreamingPdo::fromConnection(),
            table: (string) config('invoices.table', 'invoices'),
            status: InvoiceStatus::Pending,
        );
    }

    /**
     * Fetch at most $size + 1 rows to know whether another page exists without
     * a second COUNT(*) query (which would scan the same range again).
     */
    public function fetchBatch(int $afterId, int $size, int $batchNumber = 1): ?InvoiceBatch
    {
        $size = max(1, $size);
        $limit = $size + 1;

        $sql = sprintf(
            'SELECT id, number, subtotal_cents, tax_rate_bps, currency
               FROM %s
              WHERE status = ? AND id > ?
              ORDER BY id ASC
              LIMIT ?',
            $this->table,
        );

        $rows = [];

        foreach ($this->pdo->iterate($sql, [$this->status->value, $afterId, $limit]) as $row) {
            $rows[] = [
                'id' => (int) $row['id'],
                'number' => (string) $row['number'],
                'subtotal_cents' => (int) $row['subtotal_cents'],
                'tax_rate_bps' => (int) $row['tax_rate_bps'],
                'currency' => (string) $row['currency'],
            ];
        }

        if ($rows === []) {
            return null;
        }

        $hasMore = count($rows) > $size;

        if ($hasMore) {
            array_pop($rows);
        }

        $last = $rows[count($rows) - 1];

        return new InvoiceBatch(
            rows: $rows,
            lastId: $last['id'],
            hasMore: $hasMore,
            number: $batchNumber,
        );
    }
}
