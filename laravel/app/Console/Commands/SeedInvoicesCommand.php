<?php

namespace App\Console\Commands;

use Carbon\CarbonImmutable;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

/**
 * Seeds invoices with multi-row INSERTs, so generating 100k rows never holds
 * more than one chunk of PHP arrays in memory.
 */
final class SeedInvoicesCommand extends Command
{
    protected $signature = 'invoices:seed
                            {--count=100000 : Number of invoices to create}
                            {--offset=0 : Sequence offset, useful to resume an interrupted seed}
                            {--chunk=500 : Rows per INSERT statement}
                            {--fresh : Truncate the table before seeding}
                            {--if-empty : Skip when the table already contains rows}';

    protected $description = 'Seed invoices in memory-bounded batches (keyset-friendly data set).';

    private const CUSTOMERS = [
        'Almacenes Éxito S.A.', 'Bancolombia S.A.', 'Ecopetrol S.A.', 'Grupo Nutresa',
        'Cementos Argos', 'Davivienda S.A.', 'Rappi Colombia', 'Mercado Libre Colombia',
        'Claro Colombia', 'Tigo Une', 'EPM', 'Cencosud Colombia', 'Falabella Colombia',
        'Colsubsidio', 'Sura Colombia', 'Postobón S.A.', 'Bavaria S.A.', 'Terpel S.A.',
    ];

    private const RATES_BPS = [0, 500, 1900];

    public function handle(): int
    {
        $count = max(0, (int) $this->option('count'));
        $chunkSize = max(1, (int) $this->option('chunk'));
        $sequenceOffset = max(0, (int) $this->option('offset'));

        if ($this->option('if-empty') && DB::table('invoices')->exists()) {
            $this->components->info('Invoices table already populated; skipping seed (--if-empty).');

            return self::SUCCESS;
        }

        if ($this->option('fresh')) {
            DB::table('invoices')->truncate();
            $this->components->info('Invoices table truncated (--fresh).');
        }

        if ($count === 0) {
            $this->components->warn('Nothing to seed (--count=0).');

            return self::SUCCESS;
        }

        $now = CarbonImmutable::now()->format('Y-m-d H:i:s');
        $bar = $this->output->createProgressBar($count);
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% %elapsed:6s%');
        $bar->start();

        for ($offset = 0; $offset < $count; $offset += $chunkSize) {
            $size = min($chunkSize, $count - $offset);
            $rows = [];

            for ($i = 0; $i < $size; $i++) {
                $sequence = $sequenceOffset + $offset + $i + 1;

                $rows[] = [
                    'number' => sprintf('FAC-%08d', $sequence),
                    'customer_name' => self::CUSTOMERS[$sequence % count(self::CUSTOMERS)],
                    'customer_tax_id' => sprintf('9%08d-%d', $sequence % 90_000_000, $sequence % 10),
                    'subtotal_cents' => mt_rand(10_000, 50_000_000),
                    'tax_rate_bps' => self::RATES_BPS[$sequence % count(self::RATES_BPS)],
                    'tax_cents' => 0,
                    'total_cents' => 0,
                    'currency' => 'COP',
                    'status' => 'pending',
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            }

            DB::table('invoices')->insert($rows);
            $bar->advance($size);

            // Keep the seed memory-flat: the INSERT closure/bindings form short
            // lived cycles that PHP otherwise collects only in large batches.
            unset($rows);
            gc_collect_cycles();
        }

        $bar->finish();
        $this->newLine(2);
        $this->components->info(sprintf(
            'Seeded %s invoices in chunks of %d (peak PHP memory %.2f MB).',
            number_format($count),
            $chunkSize,
            memory_get_peak_usage(true) / 1048576,
        ));

        return self::SUCCESS;
    }
}
