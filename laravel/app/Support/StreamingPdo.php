<?php

namespace App\Support;

use Generator;
use PDO;
use RuntimeException;

/**
 * Dedicated PDO connection for read-only streaming.
 *
 * Two properties make the 32 MB ceiling reachable:
 *
 *  1. PDO::MYSQL_ATTR_USE_BUFFERED_QUERY = false -> mysqlnd hands rows to PHP
 *     as the server produces them instead of buffering the full result set in
 *     process memory (the default behaviour of PDO / Laravel's query builder).
 *  2. Native prepared statements with NULL emulation and a LIMIT on every
 *     keyset page, so each trip only transfers `batchSize + 1` rows.
 *
 * It is intentionally kept outside Laravel's connection manager: the default
 * connection stays free for writes and transactions, while this one holds an
 * unbuffered cursor (a connection with an open unbuffered result can run no
 * other query until the cursor is drained/closed).
 */
final class StreamingPdo
{
    private ?PDO $pdo = null;

    /** @param array<string, mixed> $config a laravel database connection definition */
    public function __construct(
        private readonly array $config,
        private readonly bool $unbuffered = true,
    ) {
    }

    public static function fromConnection(?string $name = null): self
    {
        $name ??= (string) config('invoices.reader.connection', config('database.default'));
        $config = config("database.connections.{$name}");

        if (! is_array($config)) {
            throw new RuntimeException("Unknown database connection [{$name}] for the streaming reader.");
        }

        return new self($config);
    }

    public function pdo(): PDO
    {
        return $this->pdo ??= $this->connect();
    }

    /**
     * Streams rows as associative arrays without buffering the result set.
     *
     * @param  array<int, int|string>  $bindings
     * @return Generator<int, array<string, mixed>>
     */
    public function iterate(string $sql, array $bindings = []): Generator
    {
        $statement = $this->pdo()->prepare($sql);

        foreach (array_values($bindings) as $index => $value) {
            $statement->bindValue(
                $index + 1,
                $value,
                is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR,
            );
        }

        $statement->execute();

        try {
            while (($row = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
                yield $row;
            }
        } finally {
            $statement->closeCursor();
        }
    }

    public function disconnect(): void
    {
        $this->pdo = null;
    }

    private function connect(): PDO
    {
        $driver = $this->config['driver'] ?? null;

        [$dsn, $options] = match ($driver) {
            'mysql', 'mariadb' => [$this->mysqlDsn(), $this->mysqlOptions()],
            'sqlite' => [$this->sqliteDsn(), $this->baseOptions()],
            default => throw new RuntimeException("Streaming reads are not supported for driver [{$driver}]."),
        };

        $pdo = new PDO(
            $dsn,
            $this->config['username'] ?? null,
            $this->config['password'] ?? null,
            $options + $this->baseOptions(),
        );

        return $pdo;
    }

    /** @return array<int, mixed> */
    private function baseOptions(): array
    {
        return [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ];
    }

    /** @return array<int, mixed> */
    private function mysqlOptions(): array
    {
        $options = $this->baseOptions() + [
            PDO::ATTR_EMULATE_PREPARES => false,
        ];

        if (defined('PDO::MYSQL_ATTR_USE_BUFFERED_QUERY')) {
            $options[PDO::MYSQL_ATTR_USE_BUFFERED_QUERY] = ! $this->unbuffered;
        }

        return $options;
    }

    private function mysqlDsn(): string
    {
        $socket = (string) ($this->config['unix_socket'] ?? '');
        $database = (string) ($this->config['database'] ?? '');
        $charset = (string) ($this->config['charset'] ?? 'utf8mb4');

        if ($socket !== '') {
            return "mysql:unix_socket={$socket};dbname={$database};charset={$charset}";
        }

        $host = (string) ($this->config['host'] ?? '127.0.0.1');
        $port = (string) ($this->config['port'] ?? '3306');

        return "mysql:host={$host};port={$port};dbname={$database};charset={$charset}";
    }

    private function sqliteDsn(): string
    {
        return 'sqlite:'.($this->config['database'] ?? ':memory:');
    }
}
