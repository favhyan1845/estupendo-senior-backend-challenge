#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-mysql}"
DB_PORT="${DB_PORT:-3306}"
DB_USERNAME="${DB_USERNAME:-estupendo}"
DB_PASSWORD="${DB_PASSWORD:-secret}"
DB_DATABASE="${DB_DATABASE:-estupendo}"

wait_for_database() {
    echo "Waiting for MySQL at ${DB_HOST}:${DB_PORT}..."

    for _ in $(seq 1 60); do
        if DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USERNAME="$DB_USERNAME" \
           DB_PASSWORD="$DB_PASSWORD" DB_DATABASE="$DB_DATABASE" php -r '
                try {
                    new PDO(
                        "mysql:host=".getenv("DB_HOST").";port=".getenv("DB_PORT").";dbname=".getenv("DB_DATABASE"),
                        getenv("DB_USERNAME"),
                        getenv("DB_PASSWORD")
                    );
                    exit(0);
                } catch (Throwable $e) {
                    fwrite(STDERR, $e->getMessage().PHP_EOL);
                    exit(1);
                }
            '; then
            echo "MySQL is ready."
            return 0
        fi

        sleep 2
    done

    echo "MySQL did not become ready in time." >&2
    return 1
}

if [ "${WAIT_FOR_DATABASE:-true}" = "true" ]; then
    wait_for_database
fi

if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
    echo "Running migrations..."
    php artisan migrate --force
fi

if [ "${SEED_INVOICES:-0}" -gt 0 ]; then
    echo "Seeding ${SEED_INVOICES} invoices (skipped if the table is already populated)..."
    php artisan invoices:seed --count="${SEED_INVOICES}" --chunk="${SEED_INVOICES_CHUNK:-500}" --if-empty
fi

exec "$@"
