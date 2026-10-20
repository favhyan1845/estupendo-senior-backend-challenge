-- Test database used by phpunit (DB_DATABASE=estupendo_test).
CREATE DATABASE IF NOT EXISTS estupendo_test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON estupendo_test.* TO 'estupendo'@'%';
FLUSH PRIVILEGES;
