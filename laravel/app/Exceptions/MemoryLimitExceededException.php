<?php

namespace App\Exceptions;

use RuntimeException;

final class MemoryLimitExceededException extends RuntimeException
{
    public static function at(string $stage, int $peakBytes, int $limitBytes): self
    {
        return new self(sprintf(
            'PHP peak memory of %s exceeded the %s ceiling at [%s].',
            self::megabytes($peakBytes),
            self::megabytes($limitBytes),
            $stage,
        ));
    }

    private static function megabytes(int $bytes): string
    {
        return number_format($bytes / 1048576, 2).' MB';
    }
}
