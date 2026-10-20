<?php

namespace App\Support;

use App\Exceptions\MemoryLimitExceededException;

/**
 * Guards the hard 32 MB ceiling required by the challenge.
 *
 * memory_get_peak_usage() reports the real allocator peak (not the emulated
 * one), which is the value that actually matters when PHP is pinned to
 * memory_limit=32M. The pipeline calls assertWithinLimit() after every batch,
 * so a regression fails fast with a precise message instead of an opaque
 * "Allowed memory size exhausted" fatal error.
 */
final class MemoryGuard
{
    public function __construct(private readonly int $limitBytes)
    {
    }

    public static function fromConfig(): self
    {
        return new self((int) config('invoices.memory_limit_bytes', 32 * 1024 * 1024));
    }

    public function limitBytes(): int
    {
        return $this->limitBytes;
    }

    public function peakBytes(): int
    {
        // Logical peak: this is exactly what PHP's memory_limit enforces.
        return memory_get_peak_usage();
    }

    public function realPeakBytes(): int
    {
        // Allocator peak (2 MB arenas): includes fragmentation/overhead, useful
        // for capacity planning of the container, not for memory_limit.
        return memory_get_peak_usage(true);
    }

    public function usageBytes(): int
    {
        return memory_get_usage();
    }

    public function assertWithinLimit(string $stage): void
    {
        $peak = $this->peakBytes();

        if ($peak > $this->limitBytes) {
            throw MemoryLimitExceededException::at($stage, $peak, $this->limitBytes);
        }
    }

    /**
     * @return array{limit_bytes: int, peak_bytes: int, real_peak_bytes: int, current_bytes: int, peak_megabytes: float, real_peak_megabytes: float}
     */
    public function snapshot(): array
    {
        return [
            'limit_bytes' => $this->limitBytes,
            'peak_bytes' => $this->peakBytes(),
            'real_peak_bytes' => $this->realPeakBytes(),
            'current_bytes' => $this->usageBytes(),
            'peak_megabytes' => round($this->peakBytes() / 1048576, 2),
            'real_peak_megabytes' => round($this->realPeakBytes() / 1048576, 2),
        ];
    }
}
