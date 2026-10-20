<?php

namespace Tests\Unit;

use App\Exceptions\MemoryLimitExceededException;
use App\Support\MemoryGuard;
use Tests\TestCase;

class MemoryGuardTest extends TestCase
{
    public function test_it_reports_a_snapshot_with_the_configured_ceiling(): void
    {
        $guard = new MemoryGuard(32 * 1024 * 1024);
        $snapshot = $guard->snapshot();

        $this->assertSame(32 * 1024 * 1024, $snapshot['limit_bytes']);
        $this->assertGreaterThan(0, $snapshot['peak_bytes']);
        $this->assertLessThan($snapshot['limit_bytes'], $snapshot['peak_bytes']);
    }

    public function test_it_throws_when_the_peak_exceeds_the_ceiling(): void
    {
        $guard = new MemoryGuard(1024);

        $this->expectException(MemoryLimitExceededException::class);
        $this->expectExceptionMessage('exceeded');

        $guard->assertWithinLimit('unit-test');
    }

    public function test_it_does_not_throw_below_the_ceiling(): void
    {
        $guard = new MemoryGuard(512 * 1024 * 1024);

        $guard->assertWithinLimit('unit-test');

        $this->addToAssertionCount(1);
    }
}
