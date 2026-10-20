<?php

namespace Tests\Unit;

use App\Actions\Invoices\CalculateInvoiceTax;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class CalculateInvoiceTaxTest extends TestCase
{
    #[DataProvider('taxCases')]
    public function test_it_calculates_tax_with_half_up_rounding(
        int $subtotalCents,
        int $rateBps,
        int $expectedTaxCents,
        int $expectedTotalCents,
    ): void {
        $result = (new CalculateInvoiceTax())($subtotalCents, $rateBps);

        $this->assertSame($expectedTaxCents, $result->taxCents);
        $this->assertSame($expectedTotalCents, $result->totalCents);
        $this->assertSame($subtotalCents, $result->subtotalCents);
        $this->assertSame($rateBps, $result->taxRateBps);
        $this->assertSame('COP', $result->currency);
    }

    /**
     * @return array<string, array{int, int, int, int}>
     */
    public static function taxCases(): array
    {
        return [
            '19% over 100.00' => [10_000, 1900, 1_900, 11_900],
            '19% over 99.99 (half up)' => [9_999, 1900, 1_900, 11_899],
            '5% over 33.33 (half up)' => [3_333, 500, 167, 3_500],
            '0% rate' => [12_345, 0, 0, 12_345],
            '100% rate' => [12_345, 10_000, 12_345, 24_690],
            'rounds .5 up' => [1, 5000, 1, 2],
            'rounds .49 down' => [1, 4900, 0, 1],
        ];
    }

    public function test_it_rejects_negative_subtotals(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new CalculateInvoiceTax())(-1, 1900);
    }

    public function test_it_rejects_rates_above_100_percent(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new CalculateInvoiceTax())(1_000, 10_001);
    }

    public function test_it_rejects_invalid_currency_codes(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new CalculateInvoiceTax())(1_000, 1900, 'cop');
    }
}
