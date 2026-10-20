<?php

namespace App\Actions\Invoices;

use InvalidArgumentException;

/**
 * Pure tax calculation: no I/O, no Eloquent, no globals.
 *
 * Money is handled in integer minor units (cents) with basis-point rates,
 * so there is no float drift and no bcmath dependency. Rounding is
 * half-up, which is the usual fiscal rule for COP invoices.
 */
final class CalculateInvoiceTax
{
    private const BPS = 10_000;

    /** Guarantees subtotal * rate stays inside PHP_INT_MAX (64-bit). */
    private const MAX_SUBTOTAL_CENTS = 900_000_000_000_000;

    public function __invoke(int $subtotalCents, int $taxRateBps, string $currency = 'COP'): TaxResult
    {
        if ($subtotalCents < 0) {
            throw new InvalidArgumentException("Subtotal cannot be negative, got [{$subtotalCents}].");
        }

        if ($subtotalCents > self::MAX_SUBTOTAL_CENTS) {
            throw new InvalidArgumentException("Subtotal [{$subtotalCents}] exceeds the supported range.");
        }

        if ($taxRateBps < 0 || $taxRateBps > self::BPS) {
            throw new InvalidArgumentException("Tax rate must be between 0 and 10000 bps, got [{$taxRateBps}].");
        }

        if (preg_match('/^[A-Z]{3}$/', $currency) !== 1) {
            throw new InvalidArgumentException("Currency must be an ISO-4217 code, got [{$currency}].");
        }

        // Half-up: (subtotal * bps + BPS/2) / BPS, all in integer arithmetic.
        $taxCents = intdiv($subtotalCents * $taxRateBps + intdiv(self::BPS, 2), self::BPS);

        return new TaxResult(
            subtotalCents: $subtotalCents,
            taxRateBps: $taxRateBps,
            taxCents: $taxCents,
            totalCents: $subtotalCents + $taxCents,
            currency: $currency,
        );
    }
}
