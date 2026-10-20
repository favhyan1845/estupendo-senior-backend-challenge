<?php

namespace Database\Factories;

use App\Enums\InvoiceStatus;
use App\Models\Invoice;
use Illuminate\Database\Eloquent\Factories\Factory;

/**
 * @extends Factory<Invoice>
 */
class InvoiceFactory extends Factory
{
    protected $model = Invoice::class;

    public function definition(): array
    {
        return [
            'number' => 'FAC-'.fake()->unique()->numerify('########'),
            'customer_name' => fake()->company(),
            'customer_tax_id' => fake()->numerify('#########-#'),
            'subtotal_cents' => fake()->numberBetween(10_000, 50_000_000),
            'tax_rate_bps' => fake()->randomElement([0, 500, 1900]),
            'tax_cents' => 0,
            'total_cents' => 0,
            'currency' => 'COP',
            'status' => InvoiceStatus::Pending,
            'processed_at' => null,
        ];
    }

    public function processed(): static
    {
        return $this->state(fn () => [
            'status' => InvoiceStatus::Processed,
            'processed_at' => now(),
        ]);
    }
}
