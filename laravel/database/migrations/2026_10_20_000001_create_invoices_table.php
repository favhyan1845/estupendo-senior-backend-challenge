<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('invoices', function (Blueprint $table) {
            $table->id();
            $table->string('number', 32)->unique();
            $table->string('customer_name', 120);
            $table->string('customer_tax_id', 20);

            // Money is stored in integer minor units (cents) - never floats.
            $table->unsignedBigInteger('subtotal_cents');
            $table->unsignedSmallInteger('tax_rate_bps')->default(1900);
            $table->unsignedBigInteger('tax_cents')->default(0);
            $table->unsignedBigInteger('total_cents')->default(0);
            $table->char('currency', 3)->default('COP');

            $table->string('status', 16)->default('pending');
            $table->timestamp('processed_at')->nullable();
            $table->timestamps();

            // Keyset pagination walks this index: WHERE status = ? AND id > ? ORDER BY id
            $table->index(['status', 'id'], 'invoices_status_id_index');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('invoices');
    }
};
