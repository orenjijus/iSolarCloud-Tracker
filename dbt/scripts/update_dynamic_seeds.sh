#!/bin/bash
# Script untuk update dynamic seeds yang perlu update berkala
# Usage: ./update_dynamic_seeds.sh [seed_name]

cd "$(dirname "$0")/.."

echo "🔄 Updating Dynamic Seeds..."

# Dynamic seeds yang perlu update berkala
DYNAMIC_SEEDS=(
    "seed_daily_simulation_target"
    "seed_daily_kpi_monthly"
    "seed_energy_adjustment_daily"
    "seed_ghi_adjustment_daily"
)

# Jika ada argument, update seed tertentu saja
if [ $# -gt 0 ]; then
    SEED_NAME=$1
    echo "📦 Updating seed: $SEED_NAME"
    dbt seed --select "$SEED_NAME"
else
    # Update semua dynamic seeds
    echo "📦 Updating all dynamic seeds..."
    for seed in "${DYNAMIC_SEEDS[@]}"; do
        echo "  - $seed"
        dbt seed --select "$seed"
    done
fi

echo "✅ Dynamic seeds update completed!"

