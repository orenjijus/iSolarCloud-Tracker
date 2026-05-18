#!/bin/bash
# Script untuk run dbt dengan auto-update dynamic seeds
# Usage: ./run_dbt_with_seeds.sh [dbt_command]

# Dynamic seeds yang perlu di-update sebelum dbt run
DYNAMIC_SEEDS=(
    "seed_daily_simulation_target"
    "seed_daily_kpi_monthly"
    "seed_energy_adjustment_daily"
    "seed_ghi_adjustment_daily"
)

# Static seeds yang TIDAK perlu di-update (hanya jika ada perubahan)
STATIC_SEEDS=(
    "seed_metric_mapper"
)

echo "🔄 Auto-updating dynamic seeds..."
for seed in "${DYNAMIC_SEEDS[@]}"; do
    echo "  → Updating $seed..."
    dbt seed --select "$seed"
    if [ $? -ne 0 ]; then
        echo "  ❌ Failed to update $seed"
        exit 1
    fi
done

echo "✅ Dynamic seeds updated successfully"
echo ""

# Run dbt command (default: dbt run)
DBT_COMMAND=${1:-run}
echo "🚀 Running dbt $DBT_COMMAND..."
dbt $DBT_COMMAND "${@:2}"

