# PowerShell script untuk run dbt dengan auto-update dynamic seeds
# Usage: .\run_dbt_with_seeds.ps1 [dbt_command]

# Dynamic seeds yang perlu di-update sebelum dbt run
$DynamicSeeds = @(
    "seed_daily_simulation_target",
    "seed_daily_kpi_monthly",
    "seed_energy_adjustment_daily",
    "seed_ghi_adjustment_daily"
)

# Static seeds yang TIDAK perlu di-update (hanya jika ada perubahan)
$StaticSeeds = @(
    "seed_metric_mapper"
)

Write-Host "🔄 Auto-updating dynamic seeds..." -ForegroundColor Cyan
foreach ($seed in $DynamicSeeds) {
    Write-Host "  → Updating $seed..." -ForegroundColor Yellow
    dbt seed --select $seed
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ❌ Failed to update $seed" -ForegroundColor Red
        exit 1
    }
}

Write-Host "✅ Dynamic seeds updated successfully" -ForegroundColor Green
Write-Host ""

# Run dbt command (default: dbt run)
$DbtCommand = if ($args.Count -gt 0) { $args[0] } else { "run" }
$DbtArgs = if ($args.Count -gt 1) { $args[1..($args.Count-1)] } else { @() }

Write-Host "🚀 Running dbt $DbtCommand..." -ForegroundColor Cyan
if ($DbtArgs.Count -gt 0) {
    & dbt $DbtCommand $DbtArgs
} else {
    & dbt $DbtCommand
}

