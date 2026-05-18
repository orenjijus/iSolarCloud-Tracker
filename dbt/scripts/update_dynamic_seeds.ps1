# PowerShell script untuk update dynamic seeds yang perlu update berkala
# Usage: .\update_dynamic_seeds.ps1 [seed_name]

param(
    [string]$SeedName = ""
)

$ErrorActionPreference = "Stop"

# Change to dbt directory
$dbtDir = Split-Path -Parent $PSScriptRoot
Set-Location $dbtDir

Write-Host "🔄 Updating Dynamic Seeds..." -ForegroundColor Cyan

# Dynamic seeds yang perlu update berkala
$dynamicSeeds = @(
    "seed_daily_simulation_target",
    "seed_daily_kpi_monthly",
    "seed_energy_adjustment_daily",
    "seed_ghi_adjustment_daily"
)

# Jika ada argument, update seed tertentu saja
if ($SeedName -ne "") {
    Write-Host "📦 Updating seed: $SeedName" -ForegroundColor Yellow
    dbt seed --select $SeedName
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to update seed: $SeedName" -ForegroundColor Red
        exit 1
    }
} else {
    # Update semua dynamic seeds
    Write-Host "📦 Updating all dynamic seeds..." -ForegroundColor Yellow
    foreach ($seed in $dynamicSeeds) {
        Write-Host "  - $seed" -ForegroundColor Gray
        dbt seed --select $seed
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Failed to update seed: $seed" -ForegroundColor Red
            exit 1
        }
    }
}

Write-Host "✅ Dynamic seeds update completed!" -ForegroundColor Green

