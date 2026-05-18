# PowerShell script untuk menjalankan Prefect flow manual
# Usage: .\run_flow_manual.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "MMSR Daily Pipeline - Manual Test" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Change to prefect directory
Set-Location $PSScriptRoot

# Activate virtual environment if exists
if (Test-Path "..\venv\Scripts\Activate.ps1") {
    Write-Host "Activating virtual environment..." -ForegroundColor Yellow
    & "..\venv\Scripts\Activate.ps1"
}

Write-Host "Running MMSR daily pipeline flow..." -ForegroundColor Green
Write-Host ""

# Run the flow
try {
    python flows\mmsr_daily_pipeline.py
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "✅ Flow execution completed successfully!" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "❌ Flow execution failed with exit code: $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} catch {
    Write-Host ""
    Write-Host "❌ Error running flow: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Press any key to continue..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

