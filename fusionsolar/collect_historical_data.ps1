# PowerShell script to collect historical data for all FusionSolar sites
# This script processes one site at a time to stay within API rate limits

$START_DATE = "2025-05-01"
$END_DATE = "2025-05-27"
$PYTHON_PATH = "python"  # Change this if you're using a specific Python environment

# Define sites and their device counts
$SITES = @(
    @{
        "name" = "MMKI 1";
        "code" = "NE=50488260";
        "inverters" = 10;
        "sensors" = 3;
        "meters" = 2;
    },
    @{
        "name" = "MMKI 2";
        "code" = "NE=51758766";
        "inverters" = 17;
        "sensors" = 9;
        "meters" = 11;
    },
    @{
        "name" = "Mall Panakkukang";
        "code" = "NE=53771627";
        "inverters" = 10;
        "sensors" = 10;
        "meters" = 4;
    },
    @{
        "name" = "Pusan Manis";
        "code" = "NE=54435794";
        "inverters" = 14;
        "sensors" = 18;
        "meters" = 5;
    }
)

# Function to collect data for a specific site
function Collect-SiteData {
    param (
        [string]$SiteName,
        [string]$SiteCode,
        [int]$InverterCount,
        [int]$SensorCount,
        [int]$MeterCount
    )
    
    Write-Host "============================================================"
    Write-Host "Starting data collection for $SiteName ($SiteCode)"
    Write-Host "Devices: $InverterCount inverters, $SensorCount sensors, $MeterCount meters"
    Write-Host "Date range: $START_DATE to $END_DATE"
    Write-Host "============================================================"
    
    # Calculate expected API calls
    $inverterBatches = [Math]::Ceiling($InverterCount / 10)
    $sensorBatches = [Math]::Ceiling($SensorCount / 10)
    $meterBatches = [Math]::Ceiling($MeterCount / 10)
    $totalCalls = ($inverterBatches + $sensorBatches + $meterBatches) * 9  # 9 date windows for 27 days
    
    Write-Host "Estimated API calls: $totalCalls"
    Write-Host "Daily limit: $([Math]::Ceiling(($InverterCount + $SensorCount + $MeterCount) / 10) + 24)"
    
    # Run the data harvester with rate limiting for this site
    $command = "$PYTHON_PATH d:\CAREER\Solar Radiance\iSolarCloud-Tracker\fusionsolar\fusionsolar_data_harvester.py --fetch-historical $START_DATE $END_DATE --plant-codes $SiteCode --device-types inverter,meter,meteo_station"
    Write-Host "Executing: $command"
    Invoke-Expression $command
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error collecting data for $SiteName. Please check the logs." -ForegroundColor Red
        return $false
    }
    
    Write-Host "Successfully collected data for $SiteName" -ForegroundColor Green
    return $true
}

# Main execution loop
$overallSuccess = $true
foreach ($site in $SITES) {
    $success = Collect-SiteData -SiteName $site.name -SiteCode $site.code `
                               -InverterCount $site.inverters -SensorCount $site.sensors -MeterCount $site.meters
    
    if (-not $success) {
        $overallSuccess = $false
        Write-Host "Stopping collection due to error with site: $($site.name)" -ForegroundColor Red
        break
    }
    
    # Add a small delay between sites to avoid any potential issues
    Write-Host "Waiting 30 seconds before proceeding to next site..."
    Start-Sleep -Seconds 30
}

if ($overallSuccess) {
    Write-Host "=================================================================="
    Write-Host "Historical data collection completed successfully for all sites!" -ForegroundColor Green
    Write-Host "=================================================================="
} else {
    Write-Host "=================================================================="
    Write-Host "Historical data collection incomplete. Please check the logs." -ForegroundColor Yellow
    Write-Host "=================================================================="
}
