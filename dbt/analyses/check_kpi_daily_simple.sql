-- Simple check: count rows with KPI daily values
SELECT 
    COUNT(*) as total_rows,
    COUNT(energy_kpi_daily_mwh) as rows_with_kpi_daily,
    COUNT(energy_kpi_monthly_mwh) as rows_with_kpi_monthly,
    COUNT(energy_target_monthly_mwh) as rows_with_target_monthly,
    COUNT(energy_target_mwh) as rows_with_target_daily
FROM {{ ref('mart_site_performance_daily') }}

-- Sample data with KPI daily
SELECT 
    date_key,
    site_name,
    energy_target_mwh,
    energy_kpi_daily_mwh,
    energy_kpi_monthly_mwh,
    energy_target_monthly_mwh
FROM {{ ref('mart_site_performance_daily') }}
WHERE energy_kpi_daily_mwh IS NOT NULL
ORDER BY date_key DESC, site_name
LIMIT 10

