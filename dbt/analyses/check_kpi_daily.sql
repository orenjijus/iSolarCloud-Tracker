-- Check if KPI daily values are populated correctly
SELECT 
    date_key,
    site_id,
    site_name,
    energy_target_mwh,
    energy_kpi_daily_mwh,
    energy_kpi_monthly_mwh,
    energy_target_monthly_mwh,
    -- Verify calculation: should match energy_kpi_daily_mwh
    CASE 
        WHEN energy_target_monthly_mwh > 0 AND energy_kpi_monthly_mwh IS NOT NULL
        THEN energy_kpi_monthly_mwh * (energy_target_mwh / energy_target_monthly_mwh)
        ELSE NULL
    END as calculated_kpi_daily,
    -- Check if calculation matches
    CASE 
        WHEN energy_kpi_daily_mwh IS NOT NULL 
            AND energy_target_monthly_mwh > 0 
            AND energy_kpi_monthly_mwh IS NOT NULL
        THEN ABS(energy_kpi_daily_mwh - (energy_kpi_monthly_mwh * (energy_target_mwh / energy_target_monthly_mwh)))
        ELSE NULL
    END as calculation_diff
FROM {{ ref('mart_site_performance_daily') }}
WHERE energy_kpi_daily_mwh IS NOT NULL
ORDER BY date_key DESC, site_name
LIMIT 50

