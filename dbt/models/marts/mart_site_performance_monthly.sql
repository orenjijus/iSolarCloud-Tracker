{{ config(
    materialized='view',
    schema='mart'
) }}

-- Monthly site-level performance aggregations from daily data
-- PR is calculated from monthly sums, NOT average of daily PR values
-- This matches Excel calculation methodology

WITH monthly_aggregates AS (
    SELECT 
        year,
        month,
        month_name,
        site_id,
        site_name,
        system,
        actual_capacity_kw,
        tariff,
        site_order,
        
        -- Sum metrics (additive across days)
        SUM(COALESCE(daily_energy_mwh, 0)) as monthly_energy_mwh,
        SUM(COALESCE(daily_ghi_kwh_m2, 0)) as monthly_ghi_kwh_m2,
        SUM(COALESCE(daily_poa_weighted_kwh_m2, 0)) as monthly_poa_weighted_kwh_m2,
        SUM(COALESCE(power_available_hours, 0)) as monthly_power_available_hours,
        SUM(COALESCE(unavailability_hours, 0)) as monthly_unavailability_hours,
        SUM(COALESCE(energy_target_mwh, 0)) as monthly_energy_target_mwh,
        SUM(COALESCE(ghi_target, 0)) as monthly_ghi_target,
        SUM(COALESCE(poa_target, 0)) as monthly_poa_target,
        
        -- Count days with data
        COUNT(DISTINCT date_key) as days_with_data,
        COUNT(DISTINCT CASE WHEN daily_energy_mwh IS NOT NULL AND daily_energy_mwh > 0 THEN date_key END) as days_with_energy,
        COUNT(DISTINCT CASE WHEN daily_ghi_kwh_m2 IS NOT NULL AND daily_ghi_kwh_m2 > 0 THEN date_key END) as days_with_ghi,
        COUNT(DISTINCT CASE WHEN daily_poa_weighted_kwh_m2 IS NOT NULL AND daily_poa_weighted_kwh_m2 > 0 THEN date_key END) as days_with_poa
        
    FROM {{ ref('mart_site_performance_daily') }}
    WHERE year IS NOT NULL 
        AND month IS NOT NULL
        AND site_id IS NOT NULL
    GROUP BY 
        year,
        month,
        month_name,
        site_id,
        site_name,
        system,
        actual_capacity_kw,
        tariff,
        site_order
)

SELECT 
    year,
    month,
    month_name,
    site_id,
    site_name,
    system,
    actual_capacity_kw,
    tariff,
    site_order,
    
    -- Monthly sums (matching Excel column names for comparison)
    CAST(monthly_energy_mwh AS DECIMAL(18,6)) as daily_energy_mwh,  -- Note: Excel uses "daily_energy_mwh" for monthly sum
    CAST(monthly_ghi_kwh_m2 AS DECIMAL(18,6)) as daily_ghi_kwh_m2,  -- Note: Excel uses "daily_ghi_kwh_m2" for monthly sum
    CAST(monthly_poa_weighted_kwh_m2 AS DECIMAL(18,6)) as monthly_poa_weighted_kwh_m2,
    
    -- Monthly targets
    CAST(monthly_energy_target_mwh AS DECIMAL(18,6)) as energy_target_mwh,
    CAST(monthly_ghi_target AS DECIMAL(18,6)) as ghi_target,
    CAST(monthly_poa_target AS DECIMAL(18,6)) as poa_target,
    
    -- Availability (weighted average by hours)
    CASE 
        WHEN (monthly_power_available_hours + monthly_unavailability_hours) > 0
        THEN CAST(
            (monthly_power_available_hours / NULLIF(monthly_power_available_hours + monthly_unavailability_hours, 0)) 
            AS DECIMAL(18,6)
        )
        ELSE NULL
    END as availability_percent,
    
    -- Performance Ratios calculated from monthly sums (NOT average of daily PR)
    -- PR GHI = (monthly_energy_mwh * 1000) / (monthly_ghi_kwh_m2) / actual_capacity_kw
    -- This is the correct way: calculate PR from aggregated values
    CASE 
        WHEN monthly_ghi_kwh_m2 IS NOT NULL 
            AND monthly_ghi_kwh_m2 >= 0.1  -- Minimum threshold
            AND monthly_energy_mwh IS NOT NULL
            AND monthly_energy_mwh >= 0.01  -- Minimum energy threshold
            AND actual_capacity_kw IS NOT NULL 
            AND actual_capacity_kw > 0
        THEN LEAST(
            CAST(
                ((monthly_energy_mwh * 1000.0) / NULLIF(monthly_ghi_kwh_m2, 0) / NULLIF(actual_capacity_kw, 0)) 
                AS DECIMAL(18,6)
            ),
            10.0  -- Cap at 10.0 to prevent overflow
        )
        ELSE NULL
    END as pr_ghi_actual,
    
    -- PR POA = (monthly_energy_mwh * 1000) / (monthly_poa_weighted_kwh_m2) / actual_capacity_kw
    CASE 
        WHEN monthly_poa_weighted_kwh_m2 IS NOT NULL 
            AND monthly_poa_weighted_kwh_m2 >= 0.1  -- Minimum threshold
            AND monthly_energy_mwh IS NOT NULL
            AND monthly_energy_mwh >= 0.01  -- Minimum energy threshold
            AND actual_capacity_kw IS NOT NULL 
            AND actual_capacity_kw > 0
        THEN LEAST(
            CAST(
                ((monthly_energy_mwh * 1000.0) / NULLIF(monthly_poa_weighted_kwh_m2, 0) / NULLIF(actual_capacity_kw, 0)) 
                AS DECIMAL(18,6)
            ),
            2.0  -- Cap at 2.0 to prevent overflow
        )
        ELSE NULL
    END as pr_poa_actual,
    
    -- Energy actual vs target (as decimal ratio)
    CASE 
        WHEN monthly_energy_target_mwh IS NOT NULL 
            AND monthly_energy_target_mwh > 0 
            AND monthly_energy_mwh IS NOT NULL
        THEN CAST((monthly_energy_mwh / NULLIF(monthly_energy_target_mwh, 0)) AS DECIMAL(18,6))
        ELSE NULL
    END as energy_actual_vs_target_pct,
    
    -- GHI actual vs target (as decimal ratio)
    CASE 
        WHEN monthly_ghi_target IS NOT NULL 
            AND monthly_ghi_target > 0 
            AND monthly_ghi_kwh_m2 IS NOT NULL
        THEN CAST((monthly_ghi_kwh_m2 / NULLIF(monthly_ghi_target, 0)) AS DECIMAL(18,6))
        ELSE NULL
    END as ghi_actual_vs_target_pct,
    
    -- POA actual vs target (as decimal ratio)
    CASE 
        WHEN monthly_poa_target IS NOT NULL 
            AND monthly_poa_target > 0 
            AND monthly_poa_weighted_kwh_m2 IS NOT NULL
        THEN CAST((monthly_poa_weighted_kwh_m2 / NULLIF(monthly_poa_target, 0)) AS DECIMAL(18,6))
        ELSE NULL
    END as poa_actual_vs_target_pct,
    
    -- Energy vs GHI variance (as decimal difference)
    CASE 
        WHEN monthly_energy_target_mwh IS NOT NULL 
            AND monthly_energy_target_mwh > 0 
            AND monthly_ghi_target IS NOT NULL
            AND monthly_ghi_target > 0
            AND monthly_energy_mwh IS NOT NULL
            AND monthly_ghi_kwh_m2 IS NOT NULL
        THEN CAST(
            ((monthly_energy_mwh / NULLIF(monthly_energy_target_mwh, 0)) - (monthly_ghi_kwh_m2 / NULLIF(monthly_ghi_target, 0))) 
            AS DECIMAL(18,6)
        )
        ELSE NULL
    END as energy_vs_ghi_variance_pct,
    
    -- Metadata
    days_with_data,
    days_with_energy,
    days_with_ghi,
    days_with_poa

FROM monthly_aggregates
ORDER BY year, month, site_id

