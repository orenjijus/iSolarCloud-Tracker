{{ config(
    materialized='table',
    schema='mart',
    indexes=[
        {'columns': ['site_id', 'year', 'month'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'},
        {'columns': ['year', 'month'], 'type': 'btree'}
    ]
) }}

-- Monthly KPI values for site performance comparison
-- Source: seed_daily_kpi_monthly
-- Purpose: Provides monthly KPI values for calculating Actual vs KPI Period %
-- 
-- Formula: Actual vs KPI Period % = Actual Period / (Target Period × (Monthly KPI / Monthly Target Sum)) × 100
-- This table provides the Monthly KPI value needed for the calculation
WITH kpi_data AS (
    SELECT 
        "site_Name" as site_name,
        site_id as site_id,
        CAST("Year" AS INTEGER) as year,
        "Month" as month_name,
        CAST("Month_Number" AS INTEGER) as month,
        -- Handle both comma and period decimal separators, and remove quotes from values
        CAST(REPLACE(REPLACE(COALESCE("Energy_KPI_MWh"::text, '0'), ',', '.'), '"', '') AS NUMERIC) as energy_kpi_mwh
    FROM {{ ref('seed_daily_kpi_monthly') }}
    WHERE "Energy_KPI_MWh" IS NOT NULL 
        AND "Energy_KPI_MWh"::text != ''
        AND "Energy_KPI_MWh"::text != '""'
        AND TRIM(REPLACE("Energy_KPI_MWh"::text, '"', '')) != ''
)
SELECT 
    kd.site_id,
    kd.site_name,
    kd.year,
    kd.month,
    kd.month_name,
    kd.energy_kpi_mwh,
    NOW() as created_at,
    NOW() as updated_at
FROM kpi_data kd
WHERE kd.energy_kpi_mwh > 0
    AND kd.month BETWEEN 1 AND 12
    AND kd.year > 2020  -- Basic validation
ORDER BY kd.site_id, kd.year, kd.month

