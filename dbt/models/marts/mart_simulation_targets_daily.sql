{{ config(
    materialized='table',
    schema='mart',
    indexes=[
        {'columns': ['date_key', 'site_code'], 'type': 'btree'},
        {'columns': ['site_code'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ]
) }}

-- Daily simulation targets for performance comparison
-- Source: seed_daily_simulation_target (cara lama). Pipeline 25y (mart_simulation_daily_25y) tetap ada tapi tidak dipakai di sini.
WITH simulation_staged AS (
    SELECT
        "Site_Name"::text AS site_name,
        "Site_Code"::text AS site_code,
        TO_DATE(TRIM("Date"::text), 'DD/MM/YYYY') AS date_key,
        CAST(REPLACE(COALESCE("GHI"::text, '0'), ',', '.') AS NUMERIC) AS ghi,
        CAST(REPLACE(COALESCE("POA"::text, '0'), ',', '.') AS NUMERIC) AS poa,
        CAST(REPLACE(COALESCE("GHI"::text, '0'), ',', '.') AS NUMERIC) - CAST(REPLACE(COALESCE("POA"::text, '0'), ',', '.') AS NUMERIC) AS ghi_vs_poa,
        CAST(REPLACE(COALESCE("Energy Simulation (MW)"::text, '0'), ',', '.') AS NUMERIC) AS energy_simulation_mwh,
        CAST(REPLACE(COALESCE("Energy Target (MW)"::text, '0'), ',', '.') AS NUMERIC) AS energy_target_mwh,
        CAST(REPLACE(COALESCE("Daily PR POA Simulation"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_poa_simulation,
        CAST(REPLACE(COALESCE("Daily PR POA Target"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_poa_target,
        CAST(REPLACE(COALESCE("Daily PR GHI Simulation"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_ghi_simulation,
        CAST(REPLACE(COALESCE("Daily PR GHI Target"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_ghi_target
    FROM {{ ref('seed_daily_simulation_target') }}
    WHERE TRIM(COALESCE("Site_Code"::text, '')) <> ''
        AND TRIM(COALESCE("Date"::text, '')) <> ''
),
simulation_data AS (
    SELECT
        site_name,
        site_code,
        date_key,
        ghi,
        poa,
        ghi_vs_poa,
        energy_simulation_mwh,
        energy_target_mwh,
        daily_pr_poa_simulation,
        daily_pr_poa_target,
        daily_pr_ghi_simulation,
        daily_pr_ghi_target
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY site_code, date_key
                ORDER BY site_name
            ) AS _dedupe_rn
        FROM simulation_staged s
    ) d
    WHERE _dedupe_rn = 1
),
daily_with_date_dim AS (
    SELECT
        sd.*,
        dd.year,
        dd.month,
        dd.month_name,
        dd.day_type
    FROM simulation_data sd
    LEFT JOIN {{ ref('dim_date_generated') }} dd
        ON sd.date_key = dd.date_key
),
monthly_targets AS (
    -- Calculate monthly target from daily data to avoid circular dependency
    SELECT 
        site_code,
        year,
        month,
        SUM(energy_target_mwh) as energy_target_monthly_mwh
    FROM daily_with_date_dim
    WHERE year IS NOT NULL AND month IS NOT NULL
    GROUP BY site_code, year, month
),
monthly_simulation AS (
    -- Calculate monthly simulation from daily data (includes unavailibility dates)
    SELECT 
        site_code,
        year,
        month,
        SUM(energy_simulation_mwh) as energy_simulation_monthly_mwh
    FROM daily_with_date_dim
    WHERE year IS NOT NULL AND month IS NOT NULL
    GROUP BY site_code, year, month
)
SELECT 
    sd.date_key,
    sd.year,
    sd.month,
    sd.month_name,
    sd.day_type,
    sd.site_name,
    sd.site_code,
    da.asset_id,
    da.system,
    sd.ghi,
    sd.poa,
    sd.ghi_vs_poa,
    sd.energy_simulation_mwh,
    sd.energy_target_mwh,
    sd.energy_target_mwh - sd.energy_simulation_mwh as energy_variance_mwh,
    -- Daily KPI calculation: Monthly KPI * (Daily Target / Monthly Target)
    -- Formula: KPI Daily = KPI Monthly × (Target Daily / Target Monthly)
    CASE 
        WHEN mt.energy_target_monthly_mwh > 0 AND mkpi.energy_kpi_mwh IS NOT NULL
        THEN mkpi.energy_kpi_mwh * (sd.energy_target_mwh / mt.energy_target_monthly_mwh)
        ELSE NULL
    END as energy_kpi_daily_mwh,
    -- Daily Adjusted KPI calculation: Target Daily * (KPI Monthly / Simulation Monthly)
    -- Formula: a_KPI Daily = Target Daily × (KPI Monthly / Simulation Monthly)
    -- This represents KPI adjusted to exclude unavailibility dates
    CASE 
        WHEN ms.energy_simulation_monthly_mwh > 0 AND mkpi.energy_kpi_mwh IS NOT NULL
        THEN sd.energy_target_mwh * (mkpi.energy_kpi_mwh / ms.energy_simulation_monthly_mwh)
        ELSE NULL
    END as energy_a_kpi_daily_mwh,
    mkpi.energy_kpi_mwh as energy_kpi_monthly_mwh,
    mt.energy_target_monthly_mwh as energy_target_monthly_mwh,
    ms.energy_simulation_monthly_mwh as energy_simulation_monthly_mwh,
    sd.daily_pr_poa_simulation,
    sd.daily_pr_poa_target,
    sd.daily_pr_poa_target - sd.daily_pr_poa_simulation as pr_poa_variance,
    sd.daily_pr_ghi_simulation,
    sd.daily_pr_ghi_target,
    sd.daily_pr_ghi_target - sd.daily_pr_ghi_simulation as pr_ghi_variance
FROM daily_with_date_dim sd
LEFT JOIN {{ ref('dim_assets') }} da 
    ON (da.site_id::text = sd.site_code::text OR da.site_name = sd.site_name)
    AND da.asset_level = 'Site'
LEFT JOIN {{ ref('mart_site_kpi_monthly') }} mkpi
    ON (mkpi.site_id::text = sd.site_code::text OR mkpi.site_name = sd.site_name)
    AND mkpi.year = sd.year
    AND mkpi.month = sd.month
LEFT JOIN monthly_targets mt
    ON mt.site_code = sd.site_code
    AND mt.year = sd.year
    AND mt.month = sd.month
LEFT JOIN monthly_simulation ms
    ON ms.site_code = sd.site_code
    AND ms.year = sd.year
    AND ms.month = sd.month

