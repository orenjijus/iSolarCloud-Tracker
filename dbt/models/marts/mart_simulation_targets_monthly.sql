{{ config(
    materialized='table',
    schema='mart',
    indexes=[
        {'columns': ['year', 'month', 'site_code'], 'type': 'btree'},
        {'columns': ['site_code'], 'type': 'btree'},
        {'columns': ['year', 'month'], 'type': 'btree'}
    ]
) }}

-- Monthly simulation targets aggregated from daily data
-- mart_simulation_targets_daily sources from seed_daily_simulation_target
-- Aggregates mart_simulation_targets_daily to monthly level
-- Energy, GHI, and POA values are summed; PR and GHI vs POA ratios are averaged
WITH monthly_targets AS (
    SELECT
        DATE_TRUNC('month', mstd.date_key)::DATE as month_date_key,
        mstd.year,
        mstd.month,
        mstd.month_name,
        mstd.site_name,
        mstd.site_code,
        mstd.asset_id,
        mstd.system,
        -- Sum GHI and POA (monthly totals)
        SUM(mstd.ghi) as ghi,
        SUM(mstd.poa) as poa,
        -- Average GHI vs POA ratio
        AVG(mstd.ghi_vs_poa) as ghi_vs_poa_avg,
        -- Sum energy values (MWh)
        SUM(mstd.energy_simulation_mwh) as energy_simulation_mwh,
        SUM(mstd.energy_target_mwh) as energy_target_mwh,
        SUM(mstd.energy_variance_mwh) as energy_variance_mwh,
        -- Average PR values
        AVG(mstd.daily_pr_poa_simulation) as monthly_pr_poa_simulation,
        AVG(mstd.daily_pr_poa_target) as monthly_pr_poa_target,
        AVG(mstd.pr_poa_variance) as pr_poa_variance,
        AVG(mstd.daily_pr_ghi_simulation) as monthly_pr_ghi_simulation,
        AVG(mstd.daily_pr_ghi_target) as monthly_pr_ghi_target,
        AVG(mstd.pr_ghi_variance) as pr_ghi_variance,
        -- Additional metrics
        COUNT(*) as days_count
    FROM {{ ref('mart_simulation_targets_daily') }} mstd
    WHERE mstd.date_key IS NOT NULL
        AND mstd.site_code IS NOT NULL
    GROUP BY
        DATE_TRUNC('month', mstd.date_key)::DATE,
        mstd.year,
        mstd.month,
        mstd.month_name,
        mstd.site_name,
        mstd.site_code,
        mstd.asset_id,
        mstd.system
),
kpi_by_site_code AS (
    -- Primary mapping: site_code (target model) to site_id (KPI model)
    -- MAX() provides deterministic deduping if source has repeated rows per key.
    SELECT
        mkpi.site_id::text as site_code,
        mkpi.year,
        mkpi.month,
        MAX(mkpi.energy_kpi_mwh) as energy_kpi_mwh
    FROM {{ ref('mart_site_kpi_monthly') }} mkpi
    WHERE mkpi.site_id IS NOT NULL
    GROUP BY
        mkpi.site_id::text,
        mkpi.year,
        mkpi.month
),
kpi_by_site_name AS (
    -- Fallback mapping when site code is unavailable/mismatched.
    SELECT
        LOWER(TRIM(mkpi.site_name)) as site_name_key,
        mkpi.year,
        mkpi.month,
        MAX(mkpi.energy_kpi_mwh) as energy_kpi_mwh
    FROM {{ ref('mart_site_kpi_monthly') }} mkpi
    WHERE mkpi.site_name IS NOT NULL
    GROUP BY
        LOWER(TRIM(mkpi.site_name)),
        mkpi.year,
        mkpi.month
)
SELECT
    mt.month_date_key,
    mt.year,
    mt.month,
    mt.month_name,
    mt.site_name,
    mt.site_code,
    mt.asset_id,
    mt.system,
    mt.ghi,
    mt.poa,
    mt.ghi_vs_poa_avg,
    mt.energy_simulation_mwh,
    mt.energy_target_mwh,
    mt.energy_variance_mwh,
    -- Monthly KPI value sourced from mart_site_kpi_monthly.
    COALESCE(kc.energy_kpi_mwh, kn.energy_kpi_mwh) as energy_kpi_mwh,
    -- Safe KPI/target ratio: return NULL when denominator is NULL or zero.
    CASE
        WHEN mt.energy_target_mwh IS NULL OR mt.energy_target_mwh = 0 THEN NULL
        ELSE ROUND(
            (COALESCE(kc.energy_kpi_mwh, kn.energy_kpi_mwh)::numeric / mt.energy_target_mwh::numeric),
            6
        )
    END as energy_kpi_gap_ratio,
    mt.monthly_pr_poa_simulation,
    mt.monthly_pr_poa_target,
    mt.pr_poa_variance,
    mt.monthly_pr_ghi_simulation,
    mt.monthly_pr_ghi_target,
    mt.pr_ghi_variance,
    mt.days_count,
    NOW() as created_at,
    NOW() as updated_at
FROM monthly_targets mt
LEFT JOIN kpi_by_site_code kc
    ON kc.site_code = mt.site_code::text
    AND kc.year = mt.year
    AND kc.month = mt.month
LEFT JOIN kpi_by_site_name kn
    ON kn.site_name_key = LOWER(TRIM(mt.site_name))
    AND kn.year = mt.year
    AND kn.month = mt.month
    AND kc.site_code IS NULL
ORDER BY
    mt.site_code,
    mt.year,
    mt.month

