{{ config(
    materialized='table',
    indexes=[
        {'columns': ['date_key', 'site_code'], 'type': 'btree'},
        {'columns': ['site_code'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ]
) }}

-- Daily simulation targets for performance comparison
WITH simulation_data AS (
    SELECT 
        "Site_Name" as site_name,
        "Site_Code" as site_code,
        TO_DATE("Date", 'DD/MM/YYYY') as date_key,
        "GHI"::numeric as ghi,
        "POA"::numeric as poa,
        "Energy Simulation (MW)"::numeric * 1000 as energy_simulation_mwh,
        "Energy Target (MW)"::numeric * 1000 as energy_target_mwh,
        "Daily PR POA Simulation"::numeric as daily_pr_poa_simulation,
        "Daily PR POA Target"::numeric as daily_pr_poa_target,
        "GHI vs POA"::numeric as ghi_vs_poa,
        "Daily PR GHI Simulation"::numeric as daily_pr_ghi_simulation,
        "Daily PR GHI Target"::numeric as daily_pr_ghi_target
    FROM {{ ref('seed_daily_simulation_target') }}
)
SELECT 
    sd.date_key,
    dd.year,
    dd.month,
    dd.month_name,
    dd.season,
    dd.day_type,
    sd.site_name,
    sd.site_code,
    da.asset_id as site_id,
    da.site_id as site_code_from_assets,
    da.system,
    sd.ghi,
    sd.poa,
    sd.ghi_vs_poa,
    sd.energy_simulation_mwh,
    sd.energy_target_mwh,
    sd.energy_target_mwh - sd.energy_simulation_mwh as energy_variance_mwh,
    sd.daily_pr_poa_simulation,
    sd.daily_pr_poa_target,
    sd.daily_pr_poa_target - sd.daily_pr_poa_simulation as pr_poa_variance,
    sd.daily_pr_ghi_simulation,
    sd.daily_pr_ghi_target,
    sd.daily_pr_ghi_target - sd.daily_pr_ghi_simulation as pr_ghi_variance
FROM simulation_data sd
LEFT JOIN {{ ref('dim_assets') }} da 
    ON (da.site_id::text = sd.site_code::text OR da.site_name = sd.site_name)
    AND da.asset_level = 'Site'
LEFT JOIN {{ ref('dim_date_generated') }} dd
    ON sd.date_key = dd.date_key

