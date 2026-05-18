{{ config(
    materialized='table',
    schema='mart',
    indexes=[
        {'columns': ['date_key', 'site_code'], 'type': 'btree'},
        {'columns': ['site_code'], 'type': 'btree'},
        {'columns': ['simulation_year', 'day_of_year'], 'type': 'btree'}
    ]
) }}

-- Daily simulation 25 years with degradation for all sites (skema gabungan: date_key + simulation_year + simulation_start_year)
-- Algorithm: docs/DAILY_SIMULATION_DEGRADATION_DESIGN.md
-- Sources: seed_daily_simulation_target (year 1 profile), mart_simulation_degradation_factors, mart_simulation_site_start_year

WITH sites_with_data AS (
    SELECT DISTINCT df.site_code
    FROM {{ ref('mart_simulation_degradation_factors') }} df
    WHERE df.simulation_year = 1 AND df.energy_year1_mwh IS NOT NULL
),

daily_year1_raw AS (
    SELECT
        "Site_Code"::text AS site_code,
        "Site_Name"::text AS site_name,
        TO_DATE("Date", 'DD/MM/YYYY') AS date_key,
        EXTRACT(DOY FROM TO_DATE("Date", 'DD/MM/YYYY'))::int AS day_of_year,
        CAST(REPLACE(COALESCE("Energy Simulation (MW)"::text, '0'), ',', '.') AS NUMERIC) AS energy_simulation_mwh,
        CAST(REPLACE(COALESCE("Energy Target (MW)"::text, '0'), ',', '.') AS NUMERIC) AS energy_target_mwh,
        CAST(REPLACE(COALESCE("GHI"::text, '0'), ',', '.') AS NUMERIC) AS ghi,
        CAST(REPLACE(COALESCE("POA"::text, '0'), ',', '.') AS NUMERIC) AS poa,
        CAST(REPLACE(COALESCE("Daily PR POA Simulation"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_poa_simulation,
        CAST(REPLACE(COALESCE("Daily PR POA Target"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_poa_target,
        CAST(REPLACE(COALESCE("Daily PR GHI Simulation"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_ghi_simulation,
        CAST(REPLACE(COALESCE("Daily PR GHI Target"::text, '0'), ',', '.') AS NUMERIC) AS daily_pr_ghi_target
    FROM {{ ref('seed_daily_simulation_target') }}
),

daily_year1 AS (
    SELECT
        d.site_code,
        d.site_name,
        d.day_of_year,
        d.energy_simulation_mwh,
        d.energy_target_mwh,
        d.ghi,
        d.poa,
        d.daily_pr_poa_simulation,
        d.daily_pr_poa_target,
        d.daily_pr_ghi_simulation,
        d.daily_pr_ghi_target
    FROM daily_year1_raw d
    INNER JOIN sites_with_data s ON s.site_code = d.site_code
),

start_year AS (
    SELECT
        s.site_code,
        COALESCE(sy.simulation_start_year, 2025) AS simulation_start_year
    FROM sites_with_data s
    LEFT JOIN {{ ref('mart_simulation_site_start_year') }} sy ON sy.site_code = s.site_code
),

sim_years AS (
    SELECT generate_series(1, 25)::int AS simulation_year
),
days AS (
    SELECT generate_series(1, 365)::int AS day_of_year
),
grid AS (
    SELECT sy.simulation_year, d.day_of_year
    FROM sim_years sy
    CROSS JOIN days d
),

base AS (
    SELECT
        s.site_code,
        dy.site_name,
        g.simulation_year,
        g.day_of_year,
        sy.simulation_start_year,
        (make_date(sy.simulation_start_year + g.simulation_year - 1, 1, 1) + (g.day_of_year - 1) * interval '1 day')::date AS date_key,
        df.degradation_factor,
        dy.energy_simulation_mwh AS energy_year1_mwh,
        dy.energy_target_mwh AS energy_target_year1_mwh,
        dy.ghi,
        dy.poa,
        dy.daily_pr_poa_simulation AS pr_poa_sim_year1,
        dy.daily_pr_poa_target AS pr_poa_target_year1,
        dy.daily_pr_ghi_simulation AS pr_ghi_sim_year1,
        dy.daily_pr_ghi_target AS pr_ghi_target_year1
    FROM sites_with_data s
    CROSS JOIN grid g
    JOIN start_year sy ON sy.site_code = s.site_code
    JOIN {{ ref('mart_simulation_degradation_factors') }} df
        ON df.site_code = s.site_code AND df.simulation_year = g.simulation_year
    LEFT JOIN daily_year1 dy
        ON dy.site_code = s.site_code AND dy.day_of_year = g.day_of_year
    WHERE dy.day_of_year IS NOT NULL
)

SELECT
    site_code,
    site_name,
    date_key,
    simulation_year,
    simulation_start_year,
    day_of_year,
    degradation_factor,
    energy_year1_mwh * degradation_factor AS energy_simulation_mwh,
    energy_target_year1_mwh * degradation_factor AS energy_target_mwh,
    ghi,
    poa,
    pr_poa_sim_year1 * degradation_factor AS daily_pr_poa_simulation,
    pr_poa_target_year1 * degradation_factor AS daily_pr_poa_target,
    pr_ghi_sim_year1 * degradation_factor AS daily_pr_ghi_simulation,
    pr_ghi_target_year1 * degradation_factor AS daily_pr_ghi_target
FROM base
ORDER BY site_code, date_key
