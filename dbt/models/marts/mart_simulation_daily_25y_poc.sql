{{ config(
    materialized='table',
    schema='mart',
    indexes=[
        {'columns': ['date_key', 'site_code'], 'type': 'btree'},
        {'columns': ['simulation_year', 'day_of_year'], 'type': 'btree'}
    ]
) }}

-- PoC: Daily simulation 25 years with degradation for ONE site (Garuda Metalindo 1, site_code 1458125)
-- Validates algorithm from docs/DAILY_SIMULATION_DEGRADATION_DESIGN.md before rolling out to all sites.
-- Requires: seed_yearly_simulation_target (enable in dbt_project.yml for this test), seed_daily_simulation_target, stg_isolarcloud__sites

WITH site_filter AS (
    SELECT '1458125'::text AS site_code
),

-- 1) Yearly energy and degradation factor (Energy Year n / Energy Year 1)
yearly_raw AS (
    SELECT
        "Site_Code"::text AS site_code,
        "Year"::int AS simulation_year,
        CAST(REPLACE(REPLACE(COALESCE("Energy TS (MWh)"::text, '0'), ' ', ''), ',', '.') AS NUMERIC) AS energy_yearly_mwh
    FROM {{ ref('seed_yearly_simulation_target') }}
    WHERE "Site_Code"::text = (SELECT site_code FROM site_filter)
),

yearly_energy_1 AS (
    SELECT site_code, energy_yearly_mwh AS energy_year1_mwh
    FROM yearly_raw
    WHERE simulation_year = 1
),

degradation_factors AS (
    SELECT
        y.site_code,
        y.simulation_year,
        y.energy_yearly_mwh,
        e.energy_year1_mwh,
        CASE WHEN e.energy_year1_mwh > 0 THEN y.energy_yearly_mwh / e.energy_year1_mwh ELSE 1 END AS degradation_factor
    FROM yearly_raw y
    JOIN yearly_energy_1 e ON e.site_code = y.site_code
),

-- 2) Daily year-1 profile (day_of_year -> energy, target, GHI, POA, PR)
daily_year1_raw AS (
    SELECT
        "Site_Code"::text AS site_code,
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
    WHERE "Site_Code"::text = (SELECT site_code FROM site_filter)
),

daily_year1 AS (
    SELECT
        site_code,
        day_of_year,
        energy_simulation_mwh,
        energy_target_mwh,
        ghi,
        poa,
        daily_pr_poa_simulation,
        daily_pr_poa_target,
        daily_pr_ghi_simulation,
        daily_pr_ghi_target
    FROM daily_year1_raw
),

-- 3) simulation_start_year from raw (grid connection date): fusionsolar_plants.grid_connection_date or isolarcloud_power_stations.install_date
start_year AS (
    SELECT
        s.site_code,
        COALESCE(EXTRACT(YEAR FROM i.install_date)::int, 2025) AS simulation_start_year
    FROM site_filter s
    LEFT JOIN {{ ref('stg_isolarcloud__sites') }} i ON i.ps_id::text = s.site_code
),

-- 4) Cartesian: simulation_year 1..25, day_of_year 1..365
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

-- 5) Build rows: date_key, energy = year1 * degradation_factor, PR = year1 * degradation_factor, GHI/POA = copy year1
base AS (
    SELECT
        (SELECT site_code FROM site_filter) AS site_code,
        g.simulation_year,
        g.day_of_year,
        sy.simulation_start_year,
        make_date(sy.simulation_start_year + g.simulation_year - 1, 1, 1)::date + (g.day_of_year - 1) * interval '1 day' AS date_key,
        df.degradation_factor,
        dy.energy_simulation_mwh AS energy_year1_mwh,
        dy.energy_target_mwh AS energy_target_year1_mwh,
        dy.ghi,
        dy.poa,
        dy.daily_pr_poa_simulation AS pr_poa_sim_year1,
        dy.daily_pr_poa_target AS pr_poa_target_year1,
        dy.daily_pr_ghi_simulation AS pr_ghi_sim_year1,
        dy.daily_pr_ghi_target AS pr_ghi_target_year1
    FROM grid g
    CROSS JOIN start_year sy
    JOIN degradation_factors df ON df.simulation_year = g.simulation_year AND df.site_code = sy.site_code
    LEFT JOIN daily_year1 dy ON dy.day_of_year = g.day_of_year AND dy.site_code = sy.site_code
    WHERE dy.day_of_year IS NOT NULL
)

SELECT
    site_code,
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
ORDER BY date_key
