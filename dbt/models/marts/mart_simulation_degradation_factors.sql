{{ config(
    materialized='table',
    schema='mart',
    indexes=[
        {'columns': ['site_code', 'simulation_year'], 'type': 'btree'},
        {'columns': ['site_code'], 'type': 'btree'}
    ]
) }}

-- Normalised yearly energy and degradation factor per (site_code, simulation_year)
-- Source: seed_yearly_simulation_target. Supports: semicolon delimiter + dot decimal (e.g. 930.604), or comma decimal.
-- degradation_factor = Energy Year n / Energy Year 1 (per site)

WITH yearly_raw AS (
    SELECT
        "Site_Code"::text AS site_code,
        "Site_Name"::text AS site_name,
        "Year"::int AS simulation_year,
        -- Energy TS: dot decimal (930.604) -> CAST; comma decimal (930,604) -> REPLACE
        CASE
            WHEN "Energy TS (MWh)"::text LIKE '%,%' THEN CAST(REPLACE(REPLACE(TRIM("Energy TS (MWh)"::text), ' ', ''), ',', '.') AS NUMERIC)
            ELSE CAST(REPLACE(COALESCE("Energy TS (MWh)"::text, '0'), ',', '.') AS NUMERIC)
        END AS energy_yearly_mwh
    FROM {{ ref('seed_yearly_simulation_target') }}
),

yearly_with_energy AS (
    SELECT
        site_code,
        site_name,
        simulation_year,
        CASE
            WHEN energy_yearly_mwh IS NULL OR energy_yearly_mwh <= 0 THEN NULL
            ELSE energy_yearly_mwh
        END AS energy_yearly_mwh
    FROM yearly_raw
),

year_1_energy AS (
    SELECT
        site_code,
        energy_yearly_mwh AS energy_year1_mwh
    FROM yearly_with_energy
    WHERE simulation_year = 1 AND energy_yearly_mwh IS NOT NULL
),

degradation AS (
    SELECT
        y.site_code,
        y.site_name,
        y.simulation_year,
        y.energy_yearly_mwh,
        e.energy_year1_mwh,
        CASE
            WHEN e.energy_year1_mwh IS NOT NULL AND e.energy_year1_mwh > 0 AND y.energy_yearly_mwh IS NOT NULL
            THEN y.energy_yearly_mwh / e.energy_year1_mwh
            ELSE 1
        END AS degradation_factor
    FROM yearly_with_energy y
    LEFT JOIN year_1_energy e ON e.site_code = y.site_code
)

SELECT
    site_code,
    site_name,
    simulation_year,
    energy_yearly_mwh,
    energy_year1_mwh,
    degradation_factor
FROM degradation
WHERE energy_yearly_mwh IS NOT NULL
ORDER BY site_code, simulation_year
