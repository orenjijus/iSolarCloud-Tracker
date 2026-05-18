{{ config(
    materialized='table',
    schema='mart',
    indexes=[{'columns': ['site_code'], 'type': 'btree'}]
) }}

-- simulation_start_year per site from raw: grid connection date (FusionSolar) or install_date (iSolarCloud)
-- Used by mart_simulation_daily_25y. One row per site.
-- FusionSolar: raw.fusionsolar_plants.grid_connection_date via stg_fusionsolar__sites
-- iSolarCloud: raw.isolarcloud_power_stations.install_date via stg_isolarcloud__sites

WITH isolarcloud_start AS (
    SELECT
        ps_id::text AS site_code,
        COALESCE(EXTRACT(YEAR FROM install_date)::int, 2025) AS simulation_start_year
    FROM {{ ref('stg_isolarcloud__sites') }}
),

fusionsolar_start AS (
    SELECT
        plant_code::text AS site_code,
        COALESCE(EXTRACT(YEAR FROM grid_connection_date)::int, 2025) AS simulation_start_year
    FROM {{ ref('stg_fusionsolar__sites') }}
)

SELECT site_code, simulation_start_year FROM isolarcloud_start
UNION ALL
SELECT site_code, simulation_start_year FROM fusionsolar_start
