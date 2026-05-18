{{ config(
    materialized='table',
    schema='dimensions',
    unique_key='asset_id'
) }}

-- Subset of dim_assets for Site level only.
-- Source of truth: raw tables (isolarcloud_power_stations, fusionsolar_plants)
--   → staging (stg_*__sites) → dim_assets → dim_site.
-- Used by tools (cleaning log, weekly log) for site dropdown.
-- After updating raw site/device data, run: dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ dim_assets dim_site
SELECT
    da.asset_id,
    da.site_id,
    da.site_id::text AS site_code,
    da.site_name,
    da.site_name_clean,
    da.system,
    da.actual_capacity_kw,
    da.tariff,
    da.site_order,
    da.calculation_start_date,
    da.total_inverters,
    da.latitude,
    da.longitude
FROM {{ ref('dim_assets') }} da
WHERE da.asset_level = 'Site'
