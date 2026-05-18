-- EDA Site PLTS: Join to dim_assets (site-level attributes)
-- Purpose: Check capacity, system, lat/lon completeness for ML features.

SELECT
    d.site_id,
    d.site_name,
    d.system,
    d.actual_capacity_kw,
    d.tariff,
    d.site_order,
    d.latitude,
    d.longitude,
    d.calculation_start_date,
    d.total_inverters,
    (SELECT COUNT(*) FROM mart.mart_site_performance_daily m WHERE m.site_id = d.site_id) AS performance_days
FROM dimensions.dim_assets d
WHERE d.asset_level = 'Site'
ORDER BY d.site_order NULLS LAST, d.site_name;

-- Completeness: how many sites have lat/lon, capacity, etc.
SELECT
    COUNT(*) AS total_sites,
    COUNT(actual_capacity_kw) AS has_capacity,
    COUNT(latitude) AS has_lat,
    COUNT(longitude) AS has_lon,
    COUNT(tariff) AS has_tariff
FROM dimensions.dim_assets
WHERE asset_level = 'Site';
