-- EDA Site PLTS: Numeric summary (avg/min/max/stddev) global and per site
-- Purpose: Understand scale and spread for ML scaling/outlier handling.

-- Global stats for key numeric columns
SELECT
    ROUND(AVG(daily_energy_mwh)::numeric, 4) AS avg_energy_mwh,
    MIN(daily_energy_mwh) AS min_energy_mwh,
    MAX(daily_energy_mwh) AS max_energy_mwh,
    ROUND(STDDEV(daily_energy_mwh)::numeric, 4) AS stddev_energy_mwh,
    ROUND(AVG(daily_ghi_kwh_m2)::numeric, 4) AS avg_ghi,
    MIN(daily_ghi_kwh_m2) AS min_ghi,
    MAX(daily_ghi_kwh_m2) AS max_ghi,
    ROUND(STDDEV(daily_ghi_kwh_m2)::numeric, 4) AS stddev_ghi,
    ROUND(AVG(availability_percent)::numeric, 4) AS avg_availability,
    MIN(availability_percent) AS min_availability,
    MAX(availability_percent) AS max_availability,
    ROUND(AVG(pr_ghi_actual)::numeric, 4) AS avg_pr_ghi,
    MIN(pr_ghi_actual) AS min_pr_ghi,
    MAX(pr_ghi_actual) AS max_pr_ghi,
    ROUND(AVG(pr_poa_actual)::numeric, 4) AS avg_pr_poa,
    MIN(pr_poa_actual) AS min_pr_poa,
    MAX(pr_poa_actual) AS max_pr_poa,
    ROUND(AVG(energy_target_mwh)::numeric, 4) AS avg_target_mwh,
    MIN(energy_target_mwh) AS min_target_mwh,
    MAX(energy_target_mwh) AS max_target_mwh
FROM mart.mart_site_performance_daily;

-- Per-site stats (energy and PR only for brevity)
SELECT
    site_id,
    site_name,
    system,
    COUNT(*) AS n,
    ROUND(AVG(daily_energy_mwh)::numeric, 4) AS avg_energy_mwh,
    ROUND(AVG(pr_ghi_actual)::numeric, 4) AS avg_pr_ghi,
    ROUND(AVG(availability_percent)::numeric, 4) AS avg_availability
FROM mart.mart_site_performance_daily
GROUP BY site_id, site_name, system
ORDER BY avg_energy_mwh DESC NULLS LAST;
