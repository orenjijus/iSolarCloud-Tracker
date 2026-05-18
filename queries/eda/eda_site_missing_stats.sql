-- EDA Site PLTS: Missing values per key column
-- Purpose: Assess completeness for ML feature/target selection.

SELECT
    COUNT(*) AS total_rows,
    COUNT(daily_energy_mwh)     AS n_energy,
    COUNT(*) - COUNT(daily_energy_mwh)     AS null_energy,
    COUNT(daily_ghi_kwh_m2)     AS n_ghi,
    COUNT(*) - COUNT(daily_ghi_kwh_m2)     AS null_ghi,
    COUNT(daily_poa_weighted_kwh_m2) AS n_poa,
    COUNT(*) - COUNT(daily_poa_weighted_kwh_m2) AS null_poa,
    COUNT(availability_percent) AS n_availability,
    COUNT(*) - COUNT(availability_percent) AS null_availability,
    COUNT(pr_ghi_actual)        AS n_pr_ghi,
    COUNT(*) - COUNT(pr_ghi_actual)        AS null_pr_ghi,
    COUNT(pr_poa_actual)        AS n_pr_poa,
    COUNT(*) - COUNT(pr_poa_actual)        AS null_pr_poa,
    COUNT(energy_target_mwh)    AS n_target,
    COUNT(*) - COUNT(energy_target_mwh)    AS null_target
FROM mart.mart_site_performance_daily;

-- Missing per site (for key target/feature columns)
SELECT
    site_id,
    site_name,
    COUNT(*) AS total_days,
    COUNT(daily_energy_mwh) AS has_energy,
    COUNT(daily_ghi_kwh_m2) AS has_ghi,
    COUNT(availability_percent) AS has_availability,
    COUNT(pr_ghi_actual) AS has_pr_ghi,
    COUNT(pr_poa_actual) AS has_pr_poa,
    COUNT(energy_target_mwh) AS has_target
FROM mart.mart_site_performance_daily
GROUP BY site_id, site_name
ORDER BY total_days DESC;
