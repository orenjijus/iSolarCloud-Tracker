-- One-off patch: align daily_energy and GHI with mart_simulation_targets_daily
-- for 2026-04-26 and the four Fusion sites (no dbt / no staging Fusion required).
-- Run once against database MMSR (adjust schema if needed).
--
-- Touches only: daily_energy_mwh, energy_actual, energy_adjusted,
--               daily_ghi_kwh_m2, ghi_actual
-- Source: mart.mart_simulation_targets_daily (same site_name + date_key).

BEGIN;

UPDATE mart.mart_site_performance_daily AS m
SET
    daily_energy_mwh = t.energy_target_mwh,
    energy_actual = t.energy_target_mwh,
    energy_adjusted = t.energy_target_mwh,
    daily_ghi_kwh_m2 = t.ghi,
    ghi_actual = t.ghi
FROM mart.mart_simulation_targets_daily AS t
WHERE m.date_key = DATE '2026-04-26'
  AND t.date_key = DATE '2026-04-26'
  AND m.site_name = t.site_name
  AND m.site_name IN (
      'PLTS Mall Panakkukang',
      'PT. Pusan Manis Mulia 2.06 MWp - Tangerang',
      'PT. MMKI 5.7 MWp - Phase 2',
      'PT. MMKI 4.292 MWP - Phase 3'
  );

-- Optional: keep ratio columns roughly consistent with the new energy/GHI
-- (uncomment if you want PR vs target % refreshed in the same run)
/*
UPDATE mart.mart_site_performance_daily AS m
SET
    pr_ghi_actual = CASE
        WHEN m.daily_ghi_kwh_m2 IS NOT NULL
         AND m.daily_ghi_kwh_m2 >= 0.1
         AND m.daily_energy_mwh IS NOT NULL
         AND m.daily_energy_mwh >= 0.01
         AND m.actual_capacity_kw IS NOT NULL
         AND m.actual_capacity_kw > 0
        THEN LEAST(
            ((m.daily_energy_mwh * 1000.0) / NULLIF(m.daily_ghi_kwh_m2, 0) / NULLIF(m.actual_capacity_kw, 0))::numeric(18,6),
            10.0
        )
        ELSE m.pr_ghi_actual
    END,
    energy_actual_vs_target_pct = CASE
        WHEN m.energy_target_mwh IS NOT NULL AND m.energy_target_mwh > 0
        THEN (m.daily_energy_mwh / NULLIF(m.energy_target_mwh, 0))::numeric(18,6)
        ELSE m.energy_actual_vs_target_pct
    END,
    ghi_actual_vs_target_pct = CASE
        WHEN m.ghi_target IS NOT NULL AND m.ghi_target > 0
        THEN (m.daily_ghi_kwh_m2 / NULLIF(m.ghi_target, 0))::numeric(18,6)
        ELSE m.ghi_actual_vs_target_pct
    END
WHERE m.date_key = DATE '2026-04-26'
  AND m.site_name IN (
      'PLTS Mall Panakkukang',
      'PT. Pusan Manis Mulia 2.06 MWp - Tangerang',
      'PT. MMKI 5.7 MWp - Phase 2',
      'PT. MMKI 4.292 MWP - Phase 3'
  );
*/

COMMIT;
