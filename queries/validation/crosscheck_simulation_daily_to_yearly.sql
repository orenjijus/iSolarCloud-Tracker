-- =============================================================================
-- Crosscheck: Energy Daily (25 tahun) dijumlah kembali ke Yearly
-- Bandingkan dengan sumber yearly (Energy TS & Energy Sim Target) per site.
-- Jalankan setelah: dbt run --select mart_simulation_daily_25y
-- =============================================================================

-- 1) Yearly dari daily: jumlah energy_simulation_mwh dan energy_target_mwh per (site, simulation_year)
WITH yearly_from_daily AS (
    SELECT
        site_code,
        site_name,
        simulation_year,
        SUM(energy_simulation_mwh) AS yearly_simulation_from_daily_mwh,
        SUM(energy_target_mwh)    AS yearly_target_from_daily_mwh
    FROM mart.mart_simulation_daily_25y
    GROUP BY site_code, site_name, simulation_year
),

-- 2) Sumber yearly: Energy TS (untuk simulasi) dari degradation_factors
source_yearly AS (
    SELECT
        site_code,
        site_name,
        simulation_year,
        energy_yearly_mwh AS source_energy_ts_mwh
    FROM mart.mart_simulation_degradation_factors
),

-- 3) Sumber yearly target dari seed (jika ada kolom Energy Sim Target)
source_yearly_target AS (
    SELECT
        "Site_Code"::text AS site_code,
        "Year"::int       AS simulation_year,
        CAST(REPLACE(REPLACE(TRIM(COALESCE("Energy Sim Target (MWh)"::text, '')), ',', '.'), ' ', '') AS NUMERIC) AS source_energy_sim_target_mwh
    FROM staging.seed_yearly_simulation_target
    WHERE NULLIF(TRIM(COALESCE("Energy Sim Target (MWh)"::text, '')), '') IS NOT NULL
),

-- 4) Gabung dan bandingkan
comparison AS (
    SELECT
        d.site_code,
        d.site_name,
        d.simulation_year,
        d.yearly_simulation_from_daily_mwh,
        s.source_energy_ts_mwh,
        d.yearly_simulation_from_daily_mwh - s.source_energy_ts_mwh AS diff_simulation_mwh,
        CASE
            WHEN s.source_energy_ts_mwh IS NOT NULL AND s.source_energy_ts_mwh > 0
            THEN ROUND(100.0 * (d.yearly_simulation_from_daily_mwh - s.source_energy_ts_mwh) / s.source_energy_ts_mwh, 6)
            ELSE NULL
        END AS pct_diff_simulation,
        d.yearly_target_from_daily_mwh,
        t.source_energy_sim_target_mwh,
        d.yearly_target_from_daily_mwh - t.source_energy_sim_target_mwh AS diff_target_mwh,
        CASE
            WHEN t.source_energy_sim_target_mwh IS NOT NULL AND t.source_energy_sim_target_mwh > 0
            THEN ROUND(100.0 * (d.yearly_target_from_daily_mwh - t.source_energy_sim_target_mwh) / t.source_energy_sim_target_mwh, 6)
            ELSE NULL
        END AS pct_diff_target
    FROM yearly_from_daily d
    INNER JOIN source_yearly s
        ON s.site_code = d.site_code AND s.simulation_year = d.simulation_year
    LEFT JOIN source_yearly_target t
        ON t.site_code = d.site_code AND t.simulation_year = d.simulation_year
)

-- Output: Energy TS (yearly) = referensi; yearly dari daily = jumlah daily simulation/target.
SELECT
    site_code,
    site_name,
    simulation_year,
    ROUND(source_energy_ts_mwh, 4)              AS energy_ts_yearly_mwh,
    ROUND(yearly_simulation_from_daily_mwh, 4)  AS yearly_from_daily_simulation_mwh,
    ROUND(yearly_target_from_daily_mwh, 4)      AS yearly_from_daily_target_mwh,
    ROUND(diff_simulation_mwh, 6)               AS diff_simulation_vs_ts_mwh,
    pct_diff_simulation,
    ROUND(diff_target_mwh, 6)                  AS diff_target_mwh,
    pct_diff_target,
    CASE
        WHEN ABS(COALESCE(diff_simulation_mwh, 0)) < 0.01 AND (diff_target_mwh IS NULL OR ABS(diff_target_mwh) < 0.01)
        THEN 'OK'
        WHEN ABS(COALESCE(diff_simulation_mwh, 0)) < 0.01 THEN 'OK (target N/A or small diff)'
        ELSE 'CHECK'
    END AS status
FROM comparison
ORDER BY site_code, simulation_year;


-- =============================================================================
-- PART 2: Satu tabel ringkasan semua site (angka Year 1 + status 25 tahun)
-- Energy TS yearly = referensi; yearly from daily = jumlah dari daily simulation/target.
-- =============================================================================
/*
WITH yearly_from_daily AS (
    SELECT site_code, site_name, simulation_year,
           SUM(energy_simulation_mwh) AS yearly_from_daily_simulation_mwh,
           SUM(energy_target_mwh)    AS yearly_from_daily_target_mwh
    FROM mart.mart_simulation_daily_25y
    GROUP BY site_code, site_name, simulation_year
),
source_ts AS (
    SELECT site_code, site_name, simulation_year, energy_yearly_mwh AS energy_ts_yearly_mwh
    FROM mart.mart_simulation_degradation_factors
),
cmp AS (
    SELECT d.site_code, d.site_name, d.simulation_year,
           s.energy_ts_yearly_mwh,
           d.yearly_from_daily_simulation_mwh,
           d.yearly_from_daily_target_mwh,
           d.yearly_from_daily_simulation_mwh - s.energy_ts_yearly_mwh AS diff_simulation_vs_ts_mwh
    FROM yearly_from_daily d
    JOIN source_ts s ON s.site_code = d.site_code AND s.simulation_year = d.simulation_year
),
year1 AS (
    SELECT site_code, site_name, energy_ts_yearly_mwh, yearly_from_daily_simulation_mwh, yearly_from_daily_target_mwh, diff_simulation_vs_ts_mwh
    FROM cmp WHERE simulation_year = 1
),
status_per_site AS (
    SELECT site_code, SUM(CASE WHEN ABS(diff_simulation_vs_ts_mwh) >= 0.01 THEN 1 ELSE 0 END) AS years_with_diff
    FROM cmp GROUP BY site_code
)
SELECT y.site_code, y.site_name,
       ROUND(y.energy_ts_yearly_mwh, 2) AS energy_ts_yearly_year1_mwh,
       ROUND(y.yearly_from_daily_simulation_mwh, 2) AS yearly_from_daily_simulation_year1_mwh,
       ROUND(y.yearly_from_daily_target_mwh, 2) AS yearly_from_daily_target_year1_mwh,
       ROUND(y.diff_simulation_vs_ts_mwh, 4) AS diff_simulation_vs_ts_year1_mwh,
       ROUND(MAX(ABS(c.diff_simulation_vs_ts_mwh)), 2) AS max_abs_diff_25y_mwh,
       CASE WHEN COALESCE(s.years_with_diff, 0) = 0 THEN 'OK' ELSE 'CHECK' END AS status
FROM year1 y
JOIN cmp c ON c.site_code = y.site_code
LEFT JOIN status_per_site s ON s.site_code = y.site_code
GROUP BY y.site_code, y.site_name, y.energy_ts_yearly_mwh, y.yearly_from_daily_simulation_mwh, y.yearly_from_daily_target_mwh, y.diff_simulation_vs_ts_mwh, s.years_with_diff
ORDER BY y.site_code;
*/
