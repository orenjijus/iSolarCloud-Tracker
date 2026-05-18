-- ============================================
-- MMKI GHI GOODNESS-OF-FIT ANALYSIS
-- ============================================
-- Analisis goodness-of-fit GHI simulasi vs GHI aktual untuk MMKI
-- 
-- Konteks:
-- - PLTS MMKI (±11 MW total) dibangun bertahap: Phase 1, Phase 2, Phase 3
-- - Sensor GHI aktual satu lokasi → data aktual sama untuk semua phase
-- - Setiap phase punya hasil simulasi PVSyst berbeda:
--   * Beda waktu SolarGIS
--   * Beda asumsi input
--   * Beda timeline desain
-- 
-- Pertanyaan utama:
-- "Dari tiga simulasi GHI itu, mana yang paling representatif 
--  untuk dijadikan baseline irradiance seluruh site?"
-- 
-- Data:
-- - GHI Aktual: GHI adjusted dari Phase 1 (PT. MMKI 1.75 MWp - Painting Building)
-- - GHI Simulasi: dari mart_simulation_targets_daily untuk setiap phase
-- - Periode: Januari - November 2025
-- ============================================

-- ============================================
-- STEP 1: Data Harian - GHI Aktual vs Simulasi per Phase
-- ============================================
-- Menampilkan data harian untuk analisis detail

WITH ghi_actual AS (
    -- GHI Aktual dari Phase 1 (sama untuk semua phase karena satu sensor)
    SELECT 
        date_key,
        ghi_adjusted as ghi_actual_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi_adjusted IS NOT NULL
),
ghi_simulation_phase1 AS (
    -- GHI Simulasi Phase 1
    SELECT 
        date_key,
        ghi as ghi_sim_phase1_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase2 AS (
    -- GHI Simulasi Phase 2
    SELECT 
        date_key,
        ghi as ghi_sim_phase2_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase3 AS (
    -- GHI Simulasi Phase 3
    SELECT 
        date_key,
        ghi as ghi_sim_phase3_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
daily_comparison AS (
    SELECT 
        ga.date_key,
        EXTRACT(YEAR FROM ga.date_key) as year,
        EXTRACT(MONTH FROM ga.date_key) as month,
        TO_CHAR(ga.date_key, 'Month') as month_name,
        ga.ghi_actual_kwh_m2,
        g1.ghi_sim_phase1_kwh_m2,
        g2.ghi_sim_phase2_kwh_m2,
        g3.ghi_sim_phase3_kwh_m2,
        -- Residuals (error) untuk setiap phase
        ga.ghi_actual_kwh_m2 - g1.ghi_sim_phase1_kwh_m2 as residual_phase1,
        ga.ghi_actual_kwh_m2 - g2.ghi_sim_phase2_kwh_m2 as residual_phase2,
        ga.ghi_actual_kwh_m2 - g3.ghi_sim_phase3_kwh_m2 as residual_phase3,
        -- Absolute errors
        ABS(ga.ghi_actual_kwh_m2 - g1.ghi_sim_phase1_kwh_m2) as abs_error_phase1,
        ABS(ga.ghi_actual_kwh_m2 - g2.ghi_sim_phase2_kwh_m2) as abs_error_phase2,
        ABS(ga.ghi_actual_kwh_m2 - g3.ghi_sim_phase3_kwh_m2) as abs_error_phase3,
        -- Percentage errors
        CASE 
            WHEN ga.ghi_actual_kwh_m2 > 0 
            THEN ABS((ga.ghi_actual_kwh_m2 - g1.ghi_sim_phase1_kwh_m2) / ga.ghi_actual_kwh_m2) * 100
            ELSE NULL
        END as pct_error_phase1,
        CASE 
            WHEN ga.ghi_actual_kwh_m2 > 0 
            THEN ABS((ga.ghi_actual_kwh_m2 - g2.ghi_sim_phase2_kwh_m2) / ga.ghi_actual_kwh_m2) * 100
            ELSE NULL
        END as pct_error_phase2,
        CASE 
            WHEN ga.ghi_actual_kwh_m2 > 0 
            THEN ABS((ga.ghi_actual_kwh_m2 - g3.ghi_sim_phase3_kwh_m2) / ga.ghi_actual_kwh_m2) * 100
            ELSE NULL
        END as pct_error_phase3
    FROM ghi_actual ga
    LEFT JOIN ghi_simulation_phase1 g1 ON ga.date_key = g1.date_key
    LEFT JOIN ghi_simulation_phase2 g2 ON ga.date_key = g2.date_key
    LEFT JOIN ghi_simulation_phase3 g3 ON ga.date_key = g3.date_key
)
SELECT 
    date_key,
    year,
    month,
    month_name,
    ROUND(ghi_actual_kwh_m2, 3) as ghi_actual_kwh_m2,
    ROUND(ghi_sim_phase1_kwh_m2, 3) as ghi_sim_phase1_kwh_m2,
    ROUND(ghi_sim_phase2_kwh_m2, 3) as ghi_sim_phase2_kwh_m2,
    ROUND(ghi_sim_phase3_kwh_m2, 3) as ghi_sim_phase3_kwh_m2,
    ROUND(residual_phase1, 3) as residual_phase1,
    ROUND(residual_phase2, 3) as residual_phase2,
    ROUND(residual_phase3, 3) as residual_phase3,
    ROUND(abs_error_phase1, 3) as abs_error_phase1,
    ROUND(abs_error_phase2, 3) as abs_error_phase2,
    ROUND(abs_error_phase3, 3) as abs_error_phase3,
    ROUND(pct_error_phase1, 2) as pct_error_phase1,
    ROUND(pct_error_phase2, 2) as pct_error_phase2,
    ROUND(pct_error_phase3, 2) as pct_error_phase3
FROM daily_comparison
ORDER BY date_key;


-- ============================================
-- STEP 2: Goodness-of-Fit Metrics per Phase
-- ============================================
-- Menghitung metrik statistik untuk evaluasi model

WITH ghi_actual AS (
    SELECT 
        date_key,
        ghi_adjusted as ghi_actual_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi_adjusted IS NOT NULL
),
ghi_simulation_phase1 AS (
    SELECT 
        date_key,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase2 AS (
    SELECT 
        date_key,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase3 AS (
    SELECT 
        date_key,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
paired_data_phase1 AS (
    SELECT 
        ga.date_key,
        ga.ghi_actual_kwh_m2,
        g1.ghi_sim_kwh_m2,
        ga.ghi_actual_kwh_m2 - g1.ghi_sim_kwh_m2 as residual,
        ABS(ga.ghi_actual_kwh_m2 - g1.ghi_sim_kwh_m2) as abs_error,
        CASE 
            WHEN ga.ghi_actual_kwh_m2 > 0 
            THEN ABS((ga.ghi_actual_kwh_m2 - g1.ghi_sim_kwh_m2) / ga.ghi_actual_kwh_m2) * 100
            ELSE NULL
        END as pct_error
    FROM ghi_actual ga
    INNER JOIN ghi_simulation_phase1 g1 ON ga.date_key = g1.date_key
),
paired_data_phase2 AS (
    SELECT 
        ga.date_key,
        ga.ghi_actual_kwh_m2,
        g2.ghi_sim_kwh_m2,
        ga.ghi_actual_kwh_m2 - g2.ghi_sim_kwh_m2 as residual,
        ABS(ga.ghi_actual_kwh_m2 - g2.ghi_sim_kwh_m2) as abs_error,
        CASE 
            WHEN ga.ghi_actual_kwh_m2 > 0 
            THEN ABS((ga.ghi_actual_kwh_m2 - g2.ghi_sim_kwh_m2) / ga.ghi_actual_kwh_m2) * 100
            ELSE NULL
        END as pct_error
    FROM ghi_actual ga
    INNER JOIN ghi_simulation_phase2 g2 ON ga.date_key = g2.date_key
),
paired_data_phase3 AS (
    SELECT 
        ga.date_key,
        ga.ghi_actual_kwh_m2,
        g3.ghi_sim_kwh_m2,
        ga.ghi_actual_kwh_m2 - g3.ghi_sim_kwh_m2 as residual,
        ABS(ga.ghi_actual_kwh_m2 - g3.ghi_sim_kwh_m2) as abs_error,
        CASE 
            WHEN ga.ghi_actual_kwh_m2 > 0 
            THEN ABS((ga.ghi_actual_kwh_m2 - g3.ghi_sim_kwh_m2) / ga.ghi_actual_kwh_m2) * 100
            ELSE NULL
        END as pct_error
    FROM ghi_actual ga
    INNER JOIN ghi_simulation_phase3 g3 ON ga.date_key = g3.date_key
),
mean_calc_phase1 AS (
    SELECT AVG(ghi_actual_kwh_m2) as mean_actual
    FROM paired_data_phase1
),
stats_phase1 AS (
    SELECT 
        COUNT(*) as n,
        AVG(pd.ghi_actual_kwh_m2) as mean_actual,
        AVG(pd.ghi_sim_kwh_m2) as mean_sim,
        AVG(pd.residual) as mean_bias,
        STDDEV(pd.residual) as stddev_residual,
        SQRT(AVG(pd.residual * pd.residual)) as rmse,
        AVG(pd.abs_error) as mae,
        AVG(pd.pct_error) as mape,
        -- R² = 1 - (SS_res / SS_tot)
        -- SS_res = sum of squared residuals
        -- SS_tot = sum of squared deviations from mean
        1 - (SUM(pd.residual * pd.residual) / NULLIF(SUM((pd.ghi_actual_kwh_m2 - mc.mean_actual) * 
                                                          (pd.ghi_actual_kwh_m2 - mc.mean_actual)), 0)) as r_squared,
        -- Correlation coefficient
        CORR(pd.ghi_actual_kwh_m2, pd.ghi_sim_kwh_m2) as correlation
    FROM paired_data_phase1 pd
    CROSS JOIN mean_calc_phase1 mc
),
mean_calc_phase2 AS (
    SELECT AVG(ghi_actual_kwh_m2) as mean_actual
    FROM paired_data_phase2
),
stats_phase2 AS (
    SELECT 
        COUNT(*) as n,
        AVG(pd.ghi_actual_kwh_m2) as mean_actual,
        AVG(pd.ghi_sim_kwh_m2) as mean_sim,
        AVG(pd.residual) as mean_bias,
        STDDEV(pd.residual) as stddev_residual,
        SQRT(AVG(pd.residual * pd.residual)) as rmse,
        AVG(pd.abs_error) as mae,
        AVG(pd.pct_error) as mape,
        1 - (SUM(pd.residual * pd.residual) / NULLIF(SUM((pd.ghi_actual_kwh_m2 - mc.mean_actual) * 
                                                          (pd.ghi_actual_kwh_m2 - mc.mean_actual)), 0)) as r_squared,
        CORR(pd.ghi_actual_kwh_m2, pd.ghi_sim_kwh_m2) as correlation
    FROM paired_data_phase2 pd
    CROSS JOIN mean_calc_phase2 mc
),
mean_calc_phase3 AS (
    SELECT AVG(ghi_actual_kwh_m2) as mean_actual
    FROM paired_data_phase3
),
stats_phase3 AS (
    SELECT 
        COUNT(*) as n,
        AVG(pd.ghi_actual_kwh_m2) as mean_actual,
        AVG(pd.ghi_sim_kwh_m2) as mean_sim,
        AVG(pd.residual) as mean_bias,
        STDDEV(pd.residual) as stddev_residual,
        SQRT(AVG(pd.residual * pd.residual)) as rmse,
        AVG(pd.abs_error) as mae,
        AVG(pd.pct_error) as mape,
        1 - (SUM(pd.residual * pd.residual) / NULLIF(SUM((pd.ghi_actual_kwh_m2 - mc.mean_actual) * 
                                                          (pd.ghi_actual_kwh_m2 - mc.mean_actual)), 0)) as r_squared,
        CORR(pd.ghi_actual_kwh_m2, pd.ghi_sim_kwh_m2) as correlation
    FROM paired_data_phase3 pd
    CROSS JOIN mean_calc_phase3 mc
)
SELECT 
    'Phase 1 (1.75 MWp)' as phase,
    n as sample_size,
    ROUND(mean_actual, 3) as mean_ghi_actual_kwh_m2,
    ROUND(mean_sim, 3) as mean_ghi_sim_kwh_m2,
    ROUND(mean_bias, 3) as mean_bias_kwh_m2,
    ROUND(stddev_residual, 3) as stddev_residual_kwh_m2,
    ROUND(rmse, 3) as rmse_kwh_m2,
    ROUND(mae, 3) as mae_kwh_m2,
    ROUND(mape, 2) as mape_percent,
    ROUND(r_squared, 4) as r_squared,
    ROUND(correlation, 4) as correlation_coefficient
FROM stats_phase1

UNION ALL

SELECT 
    'Phase 2 (5.7 MWp)' as phase,
    n as sample_size,
    ROUND(mean_actual, 3) as mean_ghi_actual_kwh_m2,
    ROUND(mean_sim, 3) as mean_ghi_sim_kwh_m2,
    ROUND(mean_bias, 3) as mean_bias_kwh_m2,
    ROUND(stddev_residual, 3) as stddev_residual_kwh_m2,
    ROUND(rmse, 3) as rmse_kwh_m2,
    ROUND(mae, 3) as mae_kwh_m2,
    ROUND(mape, 2) as mape_percent,
    ROUND(r_squared, 4) as r_squared,
    ROUND(correlation, 4) as correlation_coefficient
FROM stats_phase2

UNION ALL

SELECT 
    'Phase 3 (4.292 MWp)' as phase,
    n as sample_size,
    ROUND(mean_actual, 3) as mean_ghi_actual_kwh_m2,
    ROUND(mean_sim, 3) as mean_ghi_sim_kwh_m2,
    ROUND(mean_bias, 3) as mean_bias_kwh_m2,
    ROUND(stddev_residual, 3) as stddev_residual_kwh_m2,
    ROUND(rmse, 3) as rmse_kwh_m2,
    ROUND(mae, 3) as mae_kwh_m2,
    ROUND(mape, 2) as mape_percent,
    ROUND(r_squared, 4) as r_squared,
    ROUND(correlation, 4) as correlation_coefficient
FROM stats_phase3

ORDER BY 
    CASE phase
        WHEN 'Phase 1 (1.75 MWp)' THEN 1
        WHEN 'Phase 2 (5.7 MWp)' THEN 2
        WHEN 'Phase 3 (4.292 MWp)' THEN 3
    END;


-- ============================================
-- STEP 3: Agregasi Bulanan - Summary per Bulan
-- ============================================
-- Menampilkan agregasi bulanan untuk melihat pola musiman

WITH ghi_actual AS (
    SELECT 
        date_key,
        EXTRACT(YEAR FROM date_key) as year,
        EXTRACT(MONTH FROM date_key) as month,
        ghi_adjusted as ghi_actual_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi_adjusted IS NOT NULL
),
ghi_simulation_phase1 AS (
    SELECT 
        date_key,
        EXTRACT(YEAR FROM date_key) as year,
        EXTRACT(MONTH FROM date_key) as month,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase2 AS (
    SELECT 
        date_key,
        EXTRACT(YEAR FROM date_key) as year,
        EXTRACT(MONTH FROM date_key) as month,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase3 AS (
    SELECT 
        date_key,
        EXTRACT(YEAR FROM date_key) as year,
        EXTRACT(MONTH FROM date_key) as month,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
monthly_actual AS (
    SELECT 
        year,
        month,
        COUNT(*) as days_count,
        SUM(ghi_actual_kwh_m2) as monthly_ghi_actual_kwh_m2,
        AVG(ghi_actual_kwh_m2) as avg_daily_ghi_actual_kwh_m2
    FROM ghi_actual
    GROUP BY year, month
),
monthly_sim_phase1 AS (
    SELECT 
        year,
        month,
        COUNT(*) as days_count,
        SUM(ghi_sim_kwh_m2) as monthly_ghi_sim_kwh_m2,
        AVG(ghi_sim_kwh_m2) as avg_daily_ghi_sim_kwh_m2
    FROM ghi_simulation_phase1
    GROUP BY year, month
),
monthly_sim_phase2 AS (
    SELECT 
        year,
        month,
        COUNT(*) as days_count,
        SUM(ghi_sim_kwh_m2) as monthly_ghi_sim_kwh_m2,
        AVG(ghi_sim_kwh_m2) as avg_daily_ghi_sim_kwh_m2
    FROM ghi_simulation_phase2
    GROUP BY year, month
),
monthly_sim_phase3 AS (
    SELECT 
        year,
        month,
        COUNT(*) as days_count,
        SUM(ghi_sim_kwh_m2) as monthly_ghi_sim_kwh_m2,
        AVG(ghi_sim_kwh_m2) as avg_daily_ghi_sim_kwh_m2
    FROM ghi_simulation_phase3
    GROUP BY year, month
)
SELECT 
    ma.year,
    ma.month,
    TO_CHAR(TO_DATE(ma.month::text || '/1/' || ma.year::text, 'MM/DD/YYYY'), 'Month YYYY') as month_label,
    ma.days_count,
    ROUND(ma.monthly_ghi_actual_kwh_m2, 2) as monthly_ghi_actual_kwh_m2,
    ROUND(ma.avg_daily_ghi_actual_kwh_m2, 3) as avg_daily_ghi_actual_kwh_m2,
    -- Phase 1
    ROUND(m1.monthly_ghi_sim_kwh_m2, 2) as monthly_ghi_sim_phase1_kwh_m2,
    ROUND(m1.avg_daily_ghi_sim_kwh_m2, 3) as avg_daily_ghi_sim_phase1_kwh_m2,
    ROUND(ma.monthly_ghi_actual_kwh_m2 - m1.monthly_ghi_sim_kwh_m2, 2) as diff_phase1_kwh_m2,
    ROUND((ma.monthly_ghi_actual_kwh_m2 - m1.monthly_ghi_sim_kwh_m2) / NULLIF(ma.monthly_ghi_actual_kwh_m2, 0) * 100, 2) as pct_diff_phase1,
    -- Phase 2
    ROUND(m2.monthly_ghi_sim_kwh_m2, 2) as monthly_ghi_sim_phase2_kwh_m2,
    ROUND(m2.avg_daily_ghi_sim_kwh_m2, 3) as avg_daily_ghi_sim_phase2_kwh_m2,
    ROUND(ma.monthly_ghi_actual_kwh_m2 - m2.monthly_ghi_sim_kwh_m2, 2) as diff_phase2_kwh_m2,
    ROUND((ma.monthly_ghi_actual_kwh_m2 - m2.monthly_ghi_sim_kwh_m2) / NULLIF(ma.monthly_ghi_actual_kwh_m2, 0) * 100, 2) as pct_diff_phase2,
    -- Phase 3
    ROUND(m3.monthly_ghi_sim_kwh_m2, 2) as monthly_ghi_sim_phase3_kwh_m2,
    ROUND(m3.avg_daily_ghi_sim_kwh_m2, 3) as avg_daily_ghi_sim_phase3_kwh_m2,
    ROUND(ma.monthly_ghi_actual_kwh_m2 - m3.monthly_ghi_sim_kwh_m2, 2) as diff_phase3_kwh_m2,
    ROUND((ma.monthly_ghi_actual_kwh_m2 - m3.monthly_ghi_sim_kwh_m2) / NULLIF(ma.monthly_ghi_actual_kwh_m2, 0) * 100, 2) as pct_diff_phase3
FROM monthly_actual ma
LEFT JOIN monthly_sim_phase1 m1 ON ma.year = m1.year AND ma.month = m1.month
LEFT JOIN monthly_sim_phase2 m2 ON ma.year = m2.year AND ma.month = m2.month
LEFT JOIN monthly_sim_phase3 m3 ON ma.year = m3.year AND ma.month = m3.month
ORDER BY ma.year, ma.month;


-- ============================================
-- STEP 4: Rekomendasi Baseline (Summary)
-- ============================================
-- Ringkasan untuk membantu menentukan baseline mana yang terbaik

WITH ghi_actual AS (
    SELECT 
        date_key,
        ghi_adjusted as ghi_actual_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi_adjusted IS NOT NULL
),
ghi_simulation_phase1 AS (
    SELECT 
        date_key,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase2 AS (
    SELECT 
        date_key,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
ghi_simulation_phase3 AS (
    SELECT 
        date_key,
        ghi as ghi_sim_kwh_m2
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
        AND date_key >= '2025-01-01'::date
        AND date_key <= '2025-11-30'::date
        AND ghi IS NOT NULL
),
paired_data_phase1 AS (
    SELECT 
        ga.date_key,
        ga.ghi_actual_kwh_m2,
        g1.ghi_sim_kwh_m2,
        ga.ghi_actual_kwh_m2 - g1.ghi_sim_kwh_m2 as residual,
        ABS(ga.ghi_actual_kwh_m2 - g1.ghi_sim_kwh_m2) as abs_error
    FROM ghi_actual ga
    INNER JOIN ghi_simulation_phase1 g1 ON ga.date_key = g1.date_key
),
paired_data_phase2 AS (
    SELECT 
        ga.date_key,
        ga.ghi_actual_kwh_m2,
        g2.ghi_sim_kwh_m2,
        ga.ghi_actual_kwh_m2 - g2.ghi_sim_kwh_m2 as residual,
        ABS(ga.ghi_actual_kwh_m2 - g2.ghi_sim_kwh_m2) as abs_error
    FROM ghi_actual ga
    INNER JOIN ghi_simulation_phase2 g2 ON ga.date_key = g2.date_key
),
paired_data_phase3 AS (
    SELECT 
        ga.date_key,
        ga.ghi_actual_kwh_m2,
        g3.ghi_sim_kwh_m2,
        ga.ghi_actual_kwh_m2 - g3.ghi_sim_kwh_m2 as residual,
        ABS(ga.ghi_actual_kwh_m2 - g3.ghi_sim_kwh_m2) as abs_error
    FROM ghi_actual ga
    INNER JOIN ghi_simulation_phase3 g3 ON ga.date_key = g3.date_key
),
mean_calc_phase1_summary AS (
    SELECT AVG(ghi_actual_kwh_m2) as mean_actual
    FROM paired_data_phase1
),
stats_phase1 AS (
    SELECT 
        COUNT(*) as n,
        SQRT(AVG(pd.residual * pd.residual)) as rmse,
        AVG(pd.abs_error) as mae,
        1 - (SUM(pd.residual * pd.residual) / NULLIF(SUM((pd.ghi_actual_kwh_m2 - mc.mean_actual) * 
                                                          (pd.ghi_actual_kwh_m2 - mc.mean_actual)), 0)) as r_squared,
        CORR(pd.ghi_actual_kwh_m2, pd.ghi_sim_kwh_m2) as correlation
    FROM paired_data_phase1 pd
    CROSS JOIN mean_calc_phase1_summary mc
),
mean_calc_phase2_summary AS (
    SELECT AVG(ghi_actual_kwh_m2) as mean_actual
    FROM paired_data_phase2
),
stats_phase2 AS (
    SELECT 
        COUNT(*) as n,
        SQRT(AVG(pd.residual * pd.residual)) as rmse,
        AVG(pd.abs_error) as mae,
        1 - (SUM(pd.residual * pd.residual) / NULLIF(SUM((pd.ghi_actual_kwh_m2 - mc.mean_actual) * 
                                                          (pd.ghi_actual_kwh_m2 - mc.mean_actual)), 0)) as r_squared,
        CORR(pd.ghi_actual_kwh_m2, pd.ghi_sim_kwh_m2) as correlation
    FROM paired_data_phase2 pd
    CROSS JOIN mean_calc_phase2_summary mc
),
mean_calc_phase3_summary AS (
    SELECT AVG(ghi_actual_kwh_m2) as mean_actual
    FROM paired_data_phase3
),
stats_phase3 AS (
    SELECT 
        COUNT(*) as n,
        SQRT(AVG(pd.residual * pd.residual)) as rmse,
        AVG(pd.abs_error) as mae,
        1 - (SUM(pd.residual * pd.residual) / NULLIF(SUM((pd.ghi_actual_kwh_m2 - mc.mean_actual) * 
                                                          (pd.ghi_actual_kwh_m2 - mc.mean_actual)), 0)) as r_squared,
        CORR(pd.ghi_actual_kwh_m2, pd.ghi_sim_kwh_m2) as correlation
    FROM paired_data_phase3 pd
    CROSS JOIN mean_calc_phase3_summary mc
),
all_stats AS (
    SELECT 'Phase 1' as phase, rmse, mae, r_squared, correlation FROM stats_phase1
    UNION ALL
    SELECT 'Phase 2' as phase, rmse, mae, r_squared, correlation FROM stats_phase2
    UNION ALL
    SELECT 'Phase 3' as phase, rmse, mae, r_squared, correlation FROM stats_phase3
)
SELECT 
    phase,
    ROUND(rmse, 3) as rmse_kwh_m2,
    ROUND(mae, 3) as mae_kwh_m2,
    ROUND(r_squared, 4) as r_squared,
    ROUND(correlation, 4) as correlation,
    -- Ranking berdasarkan kombinasi metrik (lower RMSE/MAE dan higher R²/correlation = better)
    CASE 
        WHEN rmse = (SELECT MIN(rmse) FROM all_stats) THEN '✓ Best RMSE'
        ELSE ''
    END as rmse_rank,
    CASE 
        WHEN mae = (SELECT MIN(mae) FROM all_stats) THEN '✓ Best MAE'
        ELSE ''
    END as mae_rank,
    CASE 
        WHEN r_squared = (SELECT MAX(r_squared) FROM all_stats) THEN '✓ Best R²'
        ELSE ''
    END as r2_rank,
    CASE 
        WHEN correlation = (SELECT MAX(correlation) FROM all_stats) THEN '✓ Best Correlation'
        ELSE ''
    END as corr_rank
FROM all_stats
ORDER BY 
    -- Sort by best overall fit (weighted: R² 40%, Correlation 30%, RMSE 20%, MAE 10%)
    (r_squared * 0.4 + correlation * 0.3 - (rmse / 10) * 0.2 - (mae / 10) * 0.1) DESC;

