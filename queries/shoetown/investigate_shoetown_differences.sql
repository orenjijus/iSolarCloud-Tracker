-- ============================================
-- Investigate Shoetown Differences
-- ============================================
-- User surprised that Shoetown is very different
-- Need to understand why
-- ============================================

-- ============================================
-- 1. Shoetown Missing Data Pattern
-- ============================================
WITH db_data AS (
    SELECT 
        d.date_key,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name = 'Shoetown Ligung Indonesia'
        AND d.date_key >= '2025-01-01'
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name = 'Shoetown Ligung Indonesia'
        AND TO_DATE(date_key, 'MM/DD/YYYY') >= '2025-01-01'
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        CASE 
            WHEN db.daily_energy_mwh IS NULL OR db.daily_energy_mwh = 0 THEN 'MISSING_DB'
            WHEN excel.daily_energy_mwh IS NULL OR excel.daily_energy_mwh = 0 THEN 'MISSING_EXCEL'
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) <= 0.01 THEN 'MATCH'
            ELSE 'MISMATCH'
        END as energy_status,
        COALESCE(db.daily_energy_mwh, 0) as db_energy,
        COALESCE(excel.daily_energy_mwh, 0) as excel_energy,
        ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) as energy_diff
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key
)
SELECT 
    energy_status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(CASE WHEN energy_status = 'MISMATCH' THEN energy_diff ELSE NULL END), 4) as avg_diff,
    ROUND(MAX(CASE WHEN energy_status = 'MISMATCH' THEN energy_diff ELSE NULL END), 4) as max_diff,
    ROUND(MIN(CASE WHEN energy_status = 'MISMATCH' THEN energy_diff ELSE NULL END), 4) as min_diff
FROM comparison
GROUP BY energy_status
ORDER BY 
    CASE energy_status
        WHEN 'MISSING_DB' THEN 1
        WHEN 'MISSING_EXCEL' THEN 2
        WHEN 'MISMATCH' THEN 3
        WHEN 'MATCH' THEN 4
    END;

-- ============================================
-- 2. Shoetown Top Mismatches
-- ============================================
WITH db_data AS (
    SELECT 
        d.date_key,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name = 'Shoetown Ligung Indonesia'
        AND d.date_key >= '2025-01-01'
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name = 'Shoetown Ligung Indonesia'
        AND TO_DATE(date_key, 'MM/DD/YYYY') >= '2025-01-01'
)
SELECT 
    COALESCE(db.date_key, excel.date_key) as date_key,
    CASE 
        WHEN db.daily_energy_mwh IS NULL OR db.daily_energy_mwh = 0 THEN 'MISSING_DB'
        WHEN excel.daily_energy_mwh IS NULL OR excel.daily_energy_mwh = 0 THEN 'MISSING_EXCEL'
        WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) <= 0.01 THEN 'MATCH'
        ELSE 'MISMATCH'
    END as status,
    ROUND(COALESCE(db.daily_energy_mwh, 0)::numeric, 4) as db_energy,
    ROUND(COALESCE(excel.daily_energy_mwh, 0)::numeric, 4) as excel_energy,
    ROUND(ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0))::numeric, 4) as energy_diff,
    ROUND(COALESCE(db.daily_ghi_kwh_m2, 0)::numeric, 4) as db_ghi,
    ROUND(COALESCE(excel.daily_ghi_kwh_m2, 0)::numeric, 4) as excel_ghi,
    ROUND(COALESCE(db.daily_poa_weighted_kwh_m2, 0)::numeric, 4) as db_poa,
    ROUND(COALESCE(excel.daily_poa_weighted_kwh_m2, 0)::numeric, 4) as excel_poa
FROM db_data db
FULL OUTER JOIN excel_data excel
    ON db.date_key = excel.date_key
WHERE (db.daily_energy_mwh IS NULL OR db.daily_energy_mwh = 0)
    OR (excel.daily_energy_mwh IS NULL OR excel.daily_energy_mwh = 0)
    OR ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 1
ORDER BY 
    ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) DESC,
    date_key DESC
LIMIT 50;

-- ============================================
-- 3. Check Shoetown Meter Data Availability
-- ============================================
-- Check if meter data exists in source for missing dates
SELECT 
    m.date_key,
    m.site_name,
    m.asset_id,
    m.metric_name,
    COUNT(*) as data_points,
    MIN(m.metric_value) as min_value,
    MAX(m.metric_value) as max_value
FROM "MMSR"."mart"."mart_meter_performance_5min" m
WHERE m.site_name = 'Shoetown Ligung Indonesia'
    AND m.date_key >= '2025-01-01'
    AND m.date_key <= '2025-01-10'
    AND m.metric_name IN ('positive_active_energy', 'negative_active_energy')
GROUP BY m.date_key, m.site_name, m.asset_id, m.metric_name
ORDER BY m.date_key, m.asset_id, m.metric_name;

