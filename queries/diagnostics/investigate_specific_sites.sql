-- ============================================
-- Investigate Specific Sites with Issues
-- ============================================
-- Query untuk investigasi detail per site dengan masalah terbesar
-- ============================================

-- ============================================
-- 1. Shoetown Ligung Indonesia - Missing Data Issue
-- ============================================
-- Check missing data pattern
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
        ELSE 'BOTH_EXIST'
    END as data_status,
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
    OR ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 5
ORDER BY 
    ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) DESC,
    date_key DESC;

-- ============================================
-- 2. Sites with Low POA Match (<20%)
-- ============================================
-- Check POA differences for sites with low match
WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
)
SELECT 
    COALESCE(db.site_name, excel.site_name) as site_name,
    COALESCE(db.date_key, excel.date_key) as date_key,
    ROUND(db.daily_poa_weighted_kwh_m2::numeric, 4) as db_poa,
    ROUND(excel.daily_poa_weighted_kwh_m2::numeric, 4) as excel_poa,
    ROUND((db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2)::numeric, 4) as poa_diff,
    CASE 
        WHEN db.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_DB'
        WHEN excel.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_EXCEL'
        WHEN ABS(db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2) > 0.01 THEN 'MISMATCH'
        ELSE 'MATCH'
    END as status,
    CASE 
        WHEN excel.daily_poa_weighted_kwh_m2 > 0 
        THEN ROUND(100.0 * (db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2) / excel.daily_poa_weighted_kwh_m2, 2)
        ELSE NULL
    END as poa_diff_pct
FROM db_data db
FULL OUTER JOIN excel_data excel
    ON db.date_key = excel.date_key 
    AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
WHERE ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01
ORDER BY 
    site_name,
    ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) DESC,
    date_key DESC;

-- ============================================
-- 3. Sites with Variable Energy Ratio
-- ============================================
-- Check energy ratio pattern for sites with variable ratio
WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_energy_mwh
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'Shoetown Ligung Indonesia',
        'Garuda Metalindo 1',
        'Garuda Metalindo 2',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Madiun',
        'Garuda Metalindo (IKP)'
    )
        AND d.daily_energy_mwh IS NOT NULL
        AND d.daily_energy_mwh > 0
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        energy_actual_mwh::NUMERIC as daily_energy_mwh
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name IN (
        'Shoetown Ligung Indonesia',
        'Garuda Metalindo 1',
        'Garuda Metalindo 2',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Madiun',
        'Garuda Metalindo (IKP)'
    )
        AND energy_actual_mwh IS NOT NULL
        AND energy_actual_mwh > 0
)
SELECT 
    COALESCE(db.site_name, excel.site_name) as site_name,
    COALESCE(db.date_key, excel.date_key) as date_key,
    ROUND(db.daily_energy_mwh::numeric, 4) as db_energy,
    ROUND(excel.daily_energy_mwh::numeric, 4) as excel_energy,
    ROUND((db.daily_energy_mwh - excel.daily_energy_mwh)::numeric, 4) as energy_diff,
    ROUND((db.daily_energy_mwh / NULLIF(excel.daily_energy_mwh, 0))::numeric, 4) as energy_ratio,
    CASE 
        WHEN ABS(db.daily_energy_mwh - excel.daily_energy_mwh) <= 0.01 THEN 'MATCH'
        WHEN ABS(db.daily_energy_mwh - excel.daily_energy_mwh) > 0.01 THEN 'MISMATCH'
        ELSE 'MISSING'
    END as status
FROM db_data db
INNER JOIN excel_data excel
    ON db.date_key = excel.date_key 
    AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
WHERE ABS(db.daily_energy_mwh - excel.daily_energy_mwh) > 0.01
ORDER BY 
    site_name,
    ABS(db.daily_energy_mwh - excel.daily_energy_mwh) DESC,
    date_key DESC;

