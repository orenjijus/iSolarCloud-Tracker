-- ============================================
-- CHECK TARGET MONTHLY MMKI PHASE 3 - DECEMBER 2025
-- ============================================
-- Membandingkan target monthly di database vs Excel
-- 
-- Excel value: 418.3944483
-- Database value: perlu dicek
-- 
-- Site: PT. MMKI 4.292 MWP - Phase 3
-- Bulan: Desember 2025
-- ============================================

-- ============================================
-- STEP 1: Cek Target Monthly di Database
-- ============================================
-- Dari mart_simulation_targets_monthly

SELECT 
    'DATABASE - MART_MONTHLY' as source,
    site_name,
    site_code,
    year,
    month,
    month_name,
    days_count,
    energy_target_mwh as target_monthly_mwh,
    energy_simulation_mwh as simulation_monthly_mwh
FROM "MMSR"."mart"."mart_simulation_targets_monthly"
WHERE site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%'
    AND year = 2025
    AND month = 12;


-- ============================================
-- STEP 2: Cek Target Monthly dari SUM Daily
-- ============================================
-- Hitung manual dari daily data

SELECT 
    'DATABASE - SUM DAILY' as source,
    site_name,
    site_code,
    year,
    month,
    COUNT(*) as days_count,
    SUM(energy_target_mwh) as target_monthly_mwh_sum,
    SUM(energy_simulation_mwh) as simulation_monthly_mwh_sum
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE (site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%')
    AND year = 2025
    AND month = 12
GROUP BY site_name, site_code, year, month;


-- ============================================
-- STEP 3: Cek Target Monthly dari Seed File
-- ============================================
-- Source data asli

SELECT 
    'SEED FILE' as source,
    "Site_Name" as site_name,
    "Site_Code" as site_code,
    EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')) as year,
    EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY')) as month,
    COUNT(*) as days_count,
    SUM(CAST(REPLACE(COALESCE("Energy Target (MW)"::text, '0'), ',', '.') AS NUMERIC)) as target_monthly_mwh_seed,
    SUM(CAST(REPLACE(COALESCE("Energy Simulation (MW)"::text, '0'), ',', '.') AS NUMERIC)) as simulation_monthly_mwh_seed
FROM "MMSR"."staging"."seed_daily_simulation_target"
WHERE ("Site_Name" LIKE '%MMKI%4.292%' 
    OR "Site_Name" LIKE '%MMKI%Phase 3%'
    OR "Site_Name" LIKE '%4.292%')
    AND EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')) = 2025
    AND EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY')) = 12
GROUP BY "Site_Name", "Site_Code", EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')), EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY'));


-- ============================================
-- STEP 4: Detail Daily Data untuk Desember 2025
-- ============================================
-- Lihat semua hari untuk investigasi

SELECT 
    date_key,
    site_name,
    site_code,
    energy_target_mwh as daily_target,
    energy_simulation_mwh as daily_simulation,
    year,
    month,
    day_type
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE (site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%')
    AND year = 2025
    AND month = 12
ORDER BY date_key;


-- ============================================
-- STEP 5: Perbandingan Lengkap
-- ============================================
-- Bandingkan semua source dengan Excel value

WITH db_monthly AS (
    SELECT 
        site_name,
        year,
        month,
        energy_target_mwh as target_monthly_mwh
    FROM "MMSR"."mart"."mart_simulation_targets_monthly"
    WHERE (site_name LIKE '%MMKI%4.292%' 
        OR site_name LIKE '%MMKI%Phase 3%'
        OR site_name LIKE '%4.292%')
        AND year = 2025
        AND month = 12
),
db_sum_daily AS (
    SELECT 
        site_name,
        year,
        month,
        SUM(energy_target_mwh) as target_monthly_mwh
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE (site_name LIKE '%MMKI%4.292%' 
        OR site_name LIKE '%MMKI%Phase 3%'
        OR site_name LIKE '%4.292%')
        AND year = 2025
        AND month = 12
    GROUP BY site_name, year, month
),
seed_sum AS (
    SELECT 
        "Site_Name" as site_name,
        EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')) as year,
        EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY')) as month,
        SUM(CAST(REPLACE(COALESCE("Energy Target (MW)"::text, '0'), ',', '.') AS NUMERIC)) as target_monthly_mwh
    FROM "MMSR"."staging"."seed_daily_simulation_target"
    WHERE ("Site_Name" LIKE '%MMKI%4.292%' 
        OR "Site_Name" LIKE '%MMKI%Phase 3%'
        OR "Site_Name" LIKE '%4.292%')
        AND EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')) = 2025
        AND EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY')) = 12
    GROUP BY "Site_Name", EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')), EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY'))
)
SELECT 
    COALESCE(m.site_name, d.site_name, s.site_name) as site_name,
    COALESCE(m.year, d.year, s.year) as year,
    COALESCE(m.month, d.month, s.month) as month,
    
    -- Values from different sources
    m.target_monthly_mwh as db_monthly_table,
    d.target_monthly_mwh as db_sum_daily,
    s.target_monthly_mwh as seed_file,
    418.3944483 as excel_value,
    
    -- Differences
    m.target_monthly_mwh - 418.3944483 as diff_db_monthly_vs_excel,
    d.target_monthly_mwh - 418.3944483 as diff_db_sum_vs_excel,
    s.target_monthly_mwh - 418.3944483 as diff_seed_vs_excel,
    
    -- Percentage differences
    CASE 
        WHEN 418.3944483 > 0 
        THEN ((m.target_monthly_mwh - 418.3944483) / 418.3944483 * 100)
        ELSE NULL
    END as pct_diff_db_monthly,
    CASE 
        WHEN 418.3944483 > 0 
        THEN ((d.target_monthly_mwh - 418.3944483) / 418.3944483 * 100)
        ELSE NULL
    END as pct_diff_db_sum,
    CASE 
        WHEN 418.3944483 > 0 
        THEN ((s.target_monthly_mwh - 418.3944483) / 418.3944483 * 100)
        ELSE NULL
    END as pct_diff_seed
    
FROM db_monthly m
FULL OUTER JOIN db_sum_daily d
    ON m.site_name = d.site_name
    AND m.year = d.year
    AND m.month = d.month
FULL OUTER JOIN seed_sum s
    ON COALESCE(m.site_name, d.site_name) = s.site_name
    AND COALESCE(m.year, d.year) = s.year
    AND COALESCE(m.month, d.month) = s.month;


-- ============================================
-- STEP 6: Cek Issue Dates atau Missing Dates
-- ============================================
-- Apakah ada issue dates yang mempengaruhi perhitungan?

SELECT 
    'ISSUE_DATES' as check_type,
    COUNT(*) as issue_dates_count
FROM "MMSR"."staging"."seed_issue_dates" id
JOIN "MMSR"."staging"."seed_daily_simulation_target" sdst
    ON TO_DATE(sdst."Date", 'DD/MM/YYYY') = id.date_key
    AND sdst."Site_Name" LIKE '%MMKI%4.292%'
WHERE EXTRACT(YEAR FROM id.date_key) = 2025
    AND EXTRACT(MONTH FROM id.date_key) = 12

UNION ALL

SELECT 
    'TOTAL_DAYS_IN_MONTH' as check_type,
    COUNT(DISTINCT date_key) as days_count
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE (site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%')
    AND year = 2025
    AND month = 12

UNION ALL

SELECT 
    'EXPECTED_DAYS' as check_type,
    31 as days_count;  -- Desember punya 31 hari


-- ============================================
-- STEP 7: Cek Apakah Excel Exclude Certain Days
-- ============================================
-- Mungkin Excel exclude weekend atau issue dates?

SELECT 
    date_key,
    site_name,
    energy_target_mwh,
    day_type,
    CASE 
        WHEN id.date_key IS NOT NULL THEN 'ISSUE_DATE'
        ELSE 'NORMAL'
    END as is_issue_date
FROM "MMSR"."mart"."mart_simulation_targets_daily" mstd
LEFT JOIN "MMSR"."staging"."seed_issue_dates" id
    ON mstd.date_key = id.date_key
    AND mstd.site_name = id.site_name
WHERE (mstd.site_name LIKE '%MMKI%4.292%' 
    OR mstd.site_name LIKE '%MMKI%Phase 3%'
    OR mstd.site_name LIKE '%4.292%')
    AND mstd.year = 2025
    AND mstd.month = 12
ORDER BY date_key;


-- ============================================
-- STEP 8: Hitung Target dengan Exclude Issue Dates
-- ============================================
-- Mungkin Excel exclude issue dates?

SELECT 
    'SUM_EXCLUDE_ISSUE_DATES' as calculation_type,
    SUM(mstd.energy_target_mwh) as target_monthly_mwh
FROM "MMSR"."mart"."mart_simulation_targets_daily" mstd
LEFT JOIN "MMSR"."staging"."seed_issue_dates" id
    ON mstd.date_key = id.date_key
    AND mstd.site_name = id.site_name
WHERE (mstd.site_name LIKE '%MMKI%4.292%' 
    OR mstd.site_name LIKE '%MMKI%Phase 3%'
    OR mstd.site_name LIKE '%4.292%')
    AND mstd.year = 2025
    AND mstd.month = 12
    AND id.date_key IS NULL  -- Exclude issue dates

UNION ALL

SELECT 
    'SUM_ALL_DAYS' as calculation_type,
    SUM(energy_target_mwh) as target_monthly_mwh
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE (site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%')
    AND year = 2025
    AND month = 12;

