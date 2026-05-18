-- ============================================
-- CHECK SIMULATION MONTHLY MMKI PHASE 3
-- ============================================
-- Query untuk memeriksa dari mana simulation monthly didapat
-- dan membandingkan dengan Excel jika ada
-- 
-- Site: PT. MMKI 4.292 MWP - Phase 3
-- 
-- PENJELASAN:
-- Simulation monthly (energy_simulation_monthly_mwh) dihitung dari:
-- 1. Source: seed_daily_simulation_target.csv
-- 2. Kolom: "Energy Simulation (MW)"
-- 3. Perhitungan: SUM dari semua daily "Energy Simulation (MW)" dalam satu bulan
-- 4. Proses: 
--    - seed_daily_simulation_target → mart_simulation_targets_daily
--    - mart_simulation_targets_daily → SUM per bulan → mart_simulation_targets_monthly
-- 
-- CATATAN PENTING:
-- - Data MMKI Phase 3 di seed file mulai dari Juni 2025
-- - Jika Excel punya nilai berbeda, kemungkinan:
--   a. Excel punya data untuk bulan yang tidak ada di seed file (Jan-Mei 2025)
--   b. Ada missing dates di seed file
--   c. Ada perbedaan dalam cara menghitung SUM
-- ============================================

-- ============================================
-- STEP 1: Cek Data di Seed File (Source Data)
-- ============================================
-- Simulation monthly dihitung dari SUM daily simulation di seed file
-- Source: seed_daily_simulation_target.csv
-- Kolom: "Energy Simulation (MW)"

SELECT 
    'SEED FILE' as source,
    "Site_Name" as site_name,
    "Site_Code" as site_code,
    EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')) as year,
    EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY')) as month,
    COUNT(*) as days_count,
    SUM(CAST(REPLACE(COALESCE("Energy Simulation (MW)"::text, '0'), ',', '.') AS NUMERIC)) as energy_simulation_monthly_mwh_seed
FROM "MMSR"."staging"."seed_daily_simulation_target"
WHERE "Site_Name" LIKE '%MMKI%4.292%' 
    OR "Site_Name" LIKE '%MMKI%Phase 3%'
    OR "Site_Name" LIKE '%4.292%'
GROUP BY "Site_Name", "Site_Code", EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')), EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY'))
ORDER BY year DESC, month DESC;


-- ============================================
-- STEP 2: Cek Data di mart_simulation_targets_daily
-- ============================================
-- Ini adalah hasil dari seed file yang sudah diproses

SELECT 
    'MART_DAILY' as source,
    site_name,
    site_code,
    year,
    month,
    COUNT(*) as days_count,
    SUM(energy_simulation_mwh) as energy_simulation_monthly_mwh_daily
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%'
GROUP BY site_name, site_code, year, month
ORDER BY year DESC, month DESC;


-- ============================================
-- STEP 3: Cek Data di mart_simulation_targets_monthly
-- ============================================
-- Ini adalah agregasi dari mart_simulation_targets_daily

SELECT 
    'MART_MONTHLY' as source,
    site_name,
    site_code,
    year,
    month,
    days_count,
    energy_simulation_mwh as energy_simulation_monthly_mwh
FROM "MMSR"."mart"."mart_simulation_targets_monthly"
WHERE site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%'
ORDER BY year DESC, month DESC;


-- ============================================
-- STEP 4: Perbandingan Lengkap (All Sources)
-- ============================================
-- Membandingkan semua source untuk melihat perbedaan

WITH seed_monthly AS (
    SELECT 
        "Site_Name" as site_name,
        EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')) as year,
        EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY')) as month,
        COUNT(*) as days_count_seed,
        SUM(CAST(REPLACE(COALESCE("Energy Simulation (MW)"::text, '0'), ',', '.') AS NUMERIC)) as energy_simulation_monthly_mwh
    FROM "MMSR"."staging"."seed_daily_simulation_target"
    WHERE "Site_Name" LIKE '%MMKI%4.292%' 
        OR "Site_Name" LIKE '%MMKI%Phase 3%'
        OR "Site_Name" LIKE '%4.292%'
    GROUP BY "Site_Name", EXTRACT(YEAR FROM TO_DATE("Date", 'DD/MM/YYYY')), EXTRACT(MONTH FROM TO_DATE("Date", 'DD/MM/YYYY'))
),
daily_monthly AS (
    SELECT 
        site_name,
        year,
        month,
        COUNT(*) as days_count_daily,
        SUM(energy_simulation_mwh) as energy_simulation_monthly_mwh
    FROM "MMSR"."mart"."mart_simulation_targets_daily"
    WHERE site_name LIKE '%MMKI%4.292%' 
        OR site_name LIKE '%MMKI%Phase 3%'
        OR site_name LIKE '%4.292%'
    GROUP BY site_name, year, month
),
monthly_table AS (
    SELECT 
        site_name,
        year,
        month,
        days_count as days_count_monthly,
        energy_simulation_mwh as energy_simulation_monthly_mwh
    FROM "MMSR"."mart"."mart_simulation_targets_monthly"
    WHERE site_name LIKE '%MMKI%4.292%' 
        OR site_name LIKE '%MMKI%Phase 3%'
        OR site_name LIKE '%4.292%'
)
SELECT 
    COALESCE(s.site_name, d.site_name, m.site_name) as site_name,
    COALESCE(s.year, d.year, m.year) as year,
    COALESCE(s.month, d.month, m.month) as month,
    
    -- Days count comparison
    s.days_count_seed,
    d.days_count_daily,
    m.days_count_monthly,
    
    -- Energy simulation monthly comparison
    s.energy_simulation_monthly_mwh as seed_value,
    d.energy_simulation_monthly_mwh as daily_value,
    m.energy_simulation_monthly_mwh as monthly_value,
    
    -- Differences
    ABS(COALESCE(s.energy_simulation_monthly_mwh, 0) - COALESCE(d.energy_simulation_monthly_mwh, 0)) as diff_seed_vs_daily,
    ABS(COALESCE(d.energy_simulation_monthly_mwh, 0) - COALESCE(m.energy_simulation_monthly_mwh, 0)) as diff_daily_vs_monthly,
    
    -- Status
    CASE 
        WHEN s.site_name IS NULL THEN 'MISSING_IN_SEED'
        WHEN d.site_name IS NULL THEN 'MISSING_IN_DAILY'
        WHEN m.site_name IS NULL THEN 'MISSING_IN_MONTHLY'
        WHEN ABS(COALESCE(s.energy_simulation_monthly_mwh, 0) - COALESCE(d.energy_simulation_monthly_mwh, 0)) > 0.01 
            OR ABS(COALESCE(d.energy_simulation_monthly_mwh, 0) - COALESCE(m.energy_simulation_monthly_mwh, 0)) > 0.01
        THEN 'HAS_DIFFERENCES'
        ELSE 'MATCH'
    END as status
    
FROM seed_monthly s
FULL OUTER JOIN daily_monthly d
    ON s.site_name = d.site_name
    AND s.year = d.year
    AND s.month = d.month
FULL OUTER JOIN monthly_table m
    ON COALESCE(s.site_name, d.site_name) = m.site_name
    AND COALESCE(s.year, d.year) = m.year
    AND COALESCE(s.month, d.month) = m.month
ORDER BY COALESCE(s.year, d.year, m.year) DESC, COALESCE(s.month, d.month, m.month) DESC;


-- ============================================
-- STEP 5: Cek Detail Daily untuk Bulan Tertentu
-- ============================================
-- Untuk investigasi lebih detail, lihat data harian

-- Ganti tahun dan bulan sesuai kebutuhan
-- Contoh: Desember 2025
SELECT 
    date_key,
    site_name,
    site_code,
    energy_simulation_mwh,
    energy_target_mwh,
    year,
    month
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE (site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%')
    AND year = 2025  -- Ganti sesuai kebutuhan
    AND month = 12   -- Ganti sesuai kebutuhan
ORDER BY date_key;


-- ============================================
-- STEP 6: Perbandingan dengan Excel (jika ada)
-- ============================================
-- Jika ada data Excel di table site_monthly_performance_excel

SELECT 
    'DATABASE' as source,
    site_name,
    year,
    month,
    energy_simulation_mwh
FROM "MMSR"."mart"."mart_simulation_targets_monthly"
WHERE site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%'

UNION ALL

SELECT 
    'EXCEL' as source,
    site_name,
    year,
    month,
    NULL as energy_simulation_mwh  -- Excel mungkin tidak punya kolom ini, sesuaikan
FROM "MMSR"."public"."site_monthly_performance_excel"
WHERE site_name LIKE '%MMKI%4.292%' 
    OR site_name LIKE '%MMKI%Phase 3%'
    OR site_name LIKE '%4.292%'
ORDER BY year DESC, month DESC, source;

