-- ============================================
-- Verify Cleaning Log Data
-- ============================================
-- Script untuk verifikasi data cleaning log
-- setelah dbt seed dijalankan
-- ============================================

-- 1. Cek total records
SELECT 
    'Total Records' as check_type,
    COUNT(*) as count
FROM staging.seed_cleaning_log;

-- 2. Cek per asset_type
SELECT 
    'Per Asset Type' as check_type,
    asset_type,
    COUNT(*) as count
FROM staging.seed_cleaning_log
GROUP BY asset_type;

-- 3. Cek data dengan cleaning_date NULL
SELECT 
    'Records dengan cleaning_date NULL' as check_type,
    COUNT(*) as count
FROM staging.seed_cleaning_log
WHERE cleaning_date IS NULL;

-- 4. Cek data per site (dengan cleaning_date)
SELECT 
    'Last Cleaning Date per Site' as check_type,
    asset_type,
    COALESCE(site_id, site_name) as site_identifier,
    site_id,
    site_name,
    MAX(cleaning_date) as last_cleaning_date,
    COUNT(*) as total_records,
    COUNT(cleaning_date) as records_with_date,
    CURRENT_DATE - MAX(cleaning_date) as days_since_cleaning
FROM staging.seed_cleaning_log
WHERE is_active = TRUE
GROUP BY asset_type, site_id, site_name
ORDER BY asset_type, COALESCE(site_id, site_name);

-- 5. Cek data yang belum ada cleaning_date
SELECT 
    'Sites tanpa cleaning_date' as check_type,
    asset_type,
    COALESCE(site_id, site_name) as site_identifier,
    asset_id,
    site_id,
    site_name
FROM staging.seed_cleaning_log
WHERE cleaning_date IS NULL
ORDER BY asset_type, COALESCE(site_id, site_name);

-- 6. Cek constraint violation (site_id dan site_name keduanya kosong)
SELECT 
    'Constraint Violation Check' as check_type,
    COUNT(*) as records_without_site_identifier
FROM staging.seed_cleaning_log
WHERE (site_id IS NULL OR site_id = '') 
  AND (site_name IS NULL OR site_name = '');

