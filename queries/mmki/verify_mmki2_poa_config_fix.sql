-- ============================================================================
-- Verifikasi Perbaikan Konfigurasi POA MMKI 2
-- ============================================================================
-- Query ini digunakan untuk memverifikasi bahwa perbaikan konfigurasi sensor
-- POA untuk MMKI 2 sudah benar dan data historis sudah ter-update
--
-- Issue: Sensor IRR-AMB-NW-Bod Stamp (EM011023C7355634) sebelumnya salah
--        dikategorikan sebagai POA, padahal seharusnya bukan sensor POA
-- ============================================================================

-- 0. CEK DATA DI MART_SENSOR_MEASUREMENTS_5MIN
-- ============================================================================
-- Cek apakah sensor yang salah masih memiliki sensor_type='POA' di data historis
-- Expected: Setelah re-run, sensor_type harus NULL (bukan 'POA')
-- ============================================================================
SELECT 
    date_key,
    site_name,
    asset_id,
    sensor_type,
    sensor_dev_name,
    COUNT(*) as occurrence_count,
    MIN(timestamp) as first_occurrence,
    MAX(timestamp) as last_occurrence
FROM mart.mart_sensor_measurements_5min
WHERE asset_id = 'FS_EM011023C7355634'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2'
GROUP BY date_key, site_name, asset_id, sensor_type, sensor_dev_name
ORDER BY date_key DESC
LIMIT 10;

-- Jika query ini masih menunjukkan sensor_type='POA', berarti perlu re-run mart_sensor_measurements_5min

-- 1. VERIFIKASI KONFIGURASI SENSOR
-- ============================================================================
-- Cek konfigurasi sensor untuk MMKI 2 (NE=51758766)
-- Expected: EM011023C7355634 harus TIDAK memiliki sensor_type = 'POA'
-- ============================================================================
SELECT 
    site_id,
    dev_name,
    device_id,
    sensor_type,
    sensor_capacity,
    CASE 
        WHEN device_id = 'EM011023C7355634' AND sensor_type IS NULL OR sensor_type = '' THEN '✅ CORRECT - Not POA'
        WHEN device_id = 'EM011023C7355634' AND sensor_type = 'POA' THEN '❌ ERROR - Should not be POA'
        WHEN sensor_type = 'POA' THEN '✅ Valid POA sensor'
        ELSE 'Other sensor type'
    END as validation_status
FROM staging.seed_sensor_config
WHERE site_id = 'NE=51758766'
ORDER BY 
    CASE 
        WHEN sensor_type = 'POA' THEN 1
        WHEN device_id = 'EM011023C7355634' THEN 2
        ELSE 3
    END,
    device_id;

-- 2. CEK SENSOR POA YANG DIGUNAKAN DI MART_SENSOR_DAILY
-- ============================================================================
-- Verifikasi bahwa sensor yang salah TIDAK muncul sebagai POA di mart
-- Expected: EM011023C7355634 TIDAK muncul dengan sensor_type = 'POA'
-- ============================================================================
SELECT 
    date_key,
    site_name,
    device_id,
    sensor_dev_name,
    sensor_type,
    sensor_capacity_kwp,
    daily_irradiance_kwh_m2,
    CASE 
        WHEN device_id = 'EM011023C7355634' AND sensor_type = 'POA' THEN '❌ ERROR - Should not be POA'
        WHEN device_id = 'EM011023C7355634' THEN '✅ CORRECT - Not POA or NULL'
        ELSE '✅ Valid POA sensor'
    END as validation_status
FROM mart.mart_sensor_daily
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND date_key >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date_key DESC, device_id;

-- 3. HITUNG JUMLAH SENSOR POA PER HARI
-- ============================================================================
-- Verifikasi jumlah sensor POA yang digunakan
-- Expected: 5 sensor POA (bukan 6)
-- ============================================================================
SELECT 
    date_key,
    COUNT(DISTINCT device_id) as poa_sensor_count,
    SUM(sensor_capacity_kwp) as total_capacity_kwp,
    STRING_AGG(DISTINCT device_id, ', ' ORDER BY device_id) as poa_device_ids,
    CASE 
        WHEN COUNT(DISTINCT device_id) = 5 THEN '✅ CORRECT - 5 POA sensors'
        WHEN COUNT(DISTINCT device_id) > 5 THEN '❌ ERROR - Too many sensors (includes invalid)'
        ELSE '⚠️ WARNING - Less than 5 sensors'
    END as validation_status
FROM mart.mart_sensor_daily
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND sensor_type = 'POA'
    AND date_key >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date_key
ORDER BY date_key DESC;

-- 4. VERIFIKASI PERHITUNGAN POA WEIGHTED AVERAGE
-- ============================================================================
-- Bandingkan POA weighted dari mart_site_performance_daily dengan
-- manual calculation untuk memastikan konsistensi
-- ============================================================================
WITH manual_poa_calc AS (
    SELECT 
        sd.date_key,
        sd.site_name,
        SUM(sd.daily_irradiance_kwh_m2 * sd.sensor_capacity_kwp) as weighted_sum,
        SUM(sd.sensor_capacity_kwp) as total_capacity,
        SUM(sd.daily_irradiance_kwh_m2 * sd.sensor_capacity_kwp) / 
            NULLIF(SUM(sd.sensor_capacity_kwp), 0) as manual_poa_weighted
    FROM mart.mart_sensor_daily sd
    WHERE sd.site_name = 'PT. MMKI 5.7 MWp - Phase 2'
        AND sd.sensor_type = 'POA'
        AND sd.date_key >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY sd.date_key, sd.site_name
)
SELECT 
    sp.date_key,
    sp.site_name,
    sp.daily_poa_weighted_kwh_m2 as poa_from_mart,
    mp.manual_poa_weighted as poa_manual_calc,
    ABS(sp.daily_poa_weighted_kwh_m2 - mp.manual_poa_weighted) as difference,
    mp.total_capacity as total_capacity_kwp,
    CASE 
        WHEN ABS(sp.daily_poa_weighted_kwh_m2 - mp.manual_poa_weighted) < 0.001 THEN '✅ CORRECT - Match'
        ELSE '⚠️ WARNING - Mismatch'
    END as validation_status
FROM mart.mart_site_performance_daily sp
INNER JOIN manual_poa_calc mp
    ON sp.date_key = mp.date_key
    AND sp.site_name = mp.site_name
WHERE sp.site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND sp.date_key >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY sp.date_key DESC;

-- 5. CEK SENSOR YANG SALAH DI DATA HISTORIS
-- ============================================================================
-- Cek apakah sensor yang salah masih muncul di data historis
-- Expected: EM011023C7355634 TIDAK muncul sebagai POA di data historis
-- ============================================================================
SELECT 
    date_key,
    site_name,
    device_id,
    sensor_dev_name,
    sensor_type,
    COUNT(*) as occurrence_count,
    MIN(date_key) as first_occurrence,
    MAX(date_key) as last_occurrence
FROM mart.mart_sensor_daily
WHERE device_id = 'EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2'
GROUP BY date_key, site_name, device_id, sensor_dev_name, sensor_type
ORDER BY date_key DESC;

-- Jika query ini mengembalikan rows, berarti data historis masih salah
-- dan perlu dihapus dengan query di bawah ini

-- 6. CEK DATA DI FACT_SENSOR_CALCULATIONS_5MIN
-- ============================================================================
-- Cek apakah sensor yang salah masih muncul di fact_sensor_calculations_5min
-- Expected: FS_EM011023C7355634 TIDAK muncul sebagai POA
-- ============================================================================
SELECT 
    date_key,
    site_name,
    sensor_id,
    sensor_name,
    sensor_type,
    COUNT(*) as occurrence_count,
    MIN(timestamp) as first_occurrence,
    MAX(timestamp) as last_occurrence
FROM mart.fact_sensor_calculations_5min
WHERE sensor_id = 'FS_EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2'
GROUP BY date_key, site_name, sensor_id, sensor_name, sensor_type
ORDER BY date_key DESC
LIMIT 10;

-- 7. HAPUS DATA HISTORIS YANG SALAH (Jalankan jika query #5 atau #6 mengembalikan rows)
-- ============================================================================
-- Hapus data historis dengan sensor_type='POA' untuk sensor yang salah
-- WARNING: Jalankan query ini hanya setelah memastikan seed config sudah benar!
-- ============================================================================

-- 7.1. Hapus dari fact_sensor_calculations_5min
/*
DELETE FROM mart.fact_sensor_calculations_5min
WHERE sensor_id = 'FS_EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2';
    
-- Atau dengan date range:
DELETE FROM mart.fact_sensor_calculations_5min
WHERE sensor_id = 'FS_EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND date_key >= '2025-01-01'
    AND date_key <= '2025-12-31';
*/

-- 7.2. Hapus dari mart_sensor_daily
/*
DELETE FROM mart.mart_sensor_daily
WHERE device_id = 'EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2';
    
-- Atau dengan date range:
DELETE FROM mart.mart_sensor_daily
WHERE device_id = 'EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND date_key >= '2025-01-01'
    AND date_key <= '2025-12-31';
*/

-- 8. SUMMARY: POA SENSOR CONFIGURATION FOR MMKI 2
-- ============================================================================
-- Summary semua sensor POA yang valid untuk MMKI 2
-- ============================================================================
SELECT 
    sc.device_id,
    sc.dev_name as sensor_name,
    sc.sensor_type,
    CAST(REPLACE(sc.sensor_capacity::text, ',', '.') AS NUMERIC) as capacity_kwp,
    CASE 
        WHEN sc.sensor_type = 'POA' THEN '✅ Valid POA'
        ELSE '❌ Not POA'
    END as status
FROM staging.seed_sensor_config sc
WHERE sc.site_id = 'NE=51758766'
    AND sc.sensor_type = 'POA'
ORDER BY sc.device_id;

-- Expected Result: 5 sensors dengan total capacity ~5707.065 kWp

