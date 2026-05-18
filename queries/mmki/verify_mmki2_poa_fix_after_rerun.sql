-- ============================================================================
-- Verifikasi Perbaikan Konfigurasi POA MMKI 2 - Setelah Re-run
-- ============================================================================
-- Query ini digunakan untuk memverifikasi bahwa perbaikan konfigurasi sensor
-- POA untuk MMKI 2 sudah benar setelah re-run models
-- ============================================================================

-- 1. CEK SENSOR YANG SALAH DI MART_SENSOR_MEASUREMENTS_5MIN
-- ============================================================================
-- Verifikasi bahwa sensor yang salah sudah memiliki sensor_type = NULL
-- Expected: EM011023C7355634 harus memiliki sensor_type = NULL (bukan 'POA')
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
    AND date_key >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date_key, site_name, asset_id, sensor_type, sensor_dev_name
ORDER BY date_key DESC
LIMIT 10;

-- Expected: sensor_type harus NULL atau tidak ada (bukan 'POA')

-- 2. CEK SENSOR POA YANG DIGUNAKAN DI MART_SENSOR_DAILY
-- ============================================================================
-- Verifikasi bahwa sensor yang salah TIDAK muncul sebagai POA di mart_sensor_daily
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

-- Expected: EM011023C7355634 TIDAK muncul dengan sensor_type = 'POA'

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
        WHEN COUNT(DISTINCT device_id) < 5 THEN '⚠️ WARNING - Less than 5 sensors'
        ELSE '⚠️ WARNING - No sensors'
    END as validation_status
FROM mart.mart_sensor_daily
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND sensor_type = 'POA'
    AND date_key >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date_key
ORDER BY date_key DESC
LIMIT 30;

-- Expected: 
-- - poa_sensor_count = 5
-- - total_capacity_kwp ≈ 5707.065 kWp
-- - poa_device_ids tidak termasuk EM011023C7355634

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
ORDER BY sp.date_key DESC
LIMIT 30;

-- Expected:
-- - poa_from_mart ≈ poa_manual_calc (difference < 0.001)
-- - total_capacity_kwp ≈ 5707.065 kWp

-- 5. CEK DATA DI FACT_SENSOR_CALCULATIONS_5MIN
-- ============================================================================
-- Verifikasi bahwa sensor yang salah TIDAK muncul di fact_sensor_calculations_5min
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
    AND date_key >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date_key, site_name, sensor_id, sensor_name, sensor_type
ORDER BY date_key DESC
LIMIT 10;

-- Expected: Query ini harus mengembalikan 0 rows (tidak ada data)

-- 6. SUMMARY: SENSOR POA YANG VALID UNTUK MMKI 2
-- ============================================================================
-- Summary semua sensor POA yang valid untuk MMKI 2
-- ============================================================================
SELECT 
    date_key,
    device_id,
    sensor_dev_name,
    sensor_type,
    sensor_capacity_kwp,
    daily_irradiance_kwh_m2,
    CASE 
        WHEN device_id = 'EM011023C7355634' THEN '❌ Should not be POA'
        ELSE '✅ Valid POA'
    END as status
FROM mart.mart_sensor_daily
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND sensor_type = 'POA'
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date_key DESC, device_id;

-- Expected Result: 
-- - 5 sensors dengan total capacity ~5707.065 kWp
-- - Device IDs: EM051023B7436516, EM031023B7436516, EM011023B7436516, EM021023B7436516, EM001023C7355634
-- - EM011023C7355634 (IRR-AMB-NW-Bod Stamp) TIDAK muncul

-- 7. COMPARISON: SEBELUM vs SESUDAH (jika ada data historis)
-- ============================================================================
-- Bandingkan jumlah sensor POA sebelum dan sesudah perbaikan
-- ============================================================================
SELECT 
    CASE 
        WHEN date_key < '2025-12-17' THEN 'Before Fix'
        ELSE 'After Fix'
    END as period,
    COUNT(DISTINCT device_id) as poa_sensor_count,
    SUM(sensor_capacity_kwp) as total_capacity_kwp,
    AVG(daily_poa_weighted_kwh_m2) as avg_poa_kwh_m2
FROM mart.mart_site_performance_daily
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND date_key >= '2025-12-01'
GROUP BY 
    CASE 
        WHEN date_key < '2025-12-17' THEN 'Before Fix'
        ELSE 'After Fix'
    END
ORDER BY period;

-- Expected: After Fix harus memiliki total_capacity_kwp ≈ 5707.065 kWp

