-- ============================================================================
-- Test Queries: Validasi Materialized View
-- ============================================================================

-- ============================================================================
-- Test 1: Check Materialized View Exists
-- ============================================================================

SELECT 
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size,
    (SELECT COUNT(*) 
     FROM information_schema.tables t
     WHERE t.table_schema = schemaname
     AND t.table_name = matviewname) as estimated_rows
FROM pg_matviews
WHERE schemaname = 'mart'
    AND matviewname LIKE 'mv_performance_monitoring_5min%'
ORDER BY matviewname;

-- ============================================================================
-- Test 2: Check Indexes
-- ============================================================================

SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'mart'
    AND tablename LIKE 'mv_performance_monitoring_5min%'
ORDER BY tablename, indexname;

-- ============================================================================
-- Test 3: Query Performance Test - Full Year
-- ============================================================================

-- Replace with actual site name and year
EXPLAIN ANALYZE
SELECT * 
FROM mart.mv_performance_monitoring_5min_mmki1_2025
ORDER BY timestamp;

-- Expected: < 1 second untuk ~105K rows

-- ============================================================================
-- Test 4: Query Performance Test - Date Range (1 Month)
-- ============================================================================

EXPLAIN ANALYZE
SELECT * 
FROM mart.mv_performance_monitoring_5min_mmki1_2025
WHERE date_key >= '2025-12-01' 
  AND date_key < '2026-01-01'
ORDER BY timestamp;

-- Expected: < 100ms untuk ~8,760 rows (1 bulan)

-- ============================================================================
-- Test 5: Query Performance Test - Date Range (1 Day)
-- ============================================================================

EXPLAIN ANALYZE
SELECT * 
FROM mart.mv_performance_monitoring_5min_mmki1_2025
WHERE date_key = '2025-12-14'
ORDER BY timestamp;

-- Expected: < 50ms untuk ~288 rows (1 hari)

-- ============================================================================
-- Test 6: Data Validation - Check Row Counts
-- ============================================================================

-- Compare row counts between source tables and materialized view
WITH source_data AS (
    SELECT 
        COUNT(DISTINCT timestamp) as meter_timestamps
    FROM mart.mart_meter_performance_5min
    WHERE site_name = 'MMKI 1'
        AND date_key >= '2025-01-01'
        AND date_key < '2026-01-01'
),
mv_data AS (
    SELECT 
        COUNT(*) as mv_rows
    FROM mart.mv_performance_monitoring_5min_mmki1_2025
)
SELECT 
    s.meter_timestamps,
    m.mv_rows,
    CASE 
        WHEN m.mv_rows >= s.meter_timestamps * 0.9 THEN 'OK'
        ELSE 'WARNING: Row count mismatch'
    END as status
FROM source_data s
CROSS JOIN mv_data m;

-- ============================================================================
-- Test 7: Data Validation - Check Data Completeness
-- ============================================================================

SELECT 
    date_key,
    COUNT(*) as row_count,
    COUNT(meter_positive_energy_kwh) as meter_data_count,
    COUNT(sensor_ghi_w_m2) as sensor_data_count,
    COUNT(inverter_1_active_power_kw) as inverter_data_count
FROM mart.mv_performance_monitoring_5min_mmki1_2025
WHERE date_key >= '2025-12-01'
GROUP BY date_key
ORDER BY date_key;

-- ============================================================================
-- Test 8: Refresh Function Test
-- ============================================================================

-- Test refresh function
SELECT * FROM mart.refresh_mv_performance_5min_site_year('MMKI 1', 2025);

-- ============================================================================
-- Test 9: Get Status of All Materialized Views
-- ============================================================================

SELECT * FROM mart.get_mv_performance_5min_status(2025);

-- ============================================================================
-- Test 10: Excel Query Simulation
-- ============================================================================

-- Simulate Excel query (full year)
SELECT 
    timestamp,
    date_key,
    meter_positive_energy_kwh,
    meter_negative_energy_kwh,
    meter_active_power_kw,
    sensor_ghi_w_m2,
    sensor_poa_w_m2,
    sensor_temperature_c,
    inverter_1_active_power_kw,
    inverter_2_active_power_kw,
    inverter_3_active_power_kw
FROM mart.vw_performance_monitoring_5min_mmki1_2025
ORDER BY timestamp
LIMIT 100;  -- Test dengan limit dulu

-- ============================================================================
-- Test 11: Check for Missing Data
-- ============================================================================

-- Check for gaps in timestamp sequence
WITH expected_timestamps AS (
    SELECT generate_series(
        '2025-01-01 00:00:00'::timestamp,
        '2025-12-31 23:55:00'::timestamp,
        '5 minutes'::interval
    ) as expected_ts
),
actual_timestamps AS (
    SELECT DISTINCT timestamp
    FROM mart.mv_performance_monitoring_5min_mmki1_2025
)
SELECT 
    e.expected_ts,
    CASE WHEN a.timestamp IS NULL THEN 'MISSING' ELSE 'OK' END as status
FROM expected_timestamps e
LEFT JOIN actual_timestamps a ON e.expected_ts = a.timestamp
WHERE a.timestamp IS NULL
LIMIT 100;  -- Show first 100 missing timestamps

