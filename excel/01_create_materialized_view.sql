-- ============================================================================
-- Create Materialized View: Performance Monitoring 5-Minute Data
-- Format: Wide (pivoted) untuk Excel
-- Scope: Meter, Sensor, Inverter (5 menit raw data, tidak ada agregasi)
-- ============================================================================

-- Template untuk create materialized view per site per tahun
-- Usage: Replace {SITE_NAME} dan {YEAR} dengan nilai actual
-- Example: MMKI 1, 2025

-- ============================================================================
-- STEP 1: Create Materialized View
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_performance_monitoring_5min_{SITE_NAME}_{YEAR} AS
WITH 
-- ============================================================================
-- METER DATA: Pivot per metric per meter
-- ============================================================================
meter_pivot AS (
    SELECT 
        m.timestamp,
        m.date_key,
        -- Pivot meter metrics per meter device
        -- Format: meter_{meter_id}_{metric_name}
        MAX(CASE 
            WHEN m.metric_name = 'positive_active_energy' 
            THEN m.metric_value 
        END) as meter_positive_energy_kwh,
        MAX(CASE 
            WHEN m.metric_name = 'negative_active_energy' 
            THEN m.metric_value 
        END) as meter_negative_energy_kwh,
        MAX(CASE 
            WHEN m.metric_name = 'active_power' 
            THEN m.metric_value 
        END) as meter_active_power_kw,
        -- Meter metadata
        MAX(m.asset_name) as meter_name,
        MAX(m.asset_id) as meter_id
    FROM mart.mart_meter_performance_5min m
    WHERE m.site_name = '{SITE_NAME}'  -- Filter per site
        AND m.date_key >= '{YEAR}-01-01'::date
        AND m.date_key < '{YEAR+1}-01-01'::date  -- Filter per tahun
        AND m.metric_name IN ('positive_active_energy', 'negative_active_energy', 'active_power')
    GROUP BY m.timestamp, m.date_key
),

-- ============================================================================
-- SENSOR DATA: Pivot per metric per sensor
-- ============================================================================
sensor_pivot AS (
    SELECT 
        s.timestamp,
        s.date_key,
        -- Pivot sensor metrics
        MAX(CASE 
            WHEN s.metric_name = 'irradiance' 
            AND sc.sensor_type = 'GHI' 
            THEN s.metric_value 
        END) as sensor_ghi_w_m2,
        MAX(CASE 
            WHEN s.metric_name = 'irradiance' 
            AND sc.sensor_type = 'POA' 
            THEN s.metric_value 
        END) as sensor_poa_w_m2,
        MAX(CASE 
            WHEN s.metric_name = 'temperature' 
            THEN s.metric_value 
        END) as sensor_temperature_c,
        -- Sensor metadata
        MAX(s.asset_name) as sensor_name,
        MAX(s.asset_id) as sensor_id
    FROM mart.mart_sensor_measurements_5min s
    LEFT JOIN dbt.seed_sensor_config sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    WHERE s.site_name = '{SITE_NAME}'  -- Filter per site
        AND s.date_key >= '{YEAR}-01-01'::date
        AND s.date_key < '{YEAR+1}-01-01'::date  -- Filter per tahun
        AND s.metric_name IN ('irradiance', 'temperature')
    GROUP BY s.timestamp, s.date_key
),

-- ============================================================================
-- INVERTER DATA: Pivot per inverter (semua nilai 5 menit, bukan agregasi)
-- Format: Satu row per timestamp dengan kolom per inverter
-- Approach: Menggunakan conditional aggregation untuk pivot semua inverter
-- ============================================================================
inverter_base AS (
    SELECT 
        i.timestamp,
        i.date_key,
        i.asset_id,
        i.asset_name as inverter_name,
        i.metric_value as active_power_kw,
        -- Create row number untuk pivot (urutkan berdasarkan asset_id)
        ROW_NUMBER() OVER (
            PARTITION BY i.timestamp 
            ORDER BY i.asset_id
        ) as inverter_seq
    FROM mart.mart_inverter_performance_5min i
    WHERE i.site_name = '{SITE_NAME}'  -- Filter per site
        AND i.date_key >= '{YEAR}-01-01'::date
        AND i.date_key < '{YEAR+1}-01-01'::date  -- Filter per tahun
        AND i.metric_name = 'inv_active_power'  -- Hanya active power
        AND i.metric_value IS NOT NULL
),
inverter_pivot AS (
    SELECT 
        timestamp,
        date_key,
        -- Pivot semua inverter (handle sampai 20 inverter)
        -- Format: inverter_1_active_power_kw, inverter_2_active_power_kw, ...
        MAX(CASE WHEN inverter_seq = 1 THEN active_power_kw END) as inverter_1_active_power_kw,
        MAX(CASE WHEN inverter_seq = 2 THEN active_power_kw END) as inverter_2_active_power_kw,
        MAX(CASE WHEN inverter_seq = 3 THEN active_power_kw END) as inverter_3_active_power_kw,
        MAX(CASE WHEN inverter_seq = 4 THEN active_power_kw END) as inverter_4_active_power_kw,
        MAX(CASE WHEN inverter_seq = 5 THEN active_power_kw END) as inverter_5_active_power_kw,
        MAX(CASE WHEN inverter_seq = 6 THEN active_power_kw END) as inverter_6_active_power_kw,
        MAX(CASE WHEN inverter_seq = 7 THEN active_power_kw END) as inverter_7_active_power_kw,
        MAX(CASE WHEN inverter_seq = 8 THEN active_power_kw END) as inverter_8_active_power_kw,
        MAX(CASE WHEN inverter_seq = 9 THEN active_power_kw END) as inverter_9_active_power_kw,
        MAX(CASE WHEN inverter_seq = 10 THEN active_power_kw END) as inverter_10_active_power_kw,
        MAX(CASE WHEN inverter_seq = 11 THEN active_power_kw END) as inverter_11_active_power_kw,
        MAX(CASE WHEN inverter_seq = 12 THEN active_power_kw END) as inverter_12_active_power_kw,
        MAX(CASE WHEN inverter_seq = 13 THEN active_power_kw END) as inverter_13_active_power_kw,
        MAX(CASE WHEN inverter_seq = 14 THEN active_power_kw END) as inverter_14_active_power_kw,
        MAX(CASE WHEN inverter_seq = 15 THEN active_power_kw END) as inverter_15_active_power_kw,
        MAX(CASE WHEN inverter_seq = 16 THEN active_power_kw END) as inverter_16_active_power_kw,
        MAX(CASE WHEN inverter_seq = 17 THEN active_power_kw END) as inverter_17_active_power_kw,
        MAX(CASE WHEN inverter_seq = 18 THEN active_power_kw END) as inverter_18_active_power_kw,
        MAX(CASE WHEN inverter_seq = 19 THEN active_power_kw END) as inverter_19_active_power_kw,
        MAX(CASE WHEN inverter_seq = 20 THEN active_power_kw END) as inverter_20_active_power_kw,
        -- Metadata: total inverter count dan inverter names
        COUNT(DISTINCT asset_id) as total_inverter_count,
        STRING_AGG(DISTINCT inverter_name, ', ' ORDER BY inverter_name) as inverter_names
    FROM inverter_base
    GROUP BY timestamp, date_key
)

-- ============================================================================
-- FINAL OUTPUT: Join semua data
-- ============================================================================
SELECT 
    COALESCE(m.timestamp, s.timestamp, i.timestamp) as timestamp,
    COALESCE(m.date_key, s.date_key, i.date_key) as date_key,
    
    -- Meter columns
    m.meter_positive_energy_kwh,
    m.meter_negative_energy_kwh,
    m.meter_active_power_kw,
    m.meter_name,
    m.meter_id,
    
    -- Sensor columns
    s.sensor_ghi_w_m2,
    s.sensor_poa_w_m2,
    s.sensor_temperature_c,
    s.sensor_name,
    s.sensor_id,
    
    -- Inverter columns (per inverter, semua nilai 5 menit)
    i.inverter_1_active_power_kw,
    i.inverter_2_active_power_kw,
    i.inverter_3_active_power_kw,
    i.inverter_4_active_power_kw,
    i.inverter_5_active_power_kw,
    i.inverter_6_active_power_kw,
    i.inverter_7_active_power_kw,
    i.inverter_8_active_power_kw,
    i.inverter_9_active_power_kw,
    i.inverter_10_active_power_kw,
    i.inverter_11_active_power_kw,
    i.inverter_12_active_power_kw,
    i.inverter_13_active_power_kw,
    i.inverter_14_active_power_kw,
    i.inverter_15_active_power_kw,
    i.inverter_16_active_power_kw,
    i.inverter_17_active_power_kw,
    i.inverter_18_active_power_kw,
    i.inverter_19_active_power_kw,
    i.inverter_20_active_power_kw,
    i.total_inverter_count,
    i.inverter_names
    
FROM meter_pivot m
FULL OUTER JOIN sensor_pivot s 
    ON m.timestamp = s.timestamp
FULL OUTER JOIN inverter_pivot i 
    ON COALESCE(m.timestamp, s.timestamp) = i.timestamp
ORDER BY timestamp;

-- ============================================================================
-- STEP 2: Create Indexes
-- ============================================================================

-- UNIQUE INDEX untuk CONCURRENTLY refresh (WAJIB!)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_perf_5min_{SITE_NAME}_{YEAR}_unique_timestamp 
    ON mart.mv_performance_monitoring_5min_{SITE_NAME}_{YEAR}(timestamp);

-- Index untuk query cepat dengan date filter
CREATE INDEX IF NOT EXISTS idx_mv_perf_5min_{SITE_NAME}_{YEAR}_date 
    ON mart.mv_performance_monitoring_5min_{SITE_NAME}_{YEAR}(date_key);

-- Composite index untuk range query
CREATE INDEX IF NOT EXISTS idx_mv_perf_5min_{SITE_NAME}_{YEAR}_date_timestamp 
    ON mart.mv_performance_monitoring_5min_{SITE_NAME}_{YEAR}(date_key, timestamp);

-- ============================================================================
-- STEP 3: Grant Permissions
-- ============================================================================

GRANT SELECT ON mart.mv_performance_monitoring_5min_{SITE_NAME}_{YEAR} TO PUBLIC;

-- ============================================================================
-- STEP 4: Create View Wrapper (Optional, untuk query mudah)
-- ============================================================================

CREATE OR REPLACE VIEW mart.vw_performance_monitoring_5min_{SITE_NAME}_{YEAR} AS
SELECT * 
FROM mart.mv_performance_monitoring_5min_{SITE_NAME}_{YEAR}
ORDER BY timestamp;

GRANT SELECT ON mart.vw_performance_monitoring_5min_{SITE_NAME}_{YEAR} TO PUBLIC;

-- ============================================================================
-- NOTES:
-- 1. Replace {SITE_NAME} dengan nama site (contoh: 'MMKI 1')
-- 2. Replace {YEAR} dengan tahun (contoh: '2025')
-- 3. Inverter pivot: Support sampai 20 inverter per site
--    - Jika lebih dari 20 inverter, hanya 20 pertama yang akan muncul
--    - Check total_inverter_count untuk validasi
-- 4. Semua nilai adalah data 5 menit mentah (tidak ada agregasi)
-- 5. Gunakan script generator di 04_generate_all_sites.sql untuk auto-generate
-- ============================================================================

