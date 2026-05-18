-- ============================================================================
-- Materialized View: Performance Monitoring 5-Minute Data
-- Site: Garuda Metalindo 1
-- Year: 2025
-- Generated: C:\Users\Administrator\Documents\Code\MMSR API - Server MA\excel\generate_mv_performance_monitoring.py
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_performance_monitoring_5min_garuda_metalindo_1_2025 AS
WITH 
-- ============================================================================
-- METER DATA: Pivot per metric
-- ============================================================================
meter_pivot AS (
    SELECT 
        m.timestamp,
        m.date_key,
        MAX(CASE WHEN m.metric_name = 'positive_active_energy' THEN m.metric_value END) as meter_positive_energy_kwh,
        MAX(CASE WHEN m.metric_name = 'negative_active_energy' THEN m.metric_value END) as meter_negative_energy_kwh,
        MAX(CASE WHEN m.metric_name = 'active_power' THEN m.metric_value END) as meter_active_power_kw,
        MAX(m.asset_name) as meter_name,
        MAX(m.asset_id) as meter_id
    FROM mart.mart_meter_performance_5min m
    WHERE m.site_name = 'Garuda Metalindo 1'
        AND m.date_key >= '2025-01-01'::date
        AND m.date_key < '2026-01-01'::date
        AND m.metric_name IN ('positive_active_energy', 'negative_active_energy', 'active_power')
    GROUP BY m.timestamp, m.date_key
),

-- ============================================================================
-- SENSOR DATA: Pivot per metric
-- ============================================================================
sensor_pivot AS (
    SELECT 
        s.timestamp,
        s.date_key,
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
        MAX(CASE WHEN s.metric_name = 'temperature' THEN s.metric_value END) as sensor_temperature_c,
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
    WHERE s.site_name = 'Garuda Metalindo 1'
        AND s.date_key >= '2025-01-01'::date
        AND s.date_key < '2026-01-01'::date
        AND s.metric_name IN ('irradiance', 'temperature')
    GROUP BY s.timestamp, s.date_key
),

-- ============================================================================
-- INVERTER DATA: Pivot per inverter (semua nilai 5 menit, bukan agregasi)
-- ============================================================================
inverter_pivot AS (
    SELECT 
        i.timestamp,
        i.date_key,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_1_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_1_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_2_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_2_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_25_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_3_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_26_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_4_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_27_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_5_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_29_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_6_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_3_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_7_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_30_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_8_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_31_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_9_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_32_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_10_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_33_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_11_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_34_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_12_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_35_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_13_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_36_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_14_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_37_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_15_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_38_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_16_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_39_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_17_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_4_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_18_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_40_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_19_active_power_kw,

        MAX(CASE 
            WHEN i.asset_id = 'ISO_1458125_1_41_1'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_20_active_power_kw,
        COUNT(DISTINCT CASE 
            WHEN i.metric_name = 'inv_active_power' 
            THEN i.asset_id 
        END) as total_inverter_count,
        STRING_AGG(DISTINCT i.asset_name, ', ' ORDER BY i.asset_name) as inverter_names
    FROM mart.mart_inverter_performance_5min i
    WHERE i.site_name = 'Garuda Metalindo 1'
        AND i.date_key >= '2025-01-01'::date
        AND i.date_key < '2026-01-01'::date
        AND i.metric_name = 'inv_active_power'
        AND i.metric_value IS NOT NULL
    GROUP BY i.timestamp, i.date_key
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
-- Create Indexes
-- ============================================================================

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_performance_monitoring_5min_garuda_metalindo_1_2025_unique_timestamp 
    ON mart.mv_performance_monitoring_5min_garuda_metalindo_1_2025(timestamp);

CREATE INDEX IF NOT EXISTS idx_mv_performance_monitoring_5min_garuda_metalindo_1_2025_date 
    ON mart.mv_performance_monitoring_5min_garuda_metalindo_1_2025(date_key);

CREATE INDEX IF NOT EXISTS idx_mv_performance_monitoring_5min_garuda_metalindo_1_2025_date_timestamp 
    ON mart.mv_performance_monitoring_5min_garuda_metalindo_1_2025(date_key, timestamp);

-- ============================================================================
-- Grant Permissions
-- ============================================================================

GRANT SELECT ON mart.mv_performance_monitoring_5min_garuda_metalindo_1_2025 TO PUBLIC;

-- ============================================================================
-- Create View Wrapper
-- ============================================================================

CREATE OR REPLACE VIEW mart.vw_performance_monitoring_5min_garuda_metalindo_1_2025 AS
SELECT * 
FROM mart.mv_performance_monitoring_5min_garuda_metalindo_1_2025
ORDER BY timestamp;

GRANT SELECT ON mart.vw_performance_monitoring_5min_garuda_metalindo_1_2025 TO PUBLIC;

