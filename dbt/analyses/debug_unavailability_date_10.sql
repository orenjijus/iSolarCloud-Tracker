-- ============================================
-- Debug Unavailability untuk Tanggal 10
-- ============================================
-- Query untuk menganalisis mengapa unavailability_hours berbeda antara Excel dan DB
-- Excel: unavailability = 0.010416667, power available = 11.42708333
-- DB: unavailability_hours = 6.23
-- ============================================

-- Step 1: Check availability calculation breakdown untuk tanggal 10
-- Ganti site_name dan date_key sesuai dengan data yang ingin dicek
WITH inverter_availability_5min AS (
    SELECT 
        i.date_key,
        i.site_name,
        i.timestamp,
        COUNT(DISTINCT i.asset_id) as total_inverters,
        COUNT(DISTINCT CASE WHEN i.metric_value > 0 THEN i.asset_id END) as available_inverters,
        CASE 
            WHEN COUNT(DISTINCT i.asset_id) > 0
            THEN COUNT(DISTINCT CASE WHEN i.metric_value > 0 THEN i.asset_id END)::DECIMAL 
                 / COUNT(DISTINCT i.asset_id)::DECIMAL
            ELSE 0
        END as power_availability_ratio
    FROM "MMSR"."mart"."mart_inverter_performance_5min" i
    WHERE i.metric_name = 'inv_active_power'
        AND i.metric_value IS NOT NULL
        AND i.date_key = '2025-11-10'::date  -- GANTI TANGGAL INI
        AND i.site_name = 'PT. MMKI 5.7 MWp - Phase 2'  -- GANTI SITE INI
    GROUP BY i.date_key, i.site_name, i.timestamp
),

irradiance_for_mit AS (
    SELECT 
        s.date_key,
        s.site_name,
        s.timestamp,
        COALESCE(
            MAX(CASE WHEN sc.sensor_type = 'GHI' THEN s.metric_value END),
            MAX(CASE WHEN sc.sensor_type = 'POA' THEN s.metric_value END)
        ) as irradiance_w_m2
    FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
    JOIN "MMSR"."mart"."seed_sensor_config" sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    WHERE s.metric_name = 'irradiance'
        AND s.metric_value IS NOT NULL
        AND s.date_key = '2025-11-10'::date  -- GANTI TANGGAL INI
        AND s.site_name = 'PT. MMKI 5.7 MWp - Phase 2'  -- GANTI SITE INI
    GROUP BY s.date_key, s.site_name, s.timestamp
),

mit_calculation AS (
    SELECT 
        date_key,
        site_name,
        timestamp,
        CASE 
            WHEN irradiance_w_m2 > 40 THEN 1
            ELSE 0
        END as mit
    FROM irradiance_for_mit
),

availability_with_mit AS (
    SELECT 
        COALESCE(ia.date_key, m.date_key) as date_key,
        COALESCE(ia.site_name, m.site_name) as site_name,
        COALESCE(ia.timestamp, m.timestamp) as timestamp,
        COALESCE(m.mit, 0) as mit,
        COALESCE(ia.power_availability_ratio, 0) as power_availability_ratio,
        CASE 
            WHEN COALESCE(m.mit, 0) = 1 
            THEN 1 - COALESCE(ia.power_availability_ratio, 0)
            ELSE 0
        END as unavailability_ratio,
        -- Flag untuk debugging
        CASE 
            WHEN ia.timestamp IS NULL AND m.timestamp IS NOT NULL THEN 'MIT_ONLY'
            WHEN ia.timestamp IS NOT NULL AND m.timestamp IS NULL THEN 'INVERTER_ONLY'
            WHEN ia.timestamp IS NOT NULL AND m.timestamp IS NOT NULL THEN 'BOTH'
            ELSE 'NEITHER'
        END as data_source
    FROM inverter_availability_5min ia
    FULL OUTER JOIN mit_calculation m 
        ON ia.date_key = m.date_key 
        AND ia.site_name = m.site_name 
        AND ia.timestamp = m.timestamp
)

-- Summary breakdown
SELECT 
    date_key,
    site_name,
    -- Total intervals
    COUNT(*) as total_intervals,
    -- Breakdown by data source
    COUNT(CASE WHEN data_source = 'BOTH' THEN 1 END) as intervals_with_both,
    COUNT(CASE WHEN data_source = 'MIT_ONLY' THEN 1 END) as intervals_mit_only,
    COUNT(CASE WHEN data_source = 'INVERTER_ONLY' THEN 1 END) as intervals_inverter_only,
    -- MIT breakdown
    COUNT(CASE WHEN mit = 1 THEN 1 END) as intervals_with_mit_1,
    COUNT(CASE WHEN mit = 0 THEN 1 END) as intervals_with_mit_0,
    -- Unavailability breakdown
    COUNT(CASE WHEN unavailability_ratio > 0 THEN 1 END) as intervals_with_unavailability,
    SUM(CASE WHEN data_source = 'MIT_ONLY' AND mit = 1 THEN 1 ELSE 0 END) as mit_only_with_mit_1,
    -- Calculated values
    SUM(power_availability_ratio) * 5.0 / 60.0 as power_available_hours,
    SUM(unavailability_ratio) * 5.0 / 60.0 as unavailability_hours,
    COUNT(*) * 5.0 / 60.0 as total_hours,
    -- Detailed breakdown of unavailability
    SUM(CASE WHEN data_source = 'BOTH' AND mit = 1 THEN unavailability_ratio ELSE 0 END) * 5.0 / 60.0 as unavailability_from_both,
    SUM(CASE WHEN data_source = 'MIT_ONLY' AND mit = 1 THEN unavailability_ratio ELSE 0 END) * 5.0 / 60.0 as unavailability_from_mit_only,
    SUM(CASE WHEN data_source = 'INVERTER_ONLY' THEN unavailability_ratio ELSE 0 END) * 5.0 / 60.0 as unavailability_from_inverter_only
FROM availability_with_mit
GROUP BY date_key, site_name;

-- Step 2: Detailed breakdown per hour untuk melihat pattern
-- Uncomment untuk melihat detail per jam
/*
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as intervals_in_hour,
    COUNT(CASE WHEN data_source = 'MIT_ONLY' AND mit = 1 THEN 1 END) as mit_only_with_mit_1,
    SUM(CASE WHEN data_source = 'MIT_ONLY' AND mit = 1 THEN unavailability_ratio ELSE 0 END) * 5.0 / 60.0 as unavailability_from_mit_only_hours,
    SUM(unavailability_ratio) * 5.0 / 60.0 as total_unavailability_hours
FROM availability_with_mit
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour;
*/

