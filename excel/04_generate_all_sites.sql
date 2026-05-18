-- ============================================================================
-- Script Generator: Create Materialized Views for All Sites
-- ============================================================================

-- ============================================================================
-- Step 1: Generate CREATE statements for all sites and years
-- ============================================================================

-- This script generates SQL statements to create materialized views
-- for all sites and specified years
-- 
-- Usage:
-- 1. Adjust the years list below
-- 2. Run this script to generate SQL statements
-- 3. Review and execute the generated SQL

DO $$
DECLARE
    site_record RECORD;
    year_val INTEGER;
    mv_name TEXT;
    view_name TEXT;
    sql_text TEXT;
    years INTEGER[] := ARRAY[2024, 2025, 2026];  -- Adjust years as needed
BEGIN
    -- Loop through all sites
    FOR site_record IN 
        SELECT DISTINCT site_name 
        FROM dbt.dim_assets 
        WHERE asset_level = 'Site'
        ORDER BY site_name
    LOOP
        -- Loop through years
        FOREACH year_val IN ARRAY years
        LOOP
            mv_name := 'mv_performance_monitoring_5min_' || 
                       LOWER(REPLACE(REPLACE(site_record.site_name, ' ', '_'), '-', '_')) || 
                       '_' || year_val::TEXT;
            
            view_name := 'vw_performance_monitoring_5min_' || 
                         LOWER(REPLACE(REPLACE(site_record.site_name, ' ', '_'), '-', '_')) || 
                         '_' || year_val::TEXT;
            
            -- Generate CREATE MATERIALIZED VIEW statement
            -- Note: This is a template - you need to replace placeholders
            sql_text := format('
-- ============================================================================
-- Materialized View: %s
-- Site: %s
-- Year: %s
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.%I AS
WITH 
meter_pivot AS (
    SELECT 
        m.timestamp,
        m.date_key,
        MAX(CASE WHEN m.metric_name = ''positive_active_energy'' THEN m.metric_value END) as meter_positive_energy_kwh,
        MAX(CASE WHEN m.metric_name = ''negative_active_energy'' THEN m.metric_value END) as meter_negative_energy_kwh,
        MAX(CASE WHEN m.metric_name = ''active_power'' THEN m.metric_value END) as meter_active_power_kw,
        MAX(m.asset_name) as meter_name,
        MAX(m.asset_id) as meter_id
    FROM mart.mart_meter_performance_5min m
    WHERE m.site_name = %L
        AND m.date_key >= %L::date
        AND m.date_key < %L::date
        AND m.metric_name IN (''positive_active_energy'', ''negative_active_energy'', ''active_power'')
    GROUP BY m.timestamp, m.date_key
),
sensor_pivot AS (
    SELECT 
        s.timestamp,
        s.date_key,
        MAX(CASE 
            WHEN s.metric_name = ''irradiance'' 
            AND sc.sensor_type = ''GHI'' 
            THEN s.metric_value 
        END) as sensor_ghi_w_m2,
        MAX(CASE 
            WHEN s.metric_name = ''irradiance'' 
            AND sc.sensor_type = ''POA'' 
            THEN s.metric_value 
        END) as sensor_poa_w_m2,
        MAX(CASE WHEN s.metric_name = ''temperature'' THEN s.metric_value END) as sensor_temperature_c,
        MAX(s.asset_name) as sensor_name,
        MAX(s.asset_id) as sensor_id
    FROM mart.mart_sensor_measurements_5min s
    LEFT JOIN dbt.seed_sensor_config sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = ''fusionsolar'' THEN ''FS''
                WHEN s.system = ''isolarcloud'' THEN ''ISO''
                ELSE UPPER(LEFT(s.system, 3))
            END, ''_'', sc.device_id
        )
    WHERE s.site_name = %L
        AND s.date_key >= %L::date
        AND s.date_key < %L::date
        AND s.metric_name IN (''irradiance'', ''temperature'')
    GROUP BY s.timestamp, s.date_key
),
inverter_pivot AS (
    SELECT 
        i.timestamp,
        i.date_key,
        -- Get all inverters for this site
        MAX(CASE 
            WHEN i.asset_id = (
                SELECT asset_id 
                FROM dbt.dim_assets 
                WHERE site_name = %L 
                AND asset_level = ''Device''
                AND (asset_name ILIKE ''%%inverter%%'')
                ORDER BY asset_id
                LIMIT 1 OFFSET 0
            )
            AND i.metric_name = ''inv_active_power''
            THEN i.metric_value 
        END) as inverter_1_active_power_kw,
        MAX(CASE 
            WHEN i.asset_id = (
                SELECT asset_id 
                FROM dbt.dim_assets 
                WHERE site_name = %L 
                AND asset_level = ''Device''
                AND (asset_name ILIKE ''%%inverter%%'')
                ORDER BY asset_id
                LIMIT 1 OFFSET 1
            )
            AND i.metric_name = ''inv_active_power''
            THEN i.metric_value 
        END) as inverter_2_active_power_kw,
        MAX(CASE 
            WHEN i.asset_id = (
                SELECT asset_id 
                FROM dbt.dim_assets 
                WHERE site_name = %L 
                AND asset_level = ''Device''
                AND (asset_name ILIKE ''%%inverter%%'')
                ORDER BY asset_id
                LIMIT 1 OFFSET 2
            )
            AND i.metric_name = ''inv_active_power''
            THEN i.metric_value 
        END) as inverter_3_active_power_kw,
        COUNT(DISTINCT CASE 
            WHEN i.metric_name = ''inv_active_power'' 
            THEN i.asset_id 
        END) as total_inverter_count
    FROM mart.mart_inverter_performance_5min i
    WHERE i.site_name = %L
        AND i.date_key >= %L::date
        AND i.date_key < %L::date
        AND i.metric_name = ''inv_active_power''
    GROUP BY i.timestamp, i.date_key
)
SELECT 
    COALESCE(m.timestamp, s.timestamp, i.timestamp) as timestamp,
    COALESCE(m.date_key, s.date_key, i.date_key) as date_key,
    m.meter_positive_energy_kwh,
    m.meter_negative_energy_kwh,
    m.meter_active_power_kw,
    m.meter_name,
    m.meter_id,
    s.sensor_ghi_w_m2,
    s.sensor_poa_w_m2,
    s.sensor_temperature_c,
    s.sensor_name,
    s.sensor_id,
    i.inverter_1_active_power_kw,
    i.inverter_2_active_power_kw,
    i.inverter_3_active_power_kw,
    i.total_inverter_count
FROM meter_pivot m
FULL OUTER JOIN sensor_pivot s 
    ON m.timestamp = s.timestamp
FULL OUTER JOIN inverter_pivot i 
    ON COALESCE(m.timestamp, s.timestamp) = i.timestamp
ORDER BY timestamp;

-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_perf_5min_%s_unique_timestamp 
    ON mart.%I(timestamp);
CREATE INDEX IF NOT EXISTS idx_mv_perf_5min_%s_date 
    ON mart.%I(date_key);
CREATE INDEX IF NOT EXISTS idx_mv_perf_5min_%s_date_timestamp 
    ON mart.%I(date_key, timestamp);

-- Grant permissions
GRANT SELECT ON mart.%I TO PUBLIC;

-- Create view wrapper
CREATE OR REPLACE VIEW mart.%I AS
SELECT * 
FROM mart.%I
ORDER BY timestamp;

GRANT SELECT ON mart.%I TO PUBLIC;

',
                mv_name,
                site_record.site_name,
                year_val,
                mv_name,
                site_record.site_name,
                year_val::TEXT || '-01-01',
                (year_val + 1)::TEXT || '-01-01',
                site_record.site_name,
                year_val::TEXT || '-01-01',
                (year_val + 1)::TEXT || '-01-01',
                site_record.site_name,
                site_record.site_name,
                site_record.site_name,
                site_record.site_name,
                year_val::TEXT || '-01-01',
                (year_val + 1)::TEXT || '-01-01',
                site_record.site_name,
                year_val::TEXT || '-01-01',
                (year_val + 1)::TEXT || '-01-01',
                LOWER(REPLACE(REPLACE(site_record.site_name, ' ', '_'), '-', '_')) || '_' || year_val::TEXT,
                mv_name,
                LOWER(REPLACE(REPLACE(site_record.site_name, ' ', '_'), '-', '_')) || '_' || year_val::TEXT,
                mv_name,
                LOWER(REPLACE(REPLACE(site_record.site_name, ' ', '_'), '-', '_')) || '_' || year_val::TEXT,
                mv_name,
                mv_name,
                view_name,
                mv_name,
                view_name
            );
            
            -- Output generated SQL (for review)
            RAISE NOTICE 'Generated SQL for: % (Site: %, Year: %)', mv_name, site_record.site_name, year_val;
            
            -- Uncomment to execute directly:
            -- EXECUTE sql_text;
        END LOOP;
    END LOOP;
END $$;

-- ============================================================================
-- Step 2: Alternative - Generate SQL file for manual execution
-- ============================================================================

-- This approach generates SQL statements to a file
-- Run this in psql with output redirection:
-- psql -d MMSR -f 04_generate_all_sites.sql -o generated_mv_scripts.sql

-- Note: The DO block above will generate NOTICE messages
-- For actual SQL generation, you may want to use a Python script instead

