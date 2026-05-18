{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['date_key', 'asset_id', 'sensor_type'],
    indexes=[
        {'columns': ['date_key', 'asset_id'], 'type': 'btree'},
        {'columns': ['date_key', 'site_name', 'sensor_type'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- Daily sensor aggregations per device
-- Aggregates all sensor types (GHI, POA, Weather) per device per day
-- This allows debugging individual sensor values before weighted averages
WITH sensor_5min AS (
    SELECT 
        s.timestamp,
        s.date_key,
        s.asset_id,
        s.asset_name,
        s.site_name,
        s.system,
        s.metric_id,
        s.metric_name,
        s.metric_value,
        s.metric_unit,
        sc.sensor_type,
        sc.dev_name as sensor_dev_name,
        sc.device_id,
        -- Get capacity for POA sensors (handle comma decimal separator)
        CASE 
            WHEN sc.sensor_type = 'POA' AND sc.sensor_capacity IS NOT NULL AND TRIM(sc.sensor_capacity::text) != ''
            THEN CAST(REPLACE(sc.sensor_capacity::text, ',', '.') AS NUMERIC)
            ELSE NULL
        END as sensor_capacity_kwp
    FROM {{ ref('mart_sensor_measurements_5min') }} s
    LEFT JOIN {{ ref('seed_sensor_config') }} sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    WHERE s.metric_value IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND s.date_key >= '{{ var("reingest_start_date") }}'::date
            AND s.date_key <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            -- Default incremental: only new data
            AND s.date_key > (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
),

-- Daily aggregations per sensor (all metrics in one row per sensor per day)
daily_sensor_metrics AS (
    SELECT 
        date_key,
        asset_id,
        asset_name,
        site_name as physical_site_name,
        system,
        sensor_type,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        -- Convert units to standard format
        -- For daily_irradiance: convert to kWh/m²
        -- FusionSolar: MJ/m² → divide by 3.6 to get kWh/m²
        -- iSolarCloud: Wh/㎡ → divide by 1000 to get kWh/m²
        MAX(CASE 
            WHEN metric_name = 'daily_irradiance' THEN
                CASE 
                    WHEN system = 'fusionsolar' THEN metric_value / 3.6  -- FusionSolar: MJ/m² to kWh/m²
                    WHEN system = 'isolarcloud' AND metric_unit = 'Wh/㎡' THEN metric_value / 1000.0  -- iSolarCloud: Wh/m² to kWh/m²
                    WHEN metric_unit = 'MJ/m²' THEN metric_value / 3.6  -- MJ/m² to kWh/m²
                    WHEN metric_unit = 'W/m²' THEN metric_value / 1000.0  -- W/m² to kW/m² (for daily, this should be kWh/m²)
                    ELSE metric_value
                END
            ELSE NULL
        END) as daily_irradiance_kwh_m2,
        -- For irradiance (instantaneous): convert to W/m²
        AVG(CASE 
            WHEN metric_name = 'irradiance' THEN
                CASE 
                    WHEN metric_unit = 'W/m²' THEN metric_value
                    WHEN metric_unit = 'kW/m²' THEN metric_value * 1000.0
                    ELSE metric_value
                END
            ELSE NULL
        END) as avg_irradiance_w_m2,
        MAX(CASE 
            WHEN metric_name = 'irradiance' THEN
                CASE 
                    WHEN metric_unit = 'W/m²' THEN metric_value
                    WHEN metric_unit = 'kW/m²' THEN metric_value * 1000.0
                    ELSE metric_value
                END
            ELSE NULL
        END) as max_irradiance_w_m2,
        -- For horizontal_irradiation: convert to kWh/m² (same as daily_irradiance)
        MAX(CASE 
            WHEN metric_name = 'horizontal_irradiation' THEN
                CASE 
                    WHEN system = 'fusionsolar' THEN metric_value / 3.6  -- FusionSolar: MJ/m² to kWh/m²
                    WHEN system = 'isolarcloud' AND metric_unit = 'Wh/㎡' THEN metric_value / 1000.0
                    WHEN metric_unit = 'MJ/m²' THEN metric_value / 3.6
                    ELSE metric_value
                END
            ELSE NULL
        END) as daily_horizontal_irradiation_kwh_m2,
        -- Temperature metrics (already in °C, just aggregate)
        AVG(CASE WHEN metric_name = 'ambient_temperature' THEN metric_value ELSE NULL END) as avg_ambient_temperature_c,
        MAX(CASE WHEN metric_name = 'ambient_temperature' THEN metric_value ELSE NULL END) as max_ambient_temperature_c,
        MIN(CASE WHEN metric_name = 'ambient_temperature' THEN metric_value ELSE NULL END) as min_ambient_temperature_c,
        AVG(CASE WHEN metric_name = 'module_temperature' THEN metric_value ELSE NULL END) as avg_module_temperature_c,
        MAX(CASE WHEN metric_name = 'module_temperature' THEN metric_value ELSE NULL END) as max_module_temperature_c,
        MIN(CASE WHEN metric_name = 'module_temperature' THEN metric_value ELSE NULL END) as min_module_temperature_c,
        -- Wind metrics
        AVG(CASE WHEN metric_name = 'wind_speed' THEN metric_value ELSE NULL END) as avg_wind_speed_m_s,
        MAX(CASE WHEN metric_name = 'wind_speed' THEN metric_value ELSE NULL END) as max_wind_speed_m_s,
        AVG(CASE WHEN metric_name = 'wind_direction' THEN metric_value ELSE NULL END) as avg_wind_direction_deg,
        -- Other weather metrics
        AVG(CASE WHEN metric_name = 'humidity' THEN metric_value ELSE NULL END) as avg_humidity_pct,
        MAX(CASE WHEN metric_name = 'humidity' THEN metric_value ELSE NULL END) as max_humidity_pct,
        MIN(CASE WHEN metric_name = 'humidity' THEN metric_value ELSE NULL END) as min_humidity_pct,
        SUM(CASE WHEN metric_name = 'rainfall' THEN metric_value ELSE NULL END) as total_rainfall_mm,
        -- Data quality metrics
        COUNT(*) as measurement_count,
        COUNT(DISTINCT timestamp) as timestamp_count
    FROM sensor_5min
    WHERE sensor_type IS NOT NULL  -- Only include sensors with defined type
    GROUP BY 
        date_key,
        asset_id,
        asset_name,
        site_name,
        system,
        sensor_type,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp
),

-- Check if logical site has active POA_OVERRIDE (to exclude physical sensors from that site)
logical_site_override_check AS (
    SELECT DISTINCT
        logical_site_id,
        MIN(effective_date_start) as override_start_date,
        MAX(CASE WHEN effective_date_end IS NULL OR effective_date_end::text = '' THEN NULL ELSE effective_date_end END) as override_end_date
    FROM {{ ref('seed_sensor_site_mapping') }}
    WHERE mapping_type = 'POA_OVERRIDE'
    GROUP BY logical_site_id
),

-- Apply POA override: change site assignment from physical to logical site
-- Exclude physical sensors from logical site when POA_OVERRIDE is active
daily_sensor_with_override AS (
    SELECT 
        dsm.date_key,
        dsm.asset_id,
        dsm.asset_name,
        -- Apply override: use logical_site_id if override exists, else use physical_site_name
        COALESCE(
            CASE 
                WHEN ssm.mapping_type = 'POA_OVERRIDE' 
                    AND (ssm.effective_date_start IS NULL OR dsm.date_key >= ssm.effective_date_start)
                    AND (
                        ssm.effective_date_end IS NULL 
                        OR ssm.effective_date_end::text = ''
                        OR (ssm.effective_date_end IS NOT NULL AND ssm.effective_date_end::text != '' AND dsm.date_key <= ssm.effective_date_end::text::date)
                    )
                THEN ssm.logical_site_id  -- Logical site (e.g., MMKI II)
                ELSE NULL
            END,
            dsm.physical_site_name  -- Physical site (e.g., MMKI I)
        ) as site_name,
        dsm.system,
        dsm.sensor_type,
        dsm.sensor_dev_name,
        dsm.device_id,
        dsm.sensor_capacity_kwp,
        dsm.daily_irradiance_kwh_m2,
        dsm.avg_irradiance_w_m2,
        dsm.max_irradiance_w_m2,
        dsm.daily_horizontal_irradiation_kwh_m2,
        dsm.avg_ambient_temperature_c,
        dsm.max_ambient_temperature_c,
        dsm.min_ambient_temperature_c,
        dsm.avg_module_temperature_c,
        dsm.max_module_temperature_c,
        dsm.min_module_temperature_c,
        dsm.avg_wind_speed_m_s,
        dsm.max_wind_speed_m_s,
        dsm.avg_wind_direction_deg,
        dsm.avg_humidity_pct,
        dsm.max_humidity_pct,
        dsm.min_humidity_pct,
        dsm.total_rainfall_mm,
        dsm.measurement_count,
        dsm.timestamp_count
    FROM daily_sensor_metrics dsm
    LEFT JOIN {{ ref('seed_sensor_site_mapping') }} ssm
        ON dsm.device_id = ssm.device_id  -- Match by device_id
        AND ssm.mapping_type = 'POA_OVERRIDE'
        AND dsm.sensor_type = 'POA'  -- Only apply override to POA sensors
    LEFT JOIN logical_site_override_check lsoc
        ON dsm.physical_site_name = lsoc.logical_site_id
    WHERE 
        -- Exclude physical sensors from logical site when POA_OVERRIDE is active
        -- Only exclude if:
        -- 1. Physical site name matches logical site (sensor is from the logical site itself)
        -- 2. POA_OVERRIDE is active for that logical site
        -- 3. Sensor is not part of the override (doesn't have ssm.mapping_type = 'POA_OVERRIDE')
        -- 4. Date is within override effective period
        NOT (
            dsm.physical_site_name = lsoc.logical_site_id
            AND lsoc.logical_site_id IS NOT NULL
            AND ssm.mapping_type IS NULL  -- Sensor is NOT part of override
            AND dsm.sensor_type = 'POA'
            AND (
                lsoc.override_start_date IS NULL 
                OR dsm.date_key >= lsoc.override_start_date
            )
            AND (
                lsoc.override_end_date IS NULL 
                OR dsm.date_key <= lsoc.override_end_date
            )
        )
)

-- Final output: one row per sensor per day (already aggregated)
SELECT 
    date_key,
    asset_id,
    asset_name,
    site_name,
    system,
    sensor_type,
    sensor_dev_name,
    device_id,
    sensor_capacity_kwp,
    -- Irradiance metrics
    daily_irradiance_kwh_m2,
    avg_irradiance_w_m2,
    max_irradiance_w_m2,
    daily_horizontal_irradiation_kwh_m2,
    -- Temperature metrics
    avg_ambient_temperature_c,
    max_ambient_temperature_c,
    min_ambient_temperature_c,
    avg_module_temperature_c,
    max_module_temperature_c,
    min_module_temperature_c,
    -- Wind metrics
    avg_wind_speed_m_s,
    max_wind_speed_m_s,
    avg_wind_direction_deg,
    -- Other weather metrics
    avg_humidity_pct,
    max_humidity_pct,
    min_humidity_pct,
    total_rainfall_mm,
    -- Data quality
    measurement_count,
    timestamp_count
FROM daily_sensor_with_override

