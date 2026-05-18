{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['timestamp', 'inverter_id'],
    indexes=[
        {'columns': ['timestamp', 'inverter_id'], 'type': 'btree'},
        {'columns': ['timestamp', 'site_id'], 'type': 'btree'},
        {'columns': ['inverter_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute inverter-level calculated metrics
-- Purpose: Calculate inverter availability with MIT
-- Grain: timestamp_5min × inverter_id
-- Depends on: fact_sensor_calculations_5min (for MIT)

WITH 
-- Step 1: Get inverter active power from mart_inverter_performance_5min
inverter_power AS (
    SELECT 
        i.timestamp,
        i.date_key,
        i.asset_id as inverter_id,
        i.asset_name as inverter_name,
        i.site_name,
        da.site_id,
        i.system,
        i.metric_value as active_power_kw
    FROM {{ ref('mart_inverter_performance_5min') }} i
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON i.site_name = da.site_name 
        AND da.asset_level = 'Site'
    WHERE i.metric_name = 'inv_active_power'
        AND i.metric_value IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND i.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND i.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            AND i.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
),

-- Step 2: Get site-level MIT from fact_sensor_calculations_5min
-- MIT is site-level (same for all inverters at same site)
-- IMPORTANT: Use same logic as mart_site_performance_daily
-- Priority: GHI (same site, non-fallback) > POA (same site) > GHI (fallback from other site)
-- This matches the COALESCE(GHI, POA) logic in daily, then applies GHI fallback
site_mit AS (
    SELECT DISTINCT ON (timestamp, site_id)
        timestamp,
        date_key,
        site_id,
        site_name,
        system,
        -- Treat NULL MIT as 0 (no valid sensor data = no sun = MIT 0)
        COALESCE(mit, 0) as mit,
        -- Priority: 1 = GHI (non-fallback), 2 = POA, 3 = GHI (fallback), 4 = NULL (no valid sensor)
        CASE 
            WHEN mit_irradiance_source = 'GHI' AND is_fallback = FALSE THEN 1
            WHEN mit_irradiance_source = 'POA' THEN 2
            WHEN mit_irradiance_source = 'GHI_FALLBACK' OR is_fallback = TRUE THEN 3
            ELSE 4  -- NULL irradiance source = lowest priority (but MIT = 0)
        END as priority
    FROM {{ ref('fact_sensor_calculations_5min') }}
    -- Allow MIT = NULL (will be converted to 0)
    -- Removed: WHERE mit IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            WHERE timestamp >= '{{ var("reingest_start_date") }}'::timestamp
                AND timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
    ORDER BY timestamp, site_id, 
        -- Priority order: GHI (non-fallback) > POA > GHI (fallback) > NULL
        CASE 
            WHEN mit_irradiance_source = 'GHI' AND is_fallback = FALSE THEN 1
            WHEN mit_irradiance_source = 'POA' THEN 2
            WHEN mit_irradiance_source = 'GHI_FALLBACK' OR is_fallback = TRUE THEN 3
            ELSE 4
        END ASC
),

-- Step 3: Join inverter power with MIT and calculate availability
-- IMPORTANT: Use LEFT JOIN to include inverter data even when MIT data is missing
-- When MIT data is missing, default MIT = 0 (no sun = MIT 0)
-- This ensures inverter data is not lost when sensor data is unavailable
inverter_calculations AS (
    SELECT 
        ip.timestamp,
        ip.date_key,
        ip.inverter_id,
        ip.inverter_name,
        ip.site_id,
        ip.site_name,
        ip.system,
        ip.active_power_kw,
        COALESCE(sm.mit, 0) as mit,  -- Default to 0 if MIT data is missing (no sun = MIT 0)
        -- Inverter availability: CASE WHEN active_power > 0 THEN 1 ELSE 0 END
        CASE 
            WHEN ip.active_power_kw > 0 THEN 1
            ELSE 0
        END as inverter_availability,
        -- Inverter availability with MIT: CASE WHEN active_power > 0 AND mit = 1 THEN 1 ELSE 0 END
        CASE 
            WHEN ip.active_power_kw > 0 AND COALESCE(sm.mit, 0) = 1 THEN 1
            ELSE 0
        END as inverter_availability_with_mit,
        NOW() as calculation_timestamp
    FROM inverter_power ip
    LEFT JOIN site_mit sm  -- LEFT JOIN: include inverter data even when MIT data is missing
        ON ip.timestamp = sm.timestamp
        AND ip.site_id = sm.site_id
)

SELECT 
    timestamp,
    date_key,
    inverter_id,
    inverter_name,
    site_id,
    site_name,
    system,
    active_power_kw,
    mit,
    inverter_availability,
    inverter_availability_with_mit,
    calculation_timestamp
FROM inverter_calculations
ORDER BY timestamp, inverter_id

