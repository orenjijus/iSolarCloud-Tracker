{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['timestamp', 'asset_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp', 'asset_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute sensor measurements fact table
-- Built directly from staging (no intermediate layer)
-- Filters by metric_group = 'sensor' from seed_metric_mapper
WITH isolarcloud_sensors AS (
    SELECT 
        p.timestamp,
        p.device_ps_key,
        p.metric_id,
        p.metric_value,
        d.device_name,
        s.ps_name as site_name
    FROM {{ ref('stg_isolarcloud__perf_unpivoted') }} p
    JOIN {{ ref('stg_isolarcloud__devices') }} d ON p.device_ps_key = d.device_ps_key
    JOIN {{ ref('stg_isolarcloud__sites') }} s ON d.ps_id = s.ps_id
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'isolarcloud'
        AND mm.metric_group = 'sensor'
        AND mm.used = 'yes'
    WHERE d.device_type = 5  -- Sensors/Weather stations (device_type 5 = Meteo Station)
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND p.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND p.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_ps_ids', none) %}
                -- Filter by specific sites
                AND d.ps_id = ANY(string_to_array('{{ var("reingest_ps_ids") }}', ',')::VARCHAR[])
            {% endif %}
            {% if var('reingest_device_ids', none) %}
                -- Filter by specific device IDs
                AND p.device_ps_key = ANY(string_to_array('{{ var("reingest_device_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            -- Default incremental
            AND p.timestamp > (
                SELECT MAX(timestamp) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            )
        {% endif %}
    {% endif %}
),

fusionsolar_sensors AS (
    SELECT 
        p.timestamp,
        p.dev_id as device_ps_key,
        p.metric_id,
        p.metric_value,
        p.device_name,
        p.plant_name as site_name
    FROM {{ ref('stg_fusionsolar__perf_unpivoted') }} p
    JOIN {{ ref('stg_fusionsolar__devices') }} d ON p.dev_id = d.dev_id
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'fusionsolar'
        AND mm.metric_group = 'sensor'
        AND mm.used = 'yes'
    WHERE d.dev_type_id = 10  -- Sensors (dev_type_id 10 = Meteo Station)
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND p.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND p.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_plant_codes', none) %}
                -- Filter by specific plants
                AND d.plant_code = ANY(string_to_array('{{ var("reingest_plant_codes") }}', ',')::VARCHAR[])
            {% endif %}
            {% if var('reingest_device_ids', none) %}
                -- Filter by specific device IDs
                AND p.dev_id = ANY(string_to_array('{{ var("reingest_device_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            -- Default incremental
            AND p.timestamp > (
                SELECT MAX(timestamp) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            )
        {% endif %}
    {% endif %}
),

unified_sensors AS (
    SELECT 
        'isolarcloud' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        CAST(metric_value AS NUMERIC) as metric_value
    FROM isolarcloud_sensors
    
    UNION ALL
    
    SELECT 
        'fusionsolar' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        CAST(metric_value AS NUMERIC) as metric_value
    FROM fusionsolar_sensors
)

SELECT 
    s.timestamp,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    s.metric_id,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    s.metric_value,
    sc.sensor_type,
    sc.dev_name as sensor_dev_name
FROM unified_sensors s
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT(
        CASE 
            WHEN s.system = 'fusionsolar' THEN 'FS'
            WHEN s.system = 'isolarcloud' THEN 'ISO'
            ELSE UPPER(LEFT(s.system, 3))
        END, '_', s.device_ps_key
    ) = da.asset_id
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(s.timestamp)
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON s.metric_id = m.metric_id 
    AND m.used = 'yes'
LEFT JOIN {{ ref('seed_sensor_config') }} sc 
    ON s.device_ps_key = sc.device_id
WHERE s.metric_value IS NOT NULL
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: ensure we only process the re-ingest range
        AND s.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
        AND s.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
    {% else %}
        -- Default incremental: filtering already done per-system in CTEs above
        -- No additional filter needed here to avoid filtering out data from one system
        -- when the other system has newer timestamps
    {% endif %}
{% endif %}

