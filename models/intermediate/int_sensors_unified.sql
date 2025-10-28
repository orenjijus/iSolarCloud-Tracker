{{ config(
    materialized='incremental',
    unique_key=['timestamp', 'device_ps_key', 'system', 'metric_id'],
    meta={'hyperscale': true}
) }}

-- Unified sensor/weather station data
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
    WHERE d.device_type = 5  -- Sensors/Weather stations (device_type 5 = Meteo Station)
    {% if is_incremental() %}
        AND p.timestamp > (SELECT MAX(timestamp) FROM {{ this }} WHERE system = 'isolarcloud')
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
    WHERE d.dev_type_id = 10  -- Sensors (dev_type_id 10 = Meteo Station)
    {% if is_incremental() %}
        AND p.timestamp > (SELECT MAX(timestamp) FROM {{ this }} WHERE system = 'fusionsolar')
    {% endif %}
)

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

