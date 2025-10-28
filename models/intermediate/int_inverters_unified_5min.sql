{{ config(
    materialized='incremental',
    unique_key=['timestamp_5min', 'device_ps_key', 'system', 'metric_id'],
    meta={'hyperscale': true}
) }}

-- Unified inverter data at 5-minute resolution
WITH isolarcloud_inverters AS (
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
    WHERE d.device_type = 1  -- Inverters
    {% if is_incremental() %}
        AND DATE_TRUNC('minute', p.timestamp) > (SELECT MAX(timestamp_5min) FROM {{ this }} WHERE system = 'isolarcloud')
    {% endif %}
),

fusionsolar_inverters AS (
    SELECT 
        p.timestamp,
        p.dev_id as device_ps_key,
        p.metric_id,
        p.metric_value,
        p.device_name,
        p.plant_name as site_name
    FROM {{ ref('stg_fusionsolar__perf_unpivoted') }} p
    JOIN {{ ref('stg_fusionsolar__devices') }} d ON p.dev_id = d.dev_id
    WHERE d.dev_type_id = 1  -- Inverters
    {% if is_incremental() %}
        AND DATE_TRUNC('minute', p.timestamp) > (SELECT MAX(timestamp_5min) FROM {{ this }} WHERE system = 'fusionsolar')
    {% endif %}
)

SELECT 
    'isolarcloud' as system,
    DATE_TRUNC('minute', timestamp) as timestamp_5min,
    device_ps_key,
    device_name,
    site_name,
    metric_id,
    AVG(CAST(metric_value AS NUMERIC)) as metric_value
FROM isolarcloud_inverters
GROUP BY timestamp_5min, device_ps_key, device_name, site_name, metric_id

UNION ALL

SELECT 
    'fusionsolar' as system,
    DATE_TRUNC('minute', timestamp) as timestamp_5min,
    device_ps_key,
    device_name,
    site_name,
    metric_id,
    AVG(CAST(metric_value AS NUMERIC)) as metric_value
FROM fusionsolar_inverters
GROUP BY timestamp_5min, device_ps_key, device_name, site_name, metric_id

