{{ config(
    materialized='incremental',
    unique_key=['timestamp', 'device_ps_key', 'system', 'metric_id'],
    meta={'hyperscale': true}
) }}

-- Unified meter data from both systems
WITH isolarcloud_meters AS (
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
    WHERE d.device_type = 7  -- Meters
    {% if is_incremental() %}
        AND p.timestamp > (SELECT MAX(timestamp) FROM {{ this }} WHERE system = 'isolarcloud')
    {% endif %}
),

fusionsolar_meters AS (
    SELECT 
        p.timestamp,
        p.dev_id as device_ps_key,
        p.metric_id,
        p.metric_value,
        p.device_name,
        p.plant_name as site_name
    FROM {{ ref('stg_fusionsolar__perf_unpivoted') }} p
    JOIN {{ ref('stg_fusionsolar__devices') }} d ON p.dev_id = d.dev_id
    WHERE d.dev_type_id = 17  -- Meters (dev_type_id 17 = Meter)
    {% if is_incremental() %}
        AND p.timestamp > (SELECT MAX(timestamp) FROM {{ this }} WHERE system = 'fusionsolar')
    {% endif %}
),

-- Convert to text and filter invalid values before final select
isolarcloud_meters_clean AS (
    SELECT 
        'isolarcloud' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        CAST(metric_value AS TEXT) as metric_value_text
    FROM isolarcloud_meters
),

fusionsolar_meters_clean AS (
    SELECT 
        'fusionsolar' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        CAST(metric_value AS TEXT) as metric_value_text
    FROM fusionsolar_meters
)

SELECT 
    system,
    timestamp,
    device_ps_key,
    device_name,
    site_name,
    metric_id,
    CAST(metric_value_text AS NUMERIC) as metric_value
FROM isolarcloud_meters_clean
WHERE metric_value_text NOT IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity')
    AND metric_value_text IS NOT NULL

UNION ALL

SELECT 
    system,
    timestamp,
    device_ps_key,
    device_name,
    site_name,
    metric_id,
    CAST(metric_value_text AS NUMERIC) as metric_value
FROM fusionsolar_meters_clean
WHERE metric_value_text NOT IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity')
    AND metric_value_text IS NOT NULL

