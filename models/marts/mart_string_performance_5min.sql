{{ config(
    materialized='incremental',
    unique_key=['timestamp_5min', 'asset_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp_5min', 'asset_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'},
        {'columns': ['measurement_type'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute string-level performance fact table
SELECT 
    s.timestamp_5min,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    s.metric_id,
    s.measurement_type,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    s.metric_value
FROM {{ ref('int_strings_unified_5min') }} s
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT(
        UPPER(LEFT(s.system, 3)), '_', s.device_ps_key
    ) = da.asset_id
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(s.timestamp_5min)
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON s.metric_id = m.metric_id 
    AND m.used = 'yes'
WHERE s.metric_value IS NOT NULL
{% if is_incremental() %}
    AND s.timestamp_5min > (SELECT MAX(timestamp_5min) FROM {{ this }})
{% endif %}

