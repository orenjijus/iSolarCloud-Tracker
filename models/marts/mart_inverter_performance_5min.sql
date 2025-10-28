{{ config(
    materialized='incremental',
    unique_key=['timestamp_5min', 'asset_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp_5min', 'asset_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'},
        {'columns': ['timestamp_5min'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute inverter performance fact table
SELECT 
    i.timestamp_5min,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    i.metric_id,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    i.metric_value
FROM {{ ref('int_inverters_unified_5min') }} i
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT(
        UPPER(LEFT(i.system, 3)), '_', i.device_ps_key
    ) = da.asset_id
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(i.timestamp_5min)
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON i.metric_id = m.metric_id 
    AND m.used = 'yes'
WHERE i.metric_value IS NOT NULL
{% if is_incremental() %}
    AND i.timestamp_5min > (SELECT MAX(timestamp_5min) FROM {{ this }})
{% endif %}

