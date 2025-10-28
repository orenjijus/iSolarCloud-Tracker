{{ config(
    materialized='incremental',
    unique_key=['timestamp', 'asset_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp', 'asset_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute meter performance fact table
SELECT 
    m.timestamp,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    m.metric_id,
    mm.unified_name as metric_name,
    mm.metric_group,
    mm.metric_unit,
    m.metric_value,
    mc.meter_type,
    mc.voltage_level,
    mc.dev_name as meter_dev_name
FROM {{ ref('int_meters_unified') }} m
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT(
        CASE 
            WHEN m.system = 'fusionsolar' THEN 'FS'
            WHEN m.system = 'isolarcloud' THEN 'ISO'
            ELSE UPPER(LEFT(m.system, 3))
        END, '_', m.device_ps_key
    ) = da.asset_id
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(m.timestamp)
LEFT JOIN {{ ref('seed_metric_mapper') }} mm 
    ON m.metric_id = mm.metric_id 
    AND mm.used = 'yes'
LEFT JOIN {{ ref('seed_meter_config') }} mc 
    ON m.device_ps_key = mc.esn_code
WHERE m.metric_value IS NOT NULL
{% if is_incremental() %}
    AND m.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}

