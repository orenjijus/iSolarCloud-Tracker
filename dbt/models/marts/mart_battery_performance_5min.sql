{{ config(
    materialized='incremental',
    tags=['intraday'],
    schema='mart',
    unique_key=['timestamp', 'dev_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp', 'dev_id'], 'type': 'btree'},
        {'columns': ['plant_code', 'timestamp'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- Battery 5-min mart: metric dan value dipetakan via seed_metric_mapper (sama seperti meter/inverter).
-- Hanya metric yang metric_group = 'battery' dan used = 'yes': charge_cap, discharge_cap, ch_discharge_power.
WITH fusionsolar_battery AS (
    SELECT 
        p.timestamp,
        p.dev_id,
        p.device_name,
        p.plant_code,
        p.plant_name,
        p.metric_id,
        p.metric_value
    FROM {{ ref('stg_fusionsolar__perf_battery_unpivoted') }} p
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'fusionsolar'
        AND mm.metric_group = 'battery'
        AND mm.used = 'yes'
        AND mm.device_type = 39
    WHERE p.metric_value IS NOT NULL
      AND CAST(p.metric_value AS TEXT) NOT IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity')
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND p.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND p.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            AND p.timestamp > (SELECT COALESCE(MAX(timestamp), '1970-01-01'::timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
)

SELECT 
    b.timestamp,
    da.asset_id,
    da.asset_name,
    da.site_name,
    'fusionsolar' AS system,
    dd.date_key,
    b.metric_id,
    mm.unified_name AS metric_name,
    mm.metric_group,
    mm.metric_unit,
    b.dev_id,
    b.device_name,
    b.plant_code,
    b.plant_name,
    CAST(b.metric_value AS NUMERIC) AS metric_value
FROM fusionsolar_battery b
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT('FS_', b.dev_id) = da.asset_id
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(b.timestamp)
LEFT JOIN {{ ref('seed_metric_mapper') }} mm 
    ON b.metric_id = mm.metric_id 
    AND mm.platform = 'fusionsolar'
    AND mm.metric_group = 'battery'
    AND mm.used = 'yes'
    AND mm.device_type = 39
