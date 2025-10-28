{{ config(
    materialized='table',
    indexes=[
        {'columns': ['date_key', 'site_id'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- Daily site-level aggregated performance
WITH daily_inverter_summary AS (
    SELECT 
        date_key,
        site_name,
        metric_id,
        AVG(metric_value) as avg_value,
        MAX(metric_value) as max_value,
        MIN(metric_value) as min_value,
        COUNT(*) as measurement_count
    FROM {{ ref('mart_inverter_performance_5min') }}
    GROUP BY date_key, site_name, metric_id
),

daily_meter_summary AS (
    SELECT 
        date_key,
        site_name,
        metric_id,
        AVG(metric_value) as avg_value,
        MAX(metric_value) as max_value,
        MIN(metric_value) as min_value,
        SUM(metric_value) as total_value,
        COUNT(*) as measurement_count
    FROM {{ ref('mart_meter_performance_5min') }}
    GROUP BY date_key, site_name, metric_id
)

SELECT 
    'inverter' as data_source,
    ds.date_key,
    da.asset_id as site_id,
    da.site_name,
    da.system,
    ds.metric_id,
    m.unified_name as metric_name,
    m.metric_unit,
    ds.avg_value,
    ds.max_value,
    ds.min_value,
    NULL as total_value,
    ds.measurement_count
FROM daily_inverter_summary ds
LEFT JOIN {{ ref('dim_assets') }} da 
    ON da.site_name = ds.site_name 
    AND da.asset_level = 'Site'
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON ds.metric_id = m.metric_id 
    AND m.used = 'yes'

UNION ALL

SELECT 
    'meter' as data_source,
    ds.date_key,
    da.asset_id as site_id,
    da.site_name,
    da.system,
    ds.metric_id,
    m.unified_name as metric_name,
    m.metric_unit,
    ds.avg_value,
    ds.max_value,
    ds.min_value,
    ds.total_value,
    ds.measurement_count
FROM daily_meter_summary ds
LEFT JOIN {{ ref('dim_assets') }} da 
    ON da.site_name = ds.site_name 
    AND da.asset_level = 'Site'
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON ds.metric_id = m.metric_id 
    AND m.used = 'yes'

