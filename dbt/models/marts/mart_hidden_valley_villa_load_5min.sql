{{ config(
    materialized='incremental',
    tags=['intraday'],
    schema='mart',
    unique_key=['timestamp', 'site_id'],
    indexes=[
        {'columns': ['timestamp', 'site_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- Hidden Valley: beban villa (kW) = total inverter active power + SmartAssistant active power.
-- Grain: timestamp_5min × site
--
-- Notes:
-- - Both sources can come in W or kW depending on platform; we normalize to kW via metric_unit.
-- - Full outer join keeps rows if one side is missing.

{% set site_name = var('hidden_valley_site_name', 'Hidden Valley') %}

WITH site_dim AS (
    SELECT
        da.asset_id AS site_id,
        da.site_name,
        da.system
    FROM {{ ref('dim_assets') }} da
    WHERE da.asset_level = 'Site'
      AND TRIM(da.site_name) = '{{ site_name }}'
    LIMIT 1
),

inv_site AS (
    SELECT
        i.timestamp,
        i.date_key,
        SUM(
            CASE
                WHEN LOWER(COALESCE(i.metric_unit, '')) = 'w' THEN i.metric_value / 1000.0
                ELSE i.metric_value
            END
        ) AS inv_active_power_kw
    FROM {{ ref('mart_inverter_performance_5min') }} i
    WHERE TRIM(i.site_name) = '{{ site_name }}'
      AND i.metric_name = 'inv_active_power'
      AND i.metric_value IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND i.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND i.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            AND i.timestamp > (SELECT COALESCE(MAX(timestamp), '1900-01-01'::timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
    GROUP BY i.timestamp, i.date_key
),

sa_site AS (
    SELECT
        m.timestamp,
        m.date_key,
        SUM(
            CASE
                WHEN LOWER(COALESCE(m.metric_unit, '')) = 'w' THEN m.metric_value / 1000.0
                ELSE m.metric_value
            END
        ) AS sa_active_power_kw
    FROM {{ ref('mart_meter_performance_5min') }} m
    WHERE TRIM(m.site_name) = '{{ site_name }}'
      AND m.metric_name = 'sa_active_power'
      AND m.metric_value IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND m.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND m.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            AND m.timestamp > (SELECT COALESCE(MAX(timestamp), '1900-01-01'::timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
    GROUP BY m.timestamp, m.date_key
),

joined AS (
    SELECT
        COALESCE(inv.timestamp, sa.timestamp) AS timestamp,
        COALESCE(inv.date_key, sa.date_key) AS date_key,
        inv.inv_active_power_kw,
        sa.sa_active_power_kw
    FROM inv_site inv
    FULL OUTER JOIN sa_site sa
        ON inv.timestamp = sa.timestamp
)

SELECT
    j.timestamp,
    j.date_key,
    (EXTRACT(HOUR FROM j.timestamp) * 60 + EXTRACT(MINUTE FROM j.timestamp))::int / 5 AS minute_key,
    sd.site_id,
    sd.site_name,
    sd.system,
    CAST(j.inv_active_power_kw AS DECIMAL(18,6)) AS inv_active_power_kw,
    CAST(j.sa_active_power_kw AS DECIMAL(18,6)) AS sa_active_power_kw,
    CAST((COALESCE(j.inv_active_power_kw, 0) + COALESCE(j.sa_active_power_kw, 0)) AS DECIMAL(18,6)) AS beban_villa_kw
FROM joined j
CROSS JOIN site_dim sd
WHERE j.timestamp IS NOT NULL
ORDER BY j.timestamp

