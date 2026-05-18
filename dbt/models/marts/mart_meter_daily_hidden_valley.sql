{{ config(
    materialized='table',
    schema='mart',
    indexes=[
        {'columns': ['date_key'], 'type': 'btree'},
        {'columns': ['site_name', 'date_key'], 'type': 'btree'},
        {'columns': ['asset_id', 'date_key'], 'type': 'btree'}
    ]
) }}

-- Daily positive/negative active energy untuk Hidden Valley (Power BI).
-- Revenue meters: plant NE=78317340 (site_name "Hidden Valley Load Meter" di staging).
-- EMMA / Meter Villa: plant NE=60951882 (site_name "Hidden Valley").
-- Power BI memetakan asset_id → PLN / Pump / Pool / Onsen / Meter Villa via calculated table dim_meter.

{% set hv_meter_asset_ids = (
    'FS_AM00102555606343',
    'FS_AM01102555606343',
    'FS_AM02102555606343',
    'FS_AM03102555606343',
    'FS_NS2451145483'
) %}

WITH meter_daily_stats AS (
    SELECT
        m.date_key,
        m.asset_id,
        m.metric_id,
        m.metric_name,
        FALSE AS is_polarity_swapped,
        MIN(
            CASE
                WHEN m.metric_value >= 0 AND m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
                WHEN m.metric_value >= 0 THEN m.metric_value
                ELSE NULL
            END
        ) AS min_value,
        MAX(
            CASE
                WHEN m.metric_value >= 0 AND m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
                WHEN m.metric_value >= 0 THEN m.metric_value
                ELSE NULL
            END
        ) AS max_value,
        (ARRAY_AGG(
            CASE
                WHEN m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
                ELSE m.metric_value
            END
            ORDER BY m.timestamp
        ))[1] AS first_value,
        (ARRAY_AGG(
            CASE
                WHEN m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
                ELSE m.metric_value
            END
            ORDER BY m.timestamp DESC
        ))[1] AS last_value
    FROM {{ ref('mart_meter_performance_5min') }} m
    WHERE m.asset_id IN (
        '{{ hv_meter_asset_ids[0] }}',
        '{{ hv_meter_asset_ids[1] }}',
        '{{ hv_meter_asset_ids[2] }}',
        '{{ hv_meter_asset_ids[3] }}',
        '{{ hv_meter_asset_ids[4] }}'
    )
      AND m.metric_id IN ('active_cap', 'reverse_active_cap')
      AND m.metric_value >= 0
    GROUP BY m.date_key, m.asset_id, m.metric_id, m.metric_name
),

meter_with_prev_day AS (
    SELECT
        d1.date_key,
        d1.asset_id,
        d1.metric_id,
        d1.metric_name,
        d1.is_polarity_swapped,
        d1.min_value,
        d1.max_value,
        d1.first_value,
        d1.last_value,
        d2.max_value AS prev_day_max,
        CASE
            WHEN d2.max_value IS NOT NULL
                AND (
                    ABS(d1.first_value - d2.max_value) < 0.01
                    OR (d2.max_value > 0 AND ABS(d1.first_value - d2.max_value) / d2.max_value <= 0.0015)
                )
            THEN TRUE
            ELSE FALSE
        END AS is_cumulative
    FROM meter_daily_stats d1
    LEFT JOIN meter_daily_stats d2
        ON d1.asset_id = d2.asset_id
        AND d1.metric_id = d2.metric_id
        AND d1.metric_name = d2.metric_name
        AND d2.date_key = d1.date_key - INTERVAL '1 day'
),

daily_energy_from_meters AS (
    SELECT
        date_key,
        asset_id,
        metric_id,
        metric_name,
        is_polarity_swapped,
        SUM(
            CASE
                WHEN is_cumulative AND prev_day_max IS NOT NULL THEN max_value - prev_day_max
                ELSE max_value - min_value
            END
        ) AS daily_energy_kwh
    FROM meter_with_prev_day
    GROUP BY date_key, asset_id, metric_id, metric_name, is_polarity_swapped
),

meter_energy AS (
    SELECT
        date_key,
        asset_id,
        is_polarity_swapped,
        SUM(CASE WHEN metric_id = 'active_cap' THEN daily_energy_kwh ELSE 0 END) AS daily_positive_active_energy_kwh,
        SUM(CASE WHEN metric_id = 'reverse_active_cap' THEN daily_energy_kwh ELSE 0 END) AS daily_negative_active_energy_kwh
    FROM daily_energy_from_meters
    GROUP BY date_key, asset_id, is_polarity_swapped
)

SELECT
    me.date_key,
    'Hidden Valley' AS site_name,
    me.asset_id,
    da.asset_name AS meter_name,
    da.asset_name AS meter_label,
    me.daily_positive_active_energy_kwh,
    me.daily_negative_active_energy_kwh,
    ABS(
        COALESCE(me.daily_negative_active_energy_kwh, 0) - COALESCE(me.daily_positive_active_energy_kwh, 0)
    ) AS daily_energy_kwh
FROM meter_energy me
LEFT JOIN {{ ref('dim_assets') }} da
    ON da.asset_id = me.asset_id
    AND da.asset_level = 'Device'
