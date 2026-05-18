{{ config(
    materialized='incremental',
    tags=['intraday'],
    schema='mart',
    unique_key=['timestamp', 'asset_id'],
    indexes=[
        {'columns': ['timestamp', 'asset_id'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- Corrected meter active power from PA + PB + PC untuk site Hidden Valley.
-- Semua meter Hidden Valley (4 meter) punya PA, PB, PC; 1 meter phase C terbalik → pakai PA+PB-PC, 3 meter lain PA+PB+PC.
-- Scope: meter di plant Hidden Valley (78317340); data dari 2026-02-11.

{% set phase_correction_start_date = var('meter_phase_correction_start_date', '2026-02-11') %}
{% set phase_correction_end_date = var('meter_phase_correction_end_date', '2026-03-18') %}
{% set hidden_valley_meter_plant = var('hidden_valley_meter_plant_code', '78317340') %}

-- Semua meter di plant Hidden Valley (meter plant 78317340)
WITH hidden_valley_meters AS (
    SELECT raw.dev_id
    FROM {{ source('raw', 'fusionsolar_devices') }} raw
    WHERE raw.dev_type_id = 17
      AND TRIM(REPLACE(raw.plant_code::text, 'NE=', '')) = '{{ hidden_valley_meter_plant }}'
),

-- Untuk tiap meter: apakah phase C di-swap (dari override, match by esn_code)
meter_phase_config AS (
    SELECT
        raw.dev_id,
        CASE WHEN UPPER(TRIM(COALESCE(ov.phase_c_swapped::text, ''))) = 'TRUE' THEN TRUE ELSE FALSE END AS phase_c_swapped
    FROM {{ source('raw', 'fusionsolar_devices') }} raw
    INNER JOIN hidden_valley_meters h ON raw.dev_id = h.dev_id
    LEFT JOIN {{ ref('seed_meter_config_override') }} ov
        ON TRIM(ov.source) = 'FusionSolar' AND ov.esn_code = raw.esn_code
),

pa_pb_pc_raw AS (
    SELECT
        p.timestamp,
        p.dev_id,
        p.plant_code,
        p.plant_name,
        p.metric_id,
        CAST(p.metric_value AS NUMERIC) AS metric_value
    FROM {{ ref('stg_fusionsolar__perf_unpivoted') }} p
    INNER JOIN hidden_valley_meters m
        ON p.dev_id = m.dev_id
    WHERE p.metric_id IN ('active_power_a', 'active_power_b', 'active_power_c')
      AND p.metric_value IS NOT NULL
      AND CAST(p.metric_value AS TEXT) NOT IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity')
      AND p.timestamp >= '{{ phase_correction_start_date }}'::timestamp
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND p.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            AND p.timestamp > (SELECT COALESCE(MAX(timestamp), '1900-01-01'::timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
),

pivoted AS (
    SELECT
        timestamp,
        dev_id,
        plant_code,
        plant_name,
        MAX(CASE WHEN metric_id = 'active_power_a' THEN metric_value END) AS pa_kw,
        MAX(CASE WHEN metric_id = 'active_power_b' THEN metric_value END) AS pb_kw,
        MAX(CASE WHEN metric_id = 'active_power_c' THEN metric_value END) AS pc_kw
    FROM pa_pb_pc_raw
    GROUP BY timestamp, dev_id, plant_code, plant_name
),

corrected AS (
    SELECT
        p.timestamp,
        p.dev_id,
        p.plant_code,
        p.plant_name,
        p.pa_kw,
        p.pb_kw,
        p.pc_kw,
        -- Phase C terbalik: PA + PB - PC hanya sampai maintenance (sebelum phase_correction_end_date).
        -- Setelah maintenance: kembali normal PA + PB + PC.
        CASE
            WHEN cfg.phase_c_swapped
                 AND p.timestamp < '{{ phase_correction_end_date }}'::timestamp
            THEN (COALESCE(p.pa_kw, 0) + COALESCE(p.pb_kw, 0) - COALESCE(p.pc_kw, 0))
            ELSE (COALESCE(p.pa_kw, 0) + COALESCE(p.pb_kw, 0) + COALESCE(p.pc_kw, 0))
        END AS active_power_corrected_kw
    FROM pivoted p
    INNER JOIN meter_phase_config cfg ON p.dev_id = cfg.dev_id
    WHERE p.pa_kw IS NOT NULL OR p.pb_kw IS NOT NULL OR p.pc_kw IS NOT NULL
)

SELECT
    c.timestamp,
    dd.date_key,
    CONCAT('FS_', c.dev_id) AS asset_id,
    COALESCE(p_logical.plant_code, c.plant_code) AS site_id,
    COALESCE(p_logical.plant_name, c.plant_name) AS site_name,
    c.pa_kw,
    c.pb_kw,
    c.pc_kw,
    c.active_power_corrected_kw
FROM corrected c
LEFT JOIN {{ ref('seed_site_merge') }} sm
    ON TRIM(REPLACE(c.plant_code::text, 'NE=', '')) = TRIM(REPLACE(sm.plant_code::text, 'NE=', ''))
LEFT JOIN {{ ref('stg_fusionsolar__sites') }} p_logical
    ON TRIM(REPLACE(p_logical.plant_code::text, 'NE=', '')) = TRIM(REPLACE(sm.logical_plant_code::text, 'NE=', ''))
LEFT JOIN {{ ref('dim_date_generated') }} dd
    ON dd.date_key = DATE(c.timestamp)
