{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    tags=['intraday'],
    schema='mart',
    unique_key=['date_key', 'site_id'],
    indexes=[
        {'columns': ['date_key', 'site_id'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- Hidden Valley: agregasi harian beban villa (energi) dari daya 5-menit.
-- Rumus sama seperti string DC: E_kWh_per_slot = P_kW * (5/60) jam.
-- Sumber: mart_hidden_valley_villa_load_5min (inv + SA sudah dijumlah per timestamp).

WITH villa_5min AS (
    SELECT
        v.date_key,
        v.site_id,
        v.site_name,
        v.system,
        v.inv_active_power_kw,
        v.sa_active_power_kw,
        v.beban_villa_kw,
        -- kW × (5 menit / 60 menit) = kWh per interval
        COALESCE(v.inv_active_power_kw, 0) * (5.0 / 60.0) AS inv_energy_kwh_5min,
        COALESCE(v.sa_active_power_kw, 0) * (5.0 / 60.0) AS sa_energy_kwh_5min,
        v.beban_villa_kw * (5.0 / 60.0) AS villa_energy_kwh_5min
    FROM {{ ref('mart_hidden_valley_villa_load_5min') }} v
    WHERE v.timestamp IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND v.date_key >= '{{ var("reingest_start_date") }}'::date
            AND v.date_key <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            AND v.date_key > (SELECT COALESCE(MAX(date_key), '1900-01-01'::date) FROM {{ this }})
        {% endif %}
    {% endif %}
),

daily AS (
    SELECT
        date_key,
        site_id,
        MAX(site_name) AS site_name,
        MAX(system) AS system,
        COUNT(*) AS interval_count_5min,
        SUM(inv_energy_kwh_5min) AS daily_inv_energy_kwh,
        SUM(sa_energy_kwh_5min) AS daily_sa_energy_kwh,
        SUM(villa_energy_kwh_5min) AS daily_villa_load_kwh
    FROM villa_5min
    GROUP BY date_key, site_id
)

SELECT
    d.date_key,
    d.site_id,
    d.site_name,
    d.system,
    CAST(d.interval_count_5min AS INTEGER) AS interval_count_5min,
    CAST(d.daily_inv_energy_kwh AS DECIMAL(18, 6)) AS daily_inv_energy_kwh,
    CAST(d.daily_sa_energy_kwh AS DECIMAL(18, 6)) AS daily_sa_energy_kwh,
    CAST(d.daily_villa_load_kwh AS DECIMAL(18, 6)) AS daily_villa_load_kwh
FROM daily d