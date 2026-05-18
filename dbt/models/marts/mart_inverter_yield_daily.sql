{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['date_key', 'asset_id'],
    indexes=[
        {'columns': ['date_key', 'asset_id'], 'type': 'btree'},
        {'columns': ['date_key', 'site_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'}
    ],
    meta={'hyperscale': true},
    pre_hook="
        {%- if is_incremental() and var('reingest_start_date', none) and var('reingest_end_date', none) -%}
            DELETE FROM {{ this }} 
            WHERE date_key >= '{{ var(\"reingest_start_date\") }}'::date 
              AND date_key <= '{{ var(\"reingest_end_date\") }}'::date
            {%- if var('reingest_site_ids', none) -%}
              AND (site_id = ANY(string_to_array('{{ var(\"reingest_site_ids\") }}', ',')::VARCHAR[])
                   OR site_name = ANY(string_to_array('{{ var(\"reingest_site_ids\") }}', ',')::VARCHAR[]))
            {%- endif -%}
            {%- if var('reingest_asset_ids', none) -%}
              AND asset_id = ANY(string_to_array('{{ var(\"reingest_asset_ids\") }}', ',')::VARCHAR[])
            {%- endif -%}
        {%- endif -%}
    "
) }}

-- Daily inverter yield aggregation
-- Purpose: Calculate daily yield per inverter for ranking in Power BI
-- Source: mart_inverter_performance_5min with metric_name = 'inv_yield'
-- Grain: date_key × asset_id (inverter)

WITH inverter_yield_5min AS (
    SELECT 
        i.timestamp,
        i.date_key,
        i.asset_id,
        i.asset_name as inverter_name,
        i.site_name,
        da.site_id,
        i.system,
        i.metric_value,
        i.metric_unit,
        -- Convert Wh to kWh for iSolarCloud (metric_unit = 'Wh')
        -- FusionSolar is already in kWh (metric_unit = 'kWh')
        CASE 
            WHEN i.metric_unit = 'Wh' THEN i.metric_value / 1000.0
            ELSE i.metric_value
        END as yield_kwh
    FROM {{ ref('mart_inverter_performance_5min') }} i
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON i.site_name = da.site_name 
        AND da.asset_level = 'Site'
    WHERE i.metric_name = 'inv_yield'
        AND i.metric_value IS NOT NULL
        AND i.metric_value > 0  -- Exclude 0 and NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND i.date_key >= '{{ var("reingest_start_date") }}'::date
            AND i.date_key <= '{{ var("reingest_end_date") }}'::date
            {% if var('reingest_site_ids', none) %}
                -- Filter by specific site IDs (handle NULL from LEFT JOIN)
                AND (da.site_id = ANY(string_to_array('{{ var("reingest_site_ids") }}', ',')::VARCHAR[])
                     OR i.site_name = ANY(string_to_array('{{ var("reingest_site_ids") }}', ',')::VARCHAR[]))
            {% endif %}
            {% if var('reingest_asset_ids', none) %}
                -- Filter by specific asset IDs (inverters)
                AND i.asset_id = ANY(string_to_array('{{ var("reingest_asset_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            -- Default incremental: only new data
            AND i.date_key > (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
),

-- Calculate daily yield per inverter
-- Since "yield of current day" resets at midnight, use MAX value of the day
-- The MAX value at end of day represents the total daily yield
inverter_yield_daily AS (
    SELECT 
        date_key,
        asset_id,
        inverter_name,
        site_id,
        site_name,
        system,
        -- Get first and last values of the day for validation
        (ARRAY_AGG(yield_kwh ORDER BY timestamp))[1] as first_yield_kwh,
        (ARRAY_AGG(yield_kwh ORDER BY timestamp DESC))[1] as last_yield_kwh,
        MIN(yield_kwh) as min_yield_kwh,
        MAX(yield_kwh) as max_yield_kwh,
        COUNT(*) as measurement_count
    FROM inverter_yield_5min
    GROUP BY date_key, asset_id, inverter_name, site_id, site_name, system
)

SELECT 
    date_key,
    asset_id,
    inverter_name,
    site_id,
    site_name,
    system,
    -- Daily yield: Since "yield of current day" resets at midnight,
    -- the MAX value at end of day is the daily yield
    -- Prefer last_yield_kwh (most recent value), fallback to max_yield_kwh
    COALESCE(last_yield_kwh, max_yield_kwh) as daily_yield_kwh,
    first_yield_kwh,
    last_yield_kwh,
    min_yield_kwh,
    max_yield_kwh,
    measurement_count
FROM inverter_yield_daily
WHERE 1=1
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: ensure we only process the re-ingest range
        AND date_key >= '{{ var("reingest_start_date") }}'::date
        AND date_key <= '{{ var("reingest_end_date") }}'::date
        {% if var('reingest_site_ids', none) %}
            -- Filter by specific site IDs
            AND site_id = ANY(string_to_array('{{ var("reingest_site_ids") }}', ',')::VARCHAR[])
        {% endif %}
        {% if var('reingest_asset_ids', none) %}
            -- Filter by specific asset IDs (inverters)
            AND asset_id = ANY(string_to_array('{{ var("reingest_asset_ids") }}', ',')::VARCHAR[])
        {% endif %}
    {% endif %}
{% endif %}
ORDER BY date_key DESC, site_id, daily_yield_kwh DESC

