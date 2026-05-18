{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['timestamp', 'site_id'],
    indexes=[
        {'columns': ['timestamp', 'site_id'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute site-level calculated metrics
-- Purpose: Aggregate inverter metrics to site level and calculate availability ratios
-- Grain: timestamp_5min × site_id
-- Depends on: fact_inverter_calculations_5min

WITH 
-- Step 1: Aggregate inverter metrics to site level
site_aggregated AS (
    SELECT 
        f.timestamp,
        f.date_key,
        f.site_id,
        f.site_name,
        f.system,
        -- Get fixed total_inverters from dim_assets (from seed_site_config)
        -- This is the expected total number of inverters, not the dynamic count
        MAX(da.total_inverters) as total_inverters_fixed,
        -- For MMKI group: use dynamic total_inverters (COUNT of reporting inverters)
        -- For other sites: use fixed total_inverters from config
        CASE 
            WHEN f.site_name LIKE '%MMKI%' THEN COUNT(DISTINCT f.inverter_id)  -- MMKI: dynamic (only reporting)
            ELSE MAX(da.total_inverters)  -- Other sites: fixed from config
        END as total_inverters,
        -- Count how many inverters are actually reporting with power > 0
        COUNT(DISTINCT CASE WHEN f.inverter_availability = 1 THEN f.inverter_id END) as available_inverters,
        -- MIT is site-level (same for all inverters at same site), so take any value
        MAX(f.mit) as mit
    FROM {{ ref('fact_inverter_calculations_5min') }} f
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON f.site_name = da.site_name
        AND da.asset_level = 'Site'
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            WHERE f.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
                AND f.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            WHERE f.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
    GROUP BY f.timestamp, f.date_key, f.site_id, f.site_name, f.system
),

-- Step 2: Calculate site-level metrics
site_calculations AS (
    SELECT 
        timestamp,
        date_key,
        site_id,
        site_name,
        system,
        -- Use dynamic total_inverters for MMKI group, fixed for other sites
        total_inverters,
        available_inverters,
        mit,
        -- power_available_ratio = available_inverters / total_inverters (0-1)
        -- For MMKI: only counting reporting inverters (missing inverters not counted as unavailable)
        -- For other sites: missing inverters (not reporting) are considered unavailable
        CASE 
            WHEN total_inverters > 0
            THEN available_inverters::DECIMAL / total_inverters::DECIMAL
            ELSE 0
        END as power_available_ratio,
        -- unavailability_ratio = CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END (0-1)
        -- For MMKI: unavailability only for inverters that are reporting but not producing power
        -- For other sites: unavailability includes missing inverters
        CASE 
            WHEN mit = 1 AND total_inverters > 0
            THEN 1 - (available_inverters::DECIMAL / total_inverters::DECIMAL)
            ELSE 0
        END as unavailability_ratio,
        NOW() as calculation_timestamp
    FROM site_aggregated
)

SELECT 
    timestamp,
    date_key,
    site_id,
    site_name,
    system,
    total_inverters,
    available_inverters,
    mit,
    power_available_ratio,
    unavailability_ratio,
    calculation_timestamp
FROM site_calculations
ORDER BY timestamp, site_id

