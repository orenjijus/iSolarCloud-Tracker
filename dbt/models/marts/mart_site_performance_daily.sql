{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['date_key', 'site_id'],
    indexes=[
        {'columns': ['date_key', 'site_id'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- Daily site-level performance aggregations
-- Based on Excel calculation flow

-- 1. Energy Calculation (Daily Yield from Revenue Meters)
-- Handle both cumulative meters (continuous across days) and resetting meters
WITH meter_readings_filtered AS (
    SELECT 
        m.date_key,
        m.site_name,
        m.asset_id,
        m.timestamp,
        m.metric_name,
        -- Get polarity_swapped flag from meter config
        CASE 
            WHEN UPPER(TRIM(COALESCE(mc.polarity_swapped::text, ''))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as is_polarity_swapped,
        -- De-duplicate duplicate readings in the same 5-minute bucket.
        -- In duplicate stale-vs-fresh scenarios (e.g. post ID consolidation),
        -- keep the higher counter so first/min values do not get pulled down.
        MAX(CASE 
            WHEN m.metric_value > 0 AND m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
            WHEN m.metric_value > 0 THEN m.metric_value
            ELSE NULL
        END) as metric_value_kwh
    FROM {{ ref('mart_meter_performance_5min') }} m
    JOIN {{ ref('seed_meter_config') }} mc 
        ON m.asset_id = CONCAT(
            CASE 
                WHEN m.system = 'fusionsolar' THEN 'FS'
                WHEN m.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(m.system, 3))
            END, '_', mc.esn_code
        )
    WHERE mc.meter_type = 'Revenue'
        -- Temporal revenue meter: if site has REVENUE_PERIOD rows, only count the meter active on date_key
        AND (
            NOT EXISTS (
                SELECT 1
                FROM {{ ref('seed_meter_site_mapping') }} rp
                WHERE rp.mapping_type = 'REVENUE_PERIOD'
                    AND TRIM(COALESCE(rp.logical_device_id::text, '')) = TRIM(COALESCE(mc.site_id::text, ''))
            )
            OR EXISTS (
                SELECT 1
                FROM {{ ref('seed_meter_site_mapping') }} rp
                WHERE rp.mapping_type = 'REVENUE_PERIOD'
                    AND TRIM(rp.device_id::text) = TRIM(mc.esn_code::text)
                    AND TRIM(COALESCE(rp.logical_device_id::text, '')) = TRIM(COALESCE(mc.site_id::text, ''))
                    AND (
                        rp.effective_date_start IS NULL
                        OR TRIM(rp.effective_date_start::text) = ''
                        OR m.date_key >= rp.effective_date_start::date
                    )
                    AND (
                        rp.effective_date_end IS NULL
                        OR TRIM(rp.effective_date_end::text) = ''
                        OR m.date_key <= rp.effective_date_end::date
                    )
            )
        )
        AND m.metric_name IN ('positive_active_energy', 'negative_active_energy')
        AND m.metric_value > 0  -- Exclude 0 and NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND m.date_key >= '{{ var("reingest_start_date") }}'::date
            AND m.date_key <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            -- Default incremental: only new data
            AND m.date_key > (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
    GROUP BY m.date_key, m.site_name, m.asset_id, m.timestamp, m.metric_name, mc.polarity_swapped
),

meter_daily_stats AS (
    SELECT
        date_key,
        site_name,
        asset_id,
        metric_name,
        is_polarity_swapped,
        MIN(metric_value_kwh) as min_value,
        MAX(metric_value_kwh) as max_value,
        -- Get first and last values of the day from de-duplicated points
        (ARRAY_AGG(metric_value_kwh ORDER BY timestamp))[1] as first_value,
        (ARRAY_AGG(metric_value_kwh ORDER BY timestamp DESC))[1] as last_value
    FROM meter_readings_filtered
    GROUP BY date_key, site_name, asset_id, metric_name, is_polarity_swapped
),

-- Detect cumulative meters by comparing consecutive days
meter_with_prev_day AS (
    SELECT 
        d1.date_key,
        d1.site_name,
        d1.asset_id,
        d1.metric_name,
        d1.is_polarity_swapped,
        d1.min_value,
        d1.max_value,
        d1.first_value,
        d1.last_value,
        d2.max_value as prev_day_max,
        -- Guardrail for stale duplicate streams:
        -- if first_value is far below previous-day counter but today's max is still
        -- close to previous-day level, treat this as stale first point.
        CASE
            WHEN d2.max_value IS NOT NULL
                AND d1.first_value < (d2.max_value * 0.5)
                AND d1.max_value >= (d2.max_value * 0.9)
            THEN d2.max_value
            ELSE d1.first_value
        END as first_value_for_cumulative_check,
        -- Check if meter is cumulative (day N starts where day N-1 ended)
        -- Use percentage-based threshold (0.15%) instead of absolute (0.01) for large values
        -- Changed to <= 0.0015 to handle edge cases where difference is slightly above 0.1% (e.g., 0.1000389%)
        CASE 
            WHEN d2.max_value IS NOT NULL 
                AND (
                    ABS(
                        CASE
                            WHEN d1.first_value < (d2.max_value * 0.5)
                                AND d1.max_value >= (d2.max_value * 0.9)
                            THEN d2.max_value
                            ELSE d1.first_value
                        END
                        - d2.max_value
                    ) < 0.01  -- For small values (< 10kWh)
                    OR (
                        d2.max_value > 0
                        AND ABS(
                            CASE
                                WHEN d1.first_value < (d2.max_value * 0.5)
                                    AND d1.max_value >= (d2.max_value * 0.9)
                                THEN d2.max_value
                                ELSE d1.first_value
                            END
                            - d2.max_value
                        ) / d2.max_value <= 0.0015
                    )  -- For large values: within 0.15% (inclusive)
                )
            THEN true
            ELSE false
        END as is_cumulative
    FROM meter_daily_stats d1
    LEFT JOIN meter_daily_stats d2
        ON d1.asset_id = d2.asset_id
        AND d1.metric_name = d2.metric_name
        AND d1.site_name = d2.site_name
        AND d2.date_key = d1.date_key - INTERVAL '1 day'
),

daily_energy_from_meters AS (
    SELECT 
        date_key,
        site_name,
        asset_id,
        metric_name,
        is_polarity_swapped,
        -- For cumulative meters: use MAX(day N) - MAX(day N-1)
        -- For resetting meters: use MAX - MIN within the day
        SUM(
            CASE 
                WHEN is_cumulative AND prev_day_max IS NOT NULL 
                THEN max_value - prev_day_max
                ELSE max_value - min_value
            END
        ) as daily_energy_kwh
    FROM meter_with_prev_day
    GROUP BY date_key, site_name, asset_id, metric_name, is_polarity_swapped
),

-- Calculate energy per meter, considering polarity swap
meter_energy AS (
    SELECT 
        date_key,
        site_name,
        asset_id,
        is_polarity_swapped,
        SUM(CASE WHEN metric_name = 'positive_active_energy' THEN daily_energy_kwh ELSE 0 END) as positive_energy_kwh,
        SUM(CASE WHEN metric_name = 'negative_active_energy' THEN daily_energy_kwh ELSE 0 END) as negative_energy_kwh
    FROM daily_energy_from_meters
    GROUP BY date_key, site_name, asset_id, is_polarity_swapped
),

-- Apply polarity correction per meter, then sum all meters per site
energy_generation_consumption AS (
    SELECT 
        date_key,
        site_name,
        -- Apply polarity correction: if swapped, use negative - positive, otherwise positive - negative
        -- For solar sites, we want generation (positive value), so take absolute value
        SUM(
            CASE 
                WHEN is_polarity_swapped THEN 
                    ABS(COALESCE(negative_energy_kwh, 0) - COALESCE(positive_energy_kwh, 0))
                ELSE 
                    ABS(COALESCE(positive_energy_kwh, 0) - COALESCE(negative_energy_kwh, 0))
            END
        ) as total_energy_kwh
    FROM meter_energy
    GROUP BY date_key, site_name
),

daily_energy AS (
    SELECT 
        date_key,
        site_name,
        -- Convert to MWh (divide by 1000) to match energy_target_mwh unit
        total_energy_kwh / 1000.0 as daily_energy_mwh
    FROM energy_generation_consumption
),

-- Energy Adjustment from manual seed file (for calculated values when meters are down)
-- This takes priority over actual energy to allow manual overrides
-- Handles comma decimal separator from CSV (European format: "3,05" -> "3.05")
-- Following same pattern as seed_daily_simulation_target and seed_ghi_adjustment_daily
energy_adjustment_daily AS (
    SELECT 
        TRIM("site_name") as site_name,
        "date_key"::date as date_key,
        -- Convert comma to period for European decimal format, then cast to numeric
        -- Following same pattern as seed_daily_simulation_target: CAST(REPLACE(COALESCE(...), ',', '.') AS NUMERIC)
        -- Exact same pattern as ghi_adjustment_daily above
        CAST(REPLACE(COALESCE("energy_adjusted_value"::text, '0'), ',', '.') AS DECIMAL(18,6)) as energy_adjusted_value,
        "notes" as notes
    FROM {{ ref('seed_energy_adjustment_daily') }}
    WHERE "date_key" IS NOT NULL
        AND TRIM(COALESCE("site_name", '')) != ''
        -- Only include rows with actual values (not NULL and not empty string)
        -- Following same pattern as seed_daily_simulation_target: check if value exists
        -- Exact same pattern as ghi_adjustment_daily above
        AND "energy_adjusted_value" IS NOT NULL 
        AND TRIM("energy_adjusted_value"::text) != ''
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND "date_key"::date >= '{{ var("reingest_start_date") }}'::date
            AND "date_key"::date <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            -- Default incremental: only new data
            AND "date_key"::date > (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
),

-- 2. Sensor Calculations (GHI and POA)
-- Now using mart_sensor_daily for cleaner aggregation and easier debugging
-- GHI calculation with fallback logic for sites without GHI sensors
-- GHI_ACTUAL_OVERRIDE (seed): for listed site + Jakarta calendar window, daily_ghi/ghi_actual follow
-- the mapped sensor (e.g. iSolar WST) instead of MAX(sensor_type = 'GHI') / pyranometer.
daily_ghi_raw AS (
    SELECT 
        date_key,
        site_name,
        daily_ghi_kwh_m2
    FROM (
        SELECT 
            date_key,
            site_name,
            -- Get MAX of daily_irradiance per GHI sensor per day
            -- Unit conversions already done in mart_sensor_daily
            MAX(daily_irradiance_kwh_m2) as daily_ghi_kwh_m2
        FROM {{ ref('mart_sensor_daily') }} msd
        WHERE sensor_type = 'GHI'
            AND daily_irradiance_kwh_m2 IS NOT NULL
            -- Exclude target sites only for dates where GHI_FALLBACK / GHI_COPY_FROM_SITE applies (effective_date_*).
            -- Otherwise dated fallbacks (e.g. MMKI III from 2025-06-01) would wipe pre-window GHI.
            AND NOT EXISTS (
                SELECT 1 
                FROM {{ ref('seed_sensor_site_mapping') }} ssm
                WHERE TRIM(UPPER(COALESCE(ssm.device_id, ''))) = TRIM(UPPER(COALESCE(msd.site_name, '')))
                    AND ssm.mapping_type IN ('GHI_FALLBACK', 'GHI_COPY_FROM_SITE')
                    AND (
                        ssm.effective_date_start IS NULL 
                        OR TRIM(ssm.effective_date_start::text) = ''
                        OR msd.date_key >= ssm.effective_date_start::date
                    )
                    AND (
                        ssm.effective_date_end IS NULL 
                        OR TRIM(ssm.effective_date_end::text) = ''
                        OR (
                            ssm.effective_date_end IS NOT NULL 
                            AND ssm.effective_date_end::text != ''
                            AND msd.date_key <= ssm.effective_date_end::text::date
                        )
                    )
            )
            -- Drop PYR/GHI aggregate when seed says actual GHI must come from another asset (e.g. WST).
            AND NOT EXISTS (
                SELECT 1
                FROM {{ ref('seed_sensor_site_mapping') }} ssm_ov
                WHERE ssm_ov.mapping_type = 'GHI_ACTUAL_OVERRIDE'
                    AND TRIM(UPPER(COALESCE(ssm_ov.device_id, ''))) = TRIM(UPPER(COALESCE(msd.site_name, '')))
                    AND (msd.date_key AT TIME ZONE 'Asia/Jakarta')::date >= COALESCE(ssm_ov.effective_date_start, '1900-01-01'::date)
                    AND (
                        ssm_ov.effective_date_end IS NULL
                        OR TRIM(COALESCE(ssm_ov.effective_date_end::text, '')) = ''
                        OR (msd.date_key AT TIME ZONE 'Asia/Jakarta')::date <= ssm_ov.effective_date_end
                    )
            )
        {% if is_incremental() %}
            {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
                -- Override window: re-process same date range
                AND date_key >= '{{ var("reingest_start_date") }}'::date
                AND date_key <= '{{ var("reingest_end_date") }}'::date
            {% else %}
                -- Default incremental: only new data
                AND date_key > (SELECT MAX(date_key) FROM {{ this }})
            {% endif %}
        {% endif %}
        GROUP BY date_key, site_name

        UNION ALL

        SELECT
            msd.date_key,
            TRIM(ssm_ov.device_id) as site_name,
            MAX(
                COALESCE(
                    msd.daily_irradiance_kwh_m2,
                    msd.daily_horizontal_irradiation_kwh_m2
                )
            ) as daily_ghi_kwh_m2
        FROM {{ ref('seed_sensor_site_mapping') }} ssm_ov
        INNER JOIN {{ ref('mart_sensor_daily') }} msd
            ON msd.asset_id = ssm_ov.logical_site_id
            AND (msd.date_key AT TIME ZONE 'Asia/Jakarta')::date >= COALESCE(ssm_ov.effective_date_start, '1900-01-01'::date)
            AND (
                ssm_ov.effective_date_end IS NULL
                OR TRIM(COALESCE(ssm_ov.effective_date_end::text, '')) = ''
                OR (msd.date_key AT TIME ZONE 'Asia/Jakarta')::date <= ssm_ov.effective_date_end
            )
            AND (
                msd.daily_irradiance_kwh_m2 IS NOT NULL
                OR msd.daily_horizontal_irradiation_kwh_m2 IS NOT NULL
            )
        WHERE ssm_ov.mapping_type = 'GHI_ACTUAL_OVERRIDE'
        {% if is_incremental() %}
            {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
                AND msd.date_key >= '{{ var("reingest_start_date") }}'::date
                AND msd.date_key <= '{{ var("reingest_end_date") }}'::date
            {% else %}
                AND msd.date_key > (SELECT MAX(date_key) FROM {{ this }})
            {% endif %}
        {% endif %}
        GROUP BY msd.date_key, TRIM(ssm_ov.device_id)
    ) daily_ghi_union
),
-- Apply GHI fallback logic: sites without GHI use source site GHI; GHI_COPY_FROM_SITE forces copy even if target has local PYR
daily_ghi AS (
    -- Sites with their own GHI sensors (exclude sites that use GHI_FALLBACK or GHI_COPY_FROM_SITE)
    SELECT 
        ghi_raw.date_key,
        ghi_raw.site_name,
        ghi_raw.daily_ghi_kwh_m2
    FROM daily_ghi_raw ghi_raw
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ ref('seed_sensor_site_mapping') }} ssm
        WHERE TRIM(UPPER(COALESCE(ssm.device_id, ''))) = TRIM(UPPER(COALESCE(ghi_raw.site_name, '')))
            AND ssm.mapping_type IN ('GHI_FALLBACK', 'GHI_COPY_FROM_SITE')
            AND (
                ssm.effective_date_start IS NULL 
                OR TRIM(ssm.effective_date_start::text) = ''
                OR ghi_raw.date_key >= ssm.effective_date_start::date
            )
            AND (
                ssm.effective_date_end IS NULL 
                OR TRIM(ssm.effective_date_end::text) = ''
                OR (
                    ssm.effective_date_end IS NOT NULL 
                    AND ssm.effective_date_end::text != ''
                    AND ghi_raw.date_key <= ssm.effective_date_end::text::date
                )
            )
    )
    
    UNION
    
    -- GHI_FALLBACK: target has no own GHI in raw. GHI_COPY_FROM_SITE: always take source (e.g. MMKI I <- Frina)
    SELECT 
        ghi_source.date_key,
        ssm.device_id as site_name,  -- Target site
        ghi_source.daily_ghi_kwh_m2
    FROM daily_ghi_raw ghi_source
    INNER JOIN {{ ref('seed_sensor_site_mapping') }} ssm
        ON TRIM(UPPER(COALESCE(ghi_source.site_name, ''))) = TRIM(UPPER(COALESCE(ssm.logical_site_id, '')))  -- Source site (e.g. MMKI I or Frina)
        AND ssm.mapping_type IN ('GHI_FALLBACK', 'GHI_COPY_FROM_SITE')
        -- Check effective_date_start: only apply fallback if date >= effective_date_start
        -- If effective_date_start is NULL, apply fallback for all dates
        -- If effective_date_start is set, only apply fallback for dates >= effective_date_start
        AND (
            ssm.effective_date_start IS NULL 
            OR ssm.effective_date_start::text = ''
            OR (
                ssm.effective_date_start IS NOT NULL 
                AND ssm.effective_date_start::text != ''
                AND ghi_source.date_key >= ssm.effective_date_start::date
            )
        )
        AND (
            ssm.effective_date_end IS NULL 
            OR ssm.effective_date_end::text = ''
            OR (
                ssm.effective_date_end IS NOT NULL 
                AND ssm.effective_date_end::text != ''
                AND ghi_source.date_key <= ssm.effective_date_end::text::date
            )
        )
    WHERE 
        ssm.mapping_type = 'GHI_COPY_FROM_SITE'
        OR NOT EXISTS (
            -- GHI_FALLBACK only if target has no own GHI row in raw
            SELECT 1 
            FROM daily_ghi_raw ghi_own
            WHERE TRIM(UPPER(COALESCE(ghi_own.site_name, ''))) = TRIM(UPPER(COALESCE(ssm.device_id, '')))
                AND ghi_own.date_key = ghi_source.date_key
        )
),

-- GHI Adjusted: Dynamic logic from seed_sensor_site_mapping
-- Uses mapping_type = 'GHI_ADJUSTED' to determine which sensor to use for which site and date range
-- Only applies to year 2025 and onwards (via effective_date_start/end in seed table)
-- IMPORTANT: If multiple mappings exist for same site/date, prioritize the one with most recent effective_date_start
ghi_adjusted_mapping_raw AS (
    SELECT 
        ssm.device_id as site_name,  -- Target site that needs adjusted GHI
        ssm.logical_site_id as sensor_asset_id,  -- Sensor asset_id to use for adjusted GHI
        ssm.effective_date_start,
        ssm.effective_date_end,
        msd.date_key,
        -- Get GHI value from sensor (handle both GHI and Weather sensor types)
        COALESCE(
            msd.daily_irradiance_kwh_m2,
            msd.daily_horizontal_irradiation_kwh_m2
        ) as ghi_adjusted_value,
        -- Rank mappings by effective_date_start (most recent first) to handle overlapping date ranges
        ROW_NUMBER() OVER (
            PARTITION BY ssm.device_id, msd.date_key 
            ORDER BY ssm.effective_date_start DESC NULLS LAST
        ) as mapping_rank
    FROM {{ ref('seed_sensor_site_mapping') }} ssm
    INNER JOIN {{ ref('mart_sensor_daily') }} msd
        ON msd.asset_id = ssm.logical_site_id  -- Match sensor asset_id
        -- date_key = start-of-WIB-day as UTC; compare using Jakarta calendar date vs seed date columns
        AND (msd.date_key AT TIME ZONE 'Asia/Jakarta')::date >= COALESCE(ssm.effective_date_start, '1900-01-01'::date)
        AND (
            ssm.effective_date_end IS NULL
            OR TRIM(COALESCE(ssm.effective_date_end::text, '')) = ''
            OR (msd.date_key AT TIME ZONE 'Asia/Jakarta')::date <= ssm.effective_date_end
        )
        AND (
            msd.daily_irradiance_kwh_m2 IS NOT NULL 
            OR msd.daily_horizontal_irradiation_kwh_m2 IS NOT NULL
        )
    WHERE ssm.mapping_type = 'GHI_ADJUSTED'
        -- Only apply to year 2025 and onwards (explicitly filter by date_key year)
        AND EXTRACT(YEAR FROM msd.date_key) >= 2025
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND msd.date_key >= '{{ var("reingest_start_date") }}'::date
            AND msd.date_key <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            AND msd.date_key > (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
),
-- Filter to only keep the most recent mapping per site/date (to handle overlapping date ranges)
ghi_adjusted_mapping AS (
    SELECT 
        site_name,
        sensor_asset_id,
        date_key,
        ghi_adjusted_value
    FROM ghi_adjusted_mapping_raw
    WHERE mapping_rank = 1  -- Only take the most recent mapping
),

-- GHI Adjustment from manual seed file (for calculated values from POA ratio, etc.)
-- This takes priority over ghi_adjusted_mapping (sensor-based) to allow manual overrides
-- Handles comma decimal separator from CSV (European format: "3,05" -> "3.05")
-- Following same pattern as seed_daily_simulation_target
ghi_adjustment_daily AS (
    SELECT 
        TRIM("site_name") as site_name,
        "date_key"::date as date_key,
        -- Convert comma to period for European decimal format, then cast to numeric
        -- Following same pattern as seed_daily_simulation_target: CAST(REPLACE(COALESCE(...), ',', '.') AS NUMERIC)
        CAST(REPLACE(COALESCE("ghi_adjusted_value"::text, '0'), ',', '.') AS DECIMAL(18,6)) as ghi_adjusted_value,
        "notes" as notes
    FROM {{ ref('seed_ghi_adjustment_daily') }}
    WHERE "date_key" IS NOT NULL
        AND TRIM(COALESCE("site_name", '')) != ''
        -- Only include rows with actual values (not NULL and not empty string)
        -- Following same pattern as seed_daily_simulation_target: check if value exists
        AND "ghi_adjusted_value" IS NOT NULL 
        AND TRIM("ghi_adjusted_value"::text) != ''
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND "date_key"::date >= '{{ var("reingest_start_date") }}'::date
            AND "date_key"::date <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            -- Default incremental: only new data
            AND "date_key"::date > (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
),

-- POA calculation: now using mart_sensor_daily
-- POA override already applied in mart_sensor_daily, so we can use site_name directly
daily_poa_per_sensor AS (
    SELECT 
        date_key,
        site_name,  -- Already has POA override applied in mart_sensor_daily
        asset_id as sensor_id,
        daily_irradiance_kwh_m2 as max_daily_poa_kwh_m2,
        sensor_capacity_kwp as poa_capacity_kwp
    FROM {{ ref('mart_sensor_daily') }}
    WHERE sensor_type = 'POA'
        AND daily_irradiance_kwh_m2 IS NOT NULL
        AND sensor_capacity_kwp IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND date_key >= '{{ var("reingest_start_date") }}'::date
            AND date_key <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            -- Default incremental: only new data
            AND date_key > (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
),

daily_poa_weighted AS (
    SELECT 
        date_key,
        site_name,
        -- Weighted average: SUM(poa_value * capacity) / SUM(capacity)
        SUM(max_daily_poa_kwh_m2 * poa_capacity_kwp) / NULLIF(SUM(poa_capacity_kwp), 0) as daily_poa_weighted_kwh_m2
    FROM daily_poa_per_sensor
    GROUP BY date_key, site_name
),

-- 3. Availability Calculation with MIT (Minimum Irradiance Threshold)
-- IMPORTANT: Now using fact_site_calculations_5min which already has the correct logic:
-- - MIT calculation with GHI/POA fallback (same as previous logic)
-- - Only counts when BOTH inverter data AND MIT data exist (INNER JOIN)
-- - Unavailability ratio = CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END
-- Daily aggregation from fact_site_calculations_5min
daily_availability AS (
    SELECT 
        fsc.date_key,
        fsc.site_name,
        -- Power Available Hours: Sum of all intervals where inverter is on (power > 0)
        -- This counts ALL intervals where inverter is producing, regardless of MIT
        -- MIT is not a factor for power_available_ratio - it only indicates if inverter is on or off
        SUM(fsc.power_available_ratio) * 5.0 / 60.0 as power_available_hours,
        -- Unavailability Hours: Sum of intervals where MIT = 1 (sunlight available) 
        -- but inverter is not producing (unavailability_ratio > 0)
        -- unavailability_ratio is already calculated as: CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END
        SUM(fsc.unavailability_ratio) * 5.0 / 60.0 as unavailability_hours,
        -- MIT hours: count of intervals where MIT = 1 (irradiance > 40 W/m²), converted to hours
        -- This represents sun hours that exceed the threshold
        SUM(CASE WHEN fsc.mit = 1 THEN 1 ELSE 0 END) * 5.0 / 60.0 as mit_hours,
        -- Total hours (count of 5-min intervals * 5/60)
        COUNT(*) * 5.0 / 60.0 as total_hours,
        -- Availability as decimal (0-1), matching Excel format
        -- e.g., 0.95 = 95% availability, 1.0 = 100% availability
        -- Calculated as: power_available_hours / (power_available_hours + unavailability_hours)
        -- This represents availability during sun hours (MIT = 1)
        CASE 
            WHEN SUM(fsc.power_available_ratio) + SUM(fsc.unavailability_ratio) > 0
            THEN (SUM(fsc.power_available_ratio) / NULLIF(SUM(fsc.power_available_ratio) + SUM(fsc.unavailability_ratio), 0))
            ELSE NULL
        END as availability_percent
    FROM {{ ref('fact_site_calculations_5min') }} fsc
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON fsc.site_name = da.site_name
        AND da.asset_level = 'Site'
    WHERE 1=1
        -- Filter out sites before their calculation_start_date at source
        -- This prevents data from being aggregated for dates before site is operational
        AND (
            da.calculation_start_date IS NULL
            OR fsc.date_key >= da.calculation_start_date
        )
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND fsc.date_key >= '{{ var("reingest_start_date") }}'::date
            AND fsc.date_key <= '{{ var("reingest_end_date") }}'::date
        {% else %}
            -- Default incremental: only new data
            AND fsc.date_key >= (SELECT MAX(date_key) FROM {{ this }})
        {% endif %}
    {% endif %}
    GROUP BY fsc.date_key, fsc.site_name
),

-- Combine all metrics
site_metrics AS (
    SELECT 
        COALESCE(e.date_key, g.date_key, p.date_key, a.date_key) as date_key,
        COALESCE(e.site_name, g.site_name, p.site_name, a.site_name) as site_name,
        e.daily_energy_mwh,
        g.daily_ghi_kwh_m2,
        p.daily_poa_weighted_kwh_m2,
        a.power_available_hours,
        a.unavailability_hours,
        a.mit_hours,
        a.total_hours,
        a.availability_percent
    FROM daily_energy e
    FULL OUTER JOIN daily_ghi g 
        ON e.date_key = g.date_key AND e.site_name = g.site_name
    FULL OUTER JOIN daily_poa_weighted p 
        ON COALESCE(e.date_key, g.date_key) = p.date_key 
        AND COALESCE(e.site_name, g.site_name) = p.site_name
    FULL OUTER JOIN daily_availability a 
        ON COALESCE(e.date_key, g.date_key, p.date_key) = a.date_key 
        AND COALESCE(e.site_name, g.site_name, p.site_name) = a.site_name
),

-- Calculate monthly target from daily targets for KPI calculation
-- This avoids circular dependency with mart_simulation_targets_monthly
-- IMPORTANT: Use consistent join key (COALESCE(site_code, site_id)) to match final SELECT JOIN logic
monthly_targets_for_kpi AS (
    SELECT 
        COALESCE(t.site_code, da.site_id::text) as join_key,
        dd.year,
        dd.month,
        SUM(t.energy_target_mwh) as energy_target_monthly_mwh
    FROM {{ ref('mart_simulation_targets_daily') }} t
    LEFT JOIN {{ ref('dim_date_generated') }} dd
        ON t.date_key = dd.date_key
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON (da.site_id::text = t.site_code::text OR da.site_name = t.site_name)
        AND da.asset_level = 'Site'
    WHERE dd.year IS NOT NULL AND dd.month IS NOT NULL
    GROUP BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
),
-- Calculate monthly simulation from daily simulation for a_KPI calculation
-- This includes unavailibility dates (simulation = include unavailibility dates)
-- IMPORTANT: Use consistent join key (COALESCE(site_code, site_id)) to match final SELECT JOIN logic
monthly_simulation_for_kpi AS (
    SELECT 
        COALESCE(t.site_code, da.site_id::text) as join_key,
        dd.year,
        dd.month,
        SUM(t.energy_simulation_mwh) as energy_simulation_monthly_mwh
    FROM {{ ref('mart_simulation_targets_daily') }} t
    LEFT JOIN {{ ref('dim_date_generated') }} dd
        ON t.date_key = dd.date_key
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON (da.site_id::text = t.site_code::text OR da.site_name = t.site_name)
        AND da.asset_level = 'Site'
    WHERE dd.year IS NOT NULL AND dd.month IS NOT NULL
    GROUP BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
)

-- Final output with PR calculations and target comparisons
-- Use DISTINCT ON to ensure only one row per date_key and site_id combination
-- This prevents duplicates from JOINs with OR conditions
SELECT DISTINCT ON (sm.date_key, da.asset_id)
    sm.date_key,
    dd.year,
    dd.month,
    dd.month_name,
    dd.week_of_month,
    da.asset_id as site_id,
    da.site_name,
    da.system,
    da.actual_capacity_kw,
    da.tariff,
    da.site_order,
    
    -- Issue date flag (for adjusted performance calculations)
    CASE WHEN id.issue_date IS NOT NULL THEN TRUE ELSE FALSE END as is_issue_date,
    CAST(NULL AS TEXT) as issue_date_remarks,
    
    -- Energy (in MWh) - converted from kWh to match energy_target_mwh unit
    -- Cast to DECIMAL for consistent format matching Excel
    CAST(COALESCE(sm.daily_energy_mwh, 0) AS DECIMAL(18,6)) as daily_energy_mwh,
    CAST(COALESCE(sm.daily_energy_mwh, 0) AS DECIMAL(18,6)) as energy_actual,
    
    -- Energy Adjusted: Dynamic energy that can be changed for analysis
    -- Priority order:
    -- 1. seed_energy_adjustment_daily (manual calculated values, e.g., from ratio calculation)
    -- 2. energy_actual (original energy from meters, same as daily_energy_mwh)
    CAST(
        COALESCE(
            energy_adj.energy_adjusted_value,  -- Priority 1: Manual adjustment from seed file
            sm.daily_energy_mwh  -- Priority 2: Default: Use actual energy
        )
        AS DECIMAL(18,6)
    ) as energy_adjusted,
    
    -- Irradiance (in kWh/m²)
    -- Cast to DECIMAL for consistent format matching Excel
    -- daily_ghi_kwh_m2 / ghi_actual: MAX(GHI) + GHI_FALLBACK + GHI_COPY_FROM_SITE + GHI_ACTUAL_OVERRIDE (seed);
    -- actual mengikuti sensor yang di-map (mis. WST iSolar), bukan pyranometer.
    -- Untuk GHI operasional / PR saat substitusi WST pakai kolom ghi_adjusted dan pr_ghi_actual (denominator mengikuti ghi_adjusted)
    CAST(COALESCE(sm.daily_ghi_kwh_m2, ghi_adj.ghi_adjusted_value) AS DECIMAL(18,6)) as daily_ghi_kwh_m2,
    CAST(COALESCE(sm.daily_ghi_kwh_m2, ghi_adj.ghi_adjusted_value) AS DECIMAL(18,6)) as ghi_actual,
    CAST(sm.daily_poa_weighted_kwh_m2 AS DECIMAL(18,6)) as daily_poa_weighted_kwh_m2,
    
    -- GHI Adjusted: Dynamic GHI that can be changed for analysis
    -- Priority order:
    -- 1. seed_ghi_adjustment_daily (manual calculated values, e.g., from POA ratio)
    -- 2. ghi_adjusted_mapping (from seed_sensor_site_mapping, uses other sensor)
    -- 3. sm.daily_ghi_kwh_m2 (agregat GHI mentah, sama basis dengan kolom ghi_actual)
    -- Only applies to year 2025 and onwards (configured via effective_date_start/end in seed table)
    CAST(
        COALESCE(
            ghi_manual.ghi_adjusted_value,  -- Priority 1: Manual adjustment from seed file (e.g., calculated from POA ratio)
            ghi_adj.ghi_adjusted_value,  -- Priority 2: Adjusted GHI from sensor mapping if available
            sm.daily_ghi_kwh_m2  -- Priority 3: Default: Use actual GHI
        )
        AS DECIMAL(18,6)
    ) as ghi_adjusted,
    
    -- Availability
    -- Cast to DECIMAL for consistent format matching Excel
    CAST(sm.power_available_hours AS DECIMAL(18,6)) as power_available_hours,
    CAST(sm.unavailability_hours AS DECIMAL(18,6)) as unavailability_hours,
    -- MIT hours: sun hours where irradiance exceeds threshold (> 40 W/m²)
    CAST(sm.mit_hours AS DECIMAL(18,6)) as mit_hours,
    -- Total hours: total operational hours in the day
    CAST(sm.total_hours AS DECIMAL(18,6)) as total_hours,
    CAST(sm.availability_percent AS DECIMAL(18,6)) as availability_percent,
    
    -- Performance Ratios (as decimal, matching Excel format)
    -- PR GHI = (daily_energy_mwh * 1000) / (effective_daily_ghi_kwh_m2) / site_capacity_kw
    -- effective_daily_ghi_kwh_m2 matches ghi_adjusted precedence (manual seed > sensor mapping > sensor actual)
    -- so PR stays usable during telemetry gaps when seed_ghi_adjustment_daily is populated.
    -- Convert MWh to kWh for PR calculation
    -- Result is decimal ratio (not percentage), matching Excel format where PR is stored as decimal
    -- Example: PR = 0.7345 means 73.45% efficiency
    -- Add minimum threshold (0.1 kWh/m²) to prevent division by very small numbers causing overflow
    -- Cast to DECIMAL(18,6) for better precision matching Excel
    CASE 
        WHEN COALESCE(
            ghi_manual.ghi_adjusted_value,
            ghi_adj.ghi_adjusted_value,
            sm.daily_ghi_kwh_m2
        ) IS NOT NULL 
            AND COALESCE(
                ghi_manual.ghi_adjusted_value,
                ghi_adj.ghi_adjusted_value,
                sm.daily_ghi_kwh_m2
            ) >= 0.1  -- Minimum threshold to prevent overflow
            AND sm.daily_energy_mwh IS NOT NULL
            AND sm.daily_energy_mwh >= 0.01  -- Minimum energy threshold
            AND da.actual_capacity_kw IS NOT NULL 
            AND da.actual_capacity_kw > 0
        THEN LEAST(
            CAST(((sm.daily_energy_mwh * 1000.0) / NULLIF(COALESCE(
                ghi_manual.ghi_adjusted_value,
                ghi_adj.ghi_adjusted_value,
                sm.daily_ghi_kwh_m2
            ), 0) / NULLIF(da.actual_capacity_kw, 0)) AS DECIMAL(18,6)),
            10.0  -- Cap at 10.0 (1000%) to prevent overflow from data errors while still capturing anomalies
        )
        ELSE NULL
    END as pr_ghi_actual,
    
    -- PR Adjusted = (energy_adjusted * 1000) / (ghi_adjusted) / site_capacity_kw
    -- Uses both energy_adjusted and ghi_adjusted (with priority: manual > sensor mapping > actual) for adjusted performance ratio calculation
    -- Convert MWh to kWh for PR calculation
    -- Result is decimal ratio (not percentage), matching Excel format where PR is stored as decimal
    -- Example: PR = 0.7345 means 73.45% efficiency
    -- Add minimum threshold (0.1 kWh/m²) to prevent division by very small numbers causing overflow
    -- Cast to DECIMAL(18,6) for better precision matching Excel
    CASE 
        WHEN COALESCE(
            ghi_manual.ghi_adjusted_value,  -- Priority 1: Manual adjustment
            ghi_adj.ghi_adjusted_value,  -- Priority 2: Sensor mapping
            sm.daily_ghi_kwh_m2  -- Priority 3: Actual GHI
        ) IS NOT NULL 
            AND COALESCE(
                ghi_manual.ghi_adjusted_value,
                ghi_adj.ghi_adjusted_value,
                sm.daily_ghi_kwh_m2
            ) >= 0.1  -- Minimum threshold to prevent overflow
            AND COALESCE(
                energy_adj.energy_adjusted_value,  -- Priority 1: Manual energy adjustment
                sm.daily_energy_mwh  -- Priority 2: Actual energy
            ) IS NOT NULL
            AND COALESCE(
                energy_adj.energy_adjusted_value,
                sm.daily_energy_mwh
            ) >= 0.01  -- Minimum energy threshold
            AND da.actual_capacity_kw IS NOT NULL 
            AND da.actual_capacity_kw > 0
        THEN LEAST(
            CAST(((COALESCE(
                energy_adj.energy_adjusted_value,
                sm.daily_energy_mwh
            ) * 1000.0) / NULLIF(COALESCE(
                ghi_manual.ghi_adjusted_value,
                ghi_adj.ghi_adjusted_value,
                sm.daily_ghi_kwh_m2
            ), 0) / NULLIF(da.actual_capacity_kw, 0)) AS DECIMAL(18,6)),
            10.0  -- Cap at 10.0 (1000%) to prevent overflow from data errors while still capturing anomalies
        )
        ELSE NULL
    END as pr_adjusted,
    
    -- PR POA = (daily_energy_mwh * 1000) / (daily_poa_weighted_kwh_m2) / site_capacity_kw
    -- Convert MWh to kWh for PR calculation
    -- Result is decimal (0-1), e.g., 0.7345 = 73.45%
    -- Add minimum threshold (0.1 kWh/m²) to prevent division by very small numbers causing overflow
    -- Cast to DECIMAL(18,6) for better precision matching Excel
    CASE 
        WHEN sm.daily_poa_weighted_kwh_m2 IS NOT NULL 
            AND sm.daily_poa_weighted_kwh_m2 >= 0.1  -- Minimum threshold to prevent overflow
            AND sm.daily_energy_mwh IS NOT NULL
            AND sm.daily_energy_mwh >= 0.01  -- Minimum energy threshold
            AND da.actual_capacity_kw IS NOT NULL 
            AND da.actual_capacity_kw > 0
        THEN LEAST(
            CAST(((sm.daily_energy_mwh * 1000.0) / NULLIF(sm.daily_poa_weighted_kwh_m2, 0) / NULLIF(da.actual_capacity_kw, 0)) AS DECIMAL(18,6)),
            2.0  -- Cap at 2.0 (200%) to prevent overflow from data errors
        )
        ELSE NULL
    END as pr_poa_actual,
    
    -- Target Comparisons
    -- Cast to DECIMAL for consistent format matching Excel
    -- Note: Target PR values should already be in decimal (0-1) format from source
    CAST(t.energy_target_mwh AS DECIMAL(18,6)) as energy_target_mwh,
    CAST(t.daily_pr_ghi_target AS DECIMAL(18,6)) as daily_pr_ghi_target,
    CAST(t.daily_pr_poa_target AS DECIMAL(18,6)) as daily_pr_poa_target,
    CAST(t.ghi AS DECIMAL(18,6)) as ghi_target,
    CAST(t.poa AS DECIMAL(18,6)) as poa_target,
    
    -- Energy actual vs target (as decimal ratio, matching Excel format)
    -- Both daily_energy_mwh and energy_target_mwh are in MWh, so no conversion needed
    -- Result is decimal ratio, e.g., 0.95 = 95% of target, 1.05 = 105% of target
    CASE 
        WHEN t.energy_target_mwh IS NOT NULL 
            AND t.energy_target_mwh > 0 
            AND sm.daily_energy_mwh IS NOT NULL
        THEN CAST((sm.daily_energy_mwh / NULLIF(t.energy_target_mwh, 0)) AS DECIMAL(18,6))
        ELSE NULL
    END as energy_actual_vs_target_pct,
    
    -- GHI actual vs target (as decimal ratio, matching Excel format)
    -- Result is decimal ratio, e.g., 0.95 = 95% of target, 1.05 = 105% of target
    CASE 
        WHEN t.ghi IS NOT NULL 
            AND t.ghi > 0 
            AND sm.daily_ghi_kwh_m2 IS NOT NULL
        THEN CAST((sm.daily_ghi_kwh_m2 / NULLIF(t.ghi, 0)) AS DECIMAL(18,6))
        ELSE NULL
    END as ghi_actual_vs_target_pct,
    
    -- POA actual vs target (as decimal ratio, matching Excel format)
    -- Result is decimal ratio, e.g., 0.95 = 95% of target, 1.05 = 105% of target
    CASE 
        WHEN t.poa IS NOT NULL 
            AND t.poa > 0 
            AND sm.daily_poa_weighted_kwh_m2 IS NOT NULL
        THEN CAST((sm.daily_poa_weighted_kwh_m2 / NULLIF(t.poa, 0)) AS DECIMAL(18,6))
        ELSE NULL
    END as poa_actual_vs_target_pct,
    
    -- Energy vs GHI variance (as decimal difference, matching Excel format)
    -- Shows how energy performance differs from GHI performance
    -- Positive = energy performing better than GHI, Negative = energy performing worse than GHI
    -- Result is decimal difference, e.g., 0.05 = 5% better, -0.05 = 5% worse
    CASE 
        WHEN t.energy_target_mwh IS NOT NULL 
            AND t.energy_target_mwh > 0 
            AND t.ghi IS NOT NULL
            AND t.ghi > 0
            AND sm.daily_energy_mwh IS NOT NULL
            AND sm.daily_ghi_kwh_m2 IS NOT NULL
        THEN CAST(
            ((sm.daily_energy_mwh / NULLIF(t.energy_target_mwh, 0)) - (sm.daily_ghi_kwh_m2 / NULLIF(t.ghi, 0))) AS DECIMAL(18,6)
        )
        ELSE NULL
    END as energy_vs_ghi_variance_pct,
    
    -- Daily KPI calculation: Monthly KPI * (Daily Target / Monthly Target)
    -- Formula: KPI Daily = KPI Monthly × (Target Daily / Target Monthly)
    -- Same formula as in mart_simulation_targets_daily
    CASE 
        WHEN mt_kpi.energy_target_monthly_mwh > 0 
            AND mkpi.energy_kpi_mwh IS NOT NULL
            AND t.energy_target_mwh IS NOT NULL
        THEN CAST(
            mkpi.energy_kpi_mwh * (t.energy_target_mwh / mt_kpi.energy_target_monthly_mwh) AS DECIMAL(18,6)
        )
        ELSE NULL
    END as energy_kpi_daily_mwh,
    
    -- Daily Adjusted KPI calculation: Target Daily * (KPI Monthly / Simulation Monthly)
    -- Formula: a_KPI Daily = Target Daily × (KPI Monthly / Simulation Monthly)
    -- This represents KPI adjusted to exclude unavailibility dates
    -- simulation = include unavailibility dates, target = exclude unavailibility dates
    -- a_kpi = target adjusted to kpi atau bisa dibilang kpi exclude unavailibility dates
    CASE 
        WHEN ms_kpi.energy_simulation_monthly_mwh > 0 
            AND mkpi.energy_kpi_mwh IS NOT NULL
            AND t.energy_target_mwh IS NOT NULL
        THEN CAST(
            t.energy_target_mwh * (mkpi.energy_kpi_mwh / ms_kpi.energy_simulation_monthly_mwh) AS DECIMAL(18,6)
        )
        ELSE NULL
    END as energy_a_kpi_daily_mwh,
    
    -- Reference columns for KPI calculation
    CAST(mkpi.energy_kpi_mwh AS DECIMAL(18,6)) as energy_kpi_monthly_mwh,
    CAST(mt_kpi.energy_target_monthly_mwh AS DECIMAL(18,6)) as energy_target_monthly_mwh,
    CAST(ms_kpi.energy_simulation_monthly_mwh AS DECIMAL(18,6)) as energy_simulation_monthly_mwh

FROM site_metrics sm
LEFT JOIN {{ ref('dim_assets') }} da 
    ON da.site_name = sm.site_name 
    AND da.asset_level = 'Site'
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON sm.date_key = dd.date_key
LEFT JOIN {{ ref('mart_simulation_targets_daily') }} t 
    ON sm.date_key = t.date_key 
    AND (
        da.asset_id = t.asset_id 
        OR da.site_name = t.site_name
        OR (da.site_id::text = t.site_code::text AND t.site_code IS NOT NULL)
    )
LEFT JOIN {{ ref('mart_site_kpi_monthly') }} mkpi
    ON (mkpi.site_id::text = da.site_id::text OR mkpi.site_name = da.site_name)
    AND mkpi.year = dd.year
    AND mkpi.month = dd.month
LEFT JOIN monthly_targets_for_kpi mt_kpi
    ON mt_kpi.join_key = COALESCE(t.site_code, da.site_id::text)
    AND mt_kpi.year = dd.year
    AND mt_kpi.month = dd.month
LEFT JOIN monthly_simulation_for_kpi ms_kpi
    ON ms_kpi.join_key = COALESCE(t.site_code, da.site_id::text)
    AND ms_kpi.year = dd.year
    AND ms_kpi.month = dd.month
-- Join with Energy manual adjustment (from seed_energy_adjustment_daily - highest priority)
LEFT JOIN energy_adjustment_daily energy_adj
    ON sm.date_key = energy_adj.date_key
    AND TRIM(UPPER(COALESCE(sm.site_name, ''))) = TRIM(UPPER(COALESCE(energy_adj.site_name, '')))
-- Join with GHI manual adjustment (from seed_ghi_adjustment_daily - highest priority)
LEFT JOIN ghi_adjustment_daily ghi_manual
    ON sm.date_key = ghi_manual.date_key
    AND TRIM(UPPER(COALESCE(sm.site_name, ''))) = TRIM(UPPER(COALESCE(ghi_manual.site_name, '')))
-- Join with GHI adjusted mapping (dynamic from seed_sensor_site_mapping - second priority)
LEFT JOIN ghi_adjusted_mapping ghi_adj
    ON sm.date_key = ghi_adj.date_key
    AND sm.site_name = ghi_adj.site_name
-- Join with issue dates to flag dates that should be excluded from adjusted performance calculations
LEFT JOIN (
    SELECT 
        site_id,
        -- Handle YYYY-MM-DD format (ISO format, e.g., 2025-09-03)
        -- Also support M/D/YYYY format as fallback for backward compatibility
        CASE 
            WHEN issue_date::text ~ '^\d{4}-\d{2}-\d{2}' THEN 
                TO_DATE(issue_date::text, 'YYYY-MM-DD')
            WHEN issue_date::text ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN 
                TO_DATE(issue_date::text, 'FMMM/FMDD/YYYY')
            ELSE 
                issue_date::date
        END as issue_date,
        site_name as issue_site_name,
        -- is_active is boolean in seed (TRUE/FALSE)
        -- Cast to boolean explicitly to handle both boolean and text types from CSV
        CASE 
            WHEN is_active::boolean IS TRUE THEN TRUE
            WHEN UPPER(TRIM(is_active::text)) = 'TRUE' THEN TRUE
            WHEN is_active::text = '1' THEN TRUE
            ELSE FALSE
        END as is_active
    FROM {{ ref('seed_issue_dates') }}
    WHERE issue_date IS NOT NULL
        AND TRIM(issue_date::text) != ''
) id
    ON sm.date_key = id.issue_date
    AND id.is_active = TRUE
    AND (
        da.asset_id = id.site_id
        OR TRIM(UPPER(COALESCE(da.site_name, ''))) = TRIM(UPPER(COALESCE(id.issue_site_name, '')))
    )
WHERE sm.date_key IS NOT NULL
    -- Filter out sites before their calculation_start_date
    -- Sites with NULL calculation_start_date will be included for all dates
    AND (
        da.calculation_start_date IS NULL
        OR sm.date_key >= da.calculation_start_date
    )
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: ensure we only process the re-ingest range
        AND sm.date_key >= '{{ var("reingest_start_date") }}'::date
        AND sm.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Default incremental: only new data
        AND sm.date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
{% endif %}
-- ORDER BY required for DISTINCT ON - ensures deterministic row selection when duplicates exist
ORDER BY sm.date_key, da.asset_id, t.date_key NULLS LAST, mkpi.year NULLS LAST, mkpi.month NULLS LAST
