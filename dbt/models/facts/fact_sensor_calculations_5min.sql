{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['timestamp', 'sensor_id'],
    indexes=[
        {'columns': ['timestamp', 'sensor_id'], 'type': 'btree'},
        {'columns': ['timestamp', 'site_id'], 'type': 'btree'},
        {'columns': ['sensor_id'], 'type': 'btree'},
        {'columns': ['date_key'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute sensor-level calculated metrics with GHI and POA fallback logic for MIT
-- Purpose: Calculate MIT (Minimum Irradiance Threshold) with fallback logic:
--   Priority 1: GHI (pyranometer) from same site
--   Priority 2: POA from same site (fallback if GHI not available)
--   Priority 3: GHI from other site (configured fallback)
-- Grain: timestamp_5min × sensor_id

WITH 
-- Step 0A: Get raw GHI sensor data with window functions to detect stuck sensors
ghi_raw AS (
    SELECT 
        s.timestamp,
        s.date_key,
        s.asset_id as sensor_id,
        s.asset_name as sensor_name,
        s.site_name,
        da.site_id,
        s.system,
        s.metric_value as raw_irradiance,
        sc.sensor_type,
        -- Window functions to detect stuck sensors
        LAG(s.metric_value, 1) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_1,
        LAG(s.metric_value, 2) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_2,
        LAG(s.metric_value, 3) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_3,
        LAG(s.metric_value, 4) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_4,
        LAG(s.metric_value, 5) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_5,
        LEAD(s.metric_value, 1) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as next_1
    FROM {{ ref('mart_sensor_measurements_5min') }} s
    JOIN {{ ref('seed_sensor_config') }} sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON s.site_name = da.site_name 
        AND da.asset_level = 'Site'
    WHERE sc.sensor_type = 'GHI'
        AND s.metric_name = 'irradiance'
        AND s.metric_value IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND s.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND s.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            AND s.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
),

-- Step 1: Get GHI sensors with validation (filter out stuck/malfunctioning sensors)
ghi_sensors AS (
    SELECT 
        timestamp,
        date_key,
        sensor_id,
        sensor_name,
        site_name,
        site_id,
        system,
        -- Validate irradiance: filter out stuck/malfunctioning sensors
        -- A sensor is STUCK if it reports the same value 6+ times in a row (30 min+)
        -- A sensor is INVALID if it reports unrealistic values
        CASE 
            -- Stuck sensor detection: same value for 6+ consecutive intervals
            WHEN raw_irradiance = prev_1 
                AND raw_irradiance = prev_2 
                AND raw_irradiance = prev_3
                AND raw_irradiance = prev_4
                AND raw_irradiance = prev_5
                AND raw_irradiance > 100  -- Only flag high values as stuck
            THEN NULL  -- Mark as stuck sensor
            -- Basic validation
            WHEN raw_irradiance > 2000  -- Unrealistic high value (max ~1400 W/m²)
            THEN NULL
            WHEN EXTRACT(HOUR FROM timestamp) < 6  -- Deep nighttime (00:00-05:59)
                AND raw_irradiance > 50  -- Any significant reading at deep night is suspicious
            THEN NULL
            ELSE raw_irradiance
        END as irradiance_w_m2,
        sensor_type,
        FALSE as is_fallback  -- Original GHI sensors are not fallback
    FROM ghi_raw
),

-- Step 2: Apply GHI fallback logic - sites without GHI sensors get GHI from source site
ghi_with_fallback AS (
    -- Original GHI sensors (sites with their own GHI)
    SELECT 
        timestamp,
        date_key,
        sensor_id,
        sensor_name,
        site_name,
        site_id,
        system,
        irradiance_w_m2,
        sensor_type,
        is_fallback
    FROM ghi_sensors
    
    UNION ALL
    
    -- Fallback GHI: sites without GHI sensors use GHI from source site
    SELECT 
        ghi_source.timestamp,
        ghi_source.date_key,
        -- Create a synthetic sensor_id for fallback (use site_id if available, else site_name)
        COALESCE(
            CONCAT(
                CASE 
                    WHEN ghi_source.system = 'fusionsolar' THEN 'FS'
                    WHEN ghi_source.system = 'isolarcloud' THEN 'ISO'
                    ELSE UPPER(LEFT(ghi_source.system, 3))
                END, '_SITE_', 
                da_fallback.site_id, '_GHI_FALLBACK'
            ),
            CONCAT(
                CASE 
                    WHEN ghi_source.system = 'fusionsolar' THEN 'FS'
                    WHEN ghi_source.system = 'isolarcloud' THEN 'ISO'
                    ELSE UPPER(LEFT(ghi_source.system, 3))
                END, '_', 
                REPLACE(ssm.device_id, ' ', '_'), '_GHI_FALLBACK'
            )
        ) as sensor_id,
        CONCAT('GHI_FALLBACK_', ssm.device_id) as sensor_name,
        ssm.device_id as site_name,  -- Target site that needs fallback
        da_fallback.site_id,
        ghi_source.system,
        ghi_source.irradiance_w_m2,  -- Use GHI from source site
        'GHI' as sensor_type,
        TRUE as is_fallback  -- Mark as fallback
    FROM ghi_sensors ghi_source
    INNER JOIN {{ ref('seed_sensor_site_mapping') }} ssm
        ON ghi_source.site_name = ssm.logical_site_id  -- Source site (e.g., MMKI I)
        AND ssm.mapping_type = 'GHI_FALLBACK'
        AND (ssm.effective_date_start IS NULL OR ghi_source.date_key >= ssm.effective_date_start::date)
        AND (
            ssm.effective_date_end IS NULL 
            OR ssm.effective_date_end::text = ''
            OR (ssm.effective_date_end IS NOT NULL AND ssm.effective_date_end::text != '' 
                AND ghi_source.date_key <= ssm.effective_date_end::text::date)
        )
    LEFT JOIN {{ ref('dim_assets') }} da_fallback
        ON ssm.device_id = da_fallback.site_name 
        AND da_fallback.asset_level = 'Site'
    WHERE NOT EXISTS (
        -- Only add fallback if target site doesn't have its own GHI at this timestamp
        SELECT 1 
        FROM ghi_sensors ghi_own
        LEFT JOIN {{ ref('dim_assets') }} da_own
            ON ghi_own.site_name = da_own.site_name 
            AND da_own.asset_level = 'Site'
        WHERE da_own.site_id = da_fallback.site_id
            AND ghi_own.timestamp = ghi_source.timestamp
    )
    -- Skip GHI fallback from MMKI 1 for MMKI group sites (MMKI 1 GHI is problematic)
    -- MMKI group sites should use their own POA instead
    AND NOT (
        -- Check if source site is MMKI 1
        ghi_source.site_name LIKE '%MMKI%1.75%'
        OR ghi_source.site_name LIKE '%MMKI%Painting%'
        -- AND target site is MMKI group (Phase 2 or Phase 3)
        AND (
            ssm.device_id LIKE '%MMKI%Phase 2%'
            OR ssm.device_id LIKE '%MMKI%Phase 3%'
            OR ssm.device_id LIKE '%MMKI%4.292%'
            OR ssm.device_id LIKE '%MMKI%5.7%'
        )
    )
),

-- Step 2B: Get raw POA sensor data with window functions to detect stuck sensors  
poa_raw AS (
    SELECT 
        s.timestamp,
        s.date_key,
        s.asset_id as sensor_id,
        s.asset_name as sensor_name,
        s.site_name,
        da.site_id,
        s.system,
        s.metric_value as raw_irradiance,
        sc.sensor_type,
        -- Window functions to detect stuck sensors
        LAG(s.metric_value, 1) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_1,
        LAG(s.metric_value, 2) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_2,
        LAG(s.metric_value, 3) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_3,
        LAG(s.metric_value, 4) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_4,
        LAG(s.metric_value, 5) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as prev_5,
        LEAD(s.metric_value, 1) OVER (PARTITION BY s.asset_id, s.date_key ORDER BY s.timestamp) as next_1
    FROM {{ ref('mart_sensor_measurements_5min') }} s
    JOIN {{ ref('seed_sensor_config') }} sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON s.site_name = da.site_name 
        AND da.asset_level = 'Site'
    WHERE sc.sensor_type = 'POA'
        AND s.metric_name = 'irradiance'
        AND s.metric_value IS NOT NULL
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND s.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND s.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
        {% else %}
            AND s.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
),

-- Check if logical site has active POA_OVERRIDE (to exclude physical sensors from that site)
logical_site_override_check AS (
    SELECT DISTINCT
        logical_site_id,
        MIN(effective_date_start) as override_start_date,
        MAX(CASE WHEN effective_date_end IS NULL OR effective_date_end::text = '' THEN NULL ELSE effective_date_end END) as override_end_date
    FROM {{ ref('seed_sensor_site_mapping') }}
    WHERE mapping_type = 'POA_OVERRIDE'
    GROUP BY logical_site_id
),

-- Step 3: Apply POA override logic - sensors physically at one site but logically belong to another
-- POA sensors from MMKI II/III are physically stored at MMKI I, but logically belong to MMKI II/III
-- Exclude physical sensors from logical site when POA_OVERRIDE is active
poa_with_override AS (
    SELECT 
        pr.timestamp,
        pr.date_key,
        pr.sensor_id,
        pr.sensor_name,
        -- Apply POA override: use logical_site_id if override exists, otherwise use physical site_name
        COALESCE(ssm.logical_site_id, pr.site_name) as site_name,
        -- Get site_id for logical site (override target)
        COALESCE(da_override.site_id, pr.site_id) as site_id,
        pr.system,
        pr.raw_irradiance,
        pr.sensor_type,
        -- Validate irradiance: same validation rules as GHI
        -- A sensor is STUCK if it reports the same value 6+ times in a row (30 min+)
        -- A sensor is INVALID if it reports unrealistic values
        CASE 
            -- Stuck sensor detection: same value for 6+ consecutive intervals
            WHEN pr.raw_irradiance = pr.prev_1 
                AND pr.raw_irradiance = pr.prev_2 
                AND pr.raw_irradiance = pr.prev_3
                AND pr.raw_irradiance = pr.prev_4
                AND pr.raw_irradiance = pr.prev_5
                AND pr.raw_irradiance > 100  -- Only flag high values as stuck
            THEN NULL  -- Mark as stuck sensor
            -- Basic validation
            WHEN pr.raw_irradiance < -1  -- Invalid negative (except -1 for sensor off)
            THEN NULL
            WHEN pr.raw_irradiance > 2000  -- Unrealistic high value for POA
            THEN NULL
            WHEN EXTRACT(HOUR FROM pr.timestamp) < 6  -- Deep nighttime (00:00-05:59)
                AND pr.raw_irradiance > 50  -- Any significant reading at deep night is suspicious
            THEN NULL
            ELSE pr.raw_irradiance
        END as irradiance_w_m2,
        FALSE as is_fallback  -- POA sensors are not fallback (they're from same site, after override)
    FROM poa_raw pr
    LEFT JOIN {{ ref('seed_sensor_site_mapping') }} ssm
        ON ssm.device_id = SUBSTRING(pr.sensor_id FROM POSITION('_' IN pr.sensor_id) + 1)  -- Extract device_id from sensor_id (e.g., "FS_EM06102287046729" -> "EM06102287046729")
        AND ssm.mapping_type = 'POA_OVERRIDE'
        -- Check effective_date_start: only apply override if date >= effective_date_start
        -- If effective_date_start is NULL, apply override for all dates
        AND (
            ssm.effective_date_start IS NULL 
            OR ssm.effective_date_start::text = ''
            OR (
                ssm.effective_date_start IS NOT NULL 
                AND ssm.effective_date_start::text != ''
                AND pr.date_key >= ssm.effective_date_start::date
            )
        )
        AND (
            ssm.effective_date_end IS NULL 
            OR ssm.effective_date_end::text = ''
            OR (
                ssm.effective_date_end IS NOT NULL 
                AND ssm.effective_date_end::text != ''
                AND pr.date_key <= ssm.effective_date_end::text::date
            )
        )
    LEFT JOIN {{ ref('dim_assets') }} da_override
        ON COALESCE(ssm.logical_site_id, pr.site_name) = da_override.site_name
        AND da_override.asset_level = 'Site'
    LEFT JOIN logical_site_override_check lsoc
        ON pr.site_name = lsoc.logical_site_id
    WHERE 
        -- Exclude physical sensors from logical site when POA_OVERRIDE is active
        -- Only exclude if:
        -- 1. Physical site name matches logical site (sensor is from the logical site itself)
        -- 2. POA_OVERRIDE is active for that logical site
        -- 3. Sensor is not part of the override (doesn't have ssm.mapping_type = 'POA_OVERRIDE')
        -- 4. Date is within override effective period
        NOT (
            pr.site_name = lsoc.logical_site_id
            AND lsoc.logical_site_id IS NOT NULL
            AND ssm.mapping_type IS NULL  -- Sensor is NOT part of override
            AND (
                lsoc.override_start_date IS NULL 
                OR pr.date_key >= lsoc.override_start_date
            )
            AND (
                lsoc.override_end_date IS NULL 
                OR pr.date_key <= lsoc.override_end_date
            )
        )
),

-- Step 3B: Get POA sensors with validation (filter out stuck/malfunctioning sensors)
poa_sensors AS (
    SELECT 
        timestamp,
        date_key,
        sensor_id,
        sensor_name,
        site_name,
        site_id,
        system,
        irradiance_w_m2,
        sensor_type,
        is_fallback
    FROM poa_with_override
),

-- Step 4: Combine all possible irradiance sources for MIT calculation
-- Priority: 
--   - For MMKI group sites: POA (validated) > GHI (validated) > GHI_FALLBACK (skip MMKI 1)
--   - For other sites: GHI (validated) > POA (validated) > GHI_FALLBACK
-- All sources are validated - stuck/malfunctioning sensors filtered out
all_irradiance_sources AS (
    -- Priority 1: GHI from same site (non-fallback, validated)
    -- For MMKI group: Priority 2 (POA preferred)
    -- For other sites: Priority 1
    SELECT 
        timestamp,
        date_key,
        site_id,
        site_name,
        system,
        irradiance_w_m2,
        'GHI' as irradiance_source,
        CASE 
            WHEN site_name LIKE '%MMKI%' THEN 2  -- MMKI group: POA preferred
            ELSE 1  -- Other sites: GHI preferred
        END as priority,
        FALSE as is_fallback
    FROM ghi_with_fallback
    WHERE is_fallback = FALSE  -- Only non-fallback GHI (from same site)
        AND irradiance_w_m2 IS NOT NULL  -- Only include validated values
    
    UNION ALL
    
    -- Priority 2: POA from same site (validated)
    -- For MMKI group: Priority 1 (POA preferred)
    -- For other sites: Priority 2 (fallback if GHI not available)
    SELECT 
        timestamp,
        date_key,
        site_id,
        site_name,
        system,
        irradiance_w_m2,
        'POA' as irradiance_source,
        CASE 
            WHEN site_name LIKE '%MMKI%' THEN 1  -- MMKI group: POA preferred
            ELSE 2  -- Other sites: GHI preferred
        END as priority,
        FALSE as is_fallback
    FROM poa_sensors
    WHERE irradiance_w_m2 IS NOT NULL  -- Only include validated values
    
    UNION ALL
    
    -- Priority 3: GHI fallback from other site (last resort)
    -- Only used if both GHI and POA from same site are unavailable
    -- Skip GHI fallback from MMKI 1 for MMKI group sites (already filtered in ghi_with_fallback)
    SELECT 
        timestamp,
        date_key,
        site_id,
        site_name,
        system,
        irradiance_w_m2,
        'GHI_FALLBACK' as irradiance_source,
        3 as priority,
        TRUE as is_fallback
    FROM ghi_with_fallback
    WHERE is_fallback = TRUE  -- Only fallback GHI (from other site)
        AND irradiance_w_m2 IS NOT NULL  -- Only include validated values
),

-- Step 5: Get final site-level irradiance for MIT (one value per site per timestamp)
-- Use DISTINCT ON to get the best available source based on priority
site_irradiance_final AS (
    SELECT DISTINCT ON (timestamp, site_id)
        timestamp,
        date_key,
        site_id,
        site_name,
        system,
        irradiance_w_m2,
        irradiance_source,
        is_fallback
    FROM all_irradiance_sources
    ORDER BY timestamp, site_id, priority ASC  -- Lower priority number = higher priority
),

-- Step 6: Calculate MIT per site (site-level, same for all sensors at same site)
site_mit AS (
    SELECT 
        timestamp,
        date_key,
        site_id,
        site_name,
        system,
        irradiance_w_m2,
        irradiance_source,
        -- MIT calculation: CASE WHEN irradiance > 40 THEN 1 ELSE 0 END
        CASE 
            WHEN irradiance_w_m2 > 40 THEN 1
            ELSE 0
        END as mit
    FROM site_irradiance_final
),

-- Step 7: Combine all sensor data (GHI sensors + POA sensors for reference)
all_sensors AS (
    SELECT 
        timestamp,
        date_key,
        sensor_id,
        sensor_name,
        site_id,
        site_name,
        system,
        sensor_type,
        irradiance_w_m2,
        is_fallback
    FROM ghi_with_fallback
    
    UNION ALL
    
    SELECT 
        timestamp,
        date_key,
        sensor_id,
        sensor_name,
        site_id,
        site_name,
        system,
        sensor_type,
        irradiance_w_m2,
        is_fallback
    FROM poa_sensors
),

-- Step 8: Join sensor data with site-level MIT
sensor_calculations AS (
    SELECT 
        s.timestamp,
        s.date_key,
        s.sensor_id,
        s.sensor_name,
        s.site_id,
        s.site_name,
        s.system,
        s.sensor_type,
        s.irradiance_w_m2,
        s.is_fallback,
        m.mit,  -- Site-level MIT (same for all sensors at same site)
        m.irradiance_source as mit_irradiance_source,  -- Track which source was used for MIT
        NOW() as calculation_timestamp
    FROM all_sensors s
    LEFT JOIN site_mit m
        ON s.timestamp = m.timestamp
        AND s.site_id = m.site_id
)

SELECT 
    timestamp,
    date_key,
    sensor_id,
    sensor_name,
    site_id,
    site_name,
    system,
    sensor_type,
    irradiance_w_m2,
    is_fallback,
    mit,
    mit_irradiance_source,  -- Track which source was used for MIT: 'GHI', 'POA', or 'GHI_FALLBACK'
    calculation_timestamp
FROM sensor_calculations
ORDER BY timestamp, sensor_id

