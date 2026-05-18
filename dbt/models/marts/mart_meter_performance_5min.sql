{{ config(
    materialized='incremental',
    tags=['intraday'],
    schema='mart',
    unique_key=['timestamp', 'asset_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp', 'asset_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute meter performance fact table
-- Built directly from staging (no intermediate layer)
-- Filters by metric_group = 'meter' (and SmartAssistant: 'smart_assistant') from seed_metric_mapper
WITH isolarcloud_meters AS (
    SELECT 
        p.timestamp,
        p.device_ps_key,
        p.metric_id,
        p.metric_value,
        d.device_name,
        s.ps_name as site_name,
        d.device_type as dev_type_id
    FROM {{ ref('stg_isolarcloud__perf_unpivoted') }} p
    JOIN {{ ref('stg_isolarcloud__devices') }} d ON p.device_ps_key = d.device_ps_key
    JOIN {{ ref('stg_isolarcloud__sites') }} s ON d.ps_id = s.ps_id
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'isolarcloud'
        AND mm.metric_group = 'meter'
        AND mm.used = 'yes'
    WHERE d.device_type = 7  -- Meters
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND p.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND p.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_ps_ids', none) %}
                -- Filter by specific sites
                AND d.ps_id = ANY(string_to_array('{{ var("reingest_ps_ids") }}', ',')::VARCHAR[])
            {% endif %}
            {% if var('reingest_device_ids', none) %}
                -- Filter by specific device IDs
                AND p.device_ps_key = ANY(string_to_array('{{ var("reingest_device_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            -- Default incremental
            AND p.timestamp > (
                SELECT MAX(timestamp) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            )
        {% endif %}
    {% endif %}
),

fusionsolar_meters AS (
    SELECT 
        p.timestamp,
        p.dev_id as device_ps_key,
        p.metric_id,
        p.metric_value,
        p.device_name,
        p.plant_name as site_name,
        d.dev_type_id as dev_type_id
    FROM {{ ref('stg_fusionsolar__perf_unpivoted') }} p
    JOIN {{ ref('stg_fusionsolar__devices') }} d ON p.dev_id = d.dev_id
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'fusionsolar'
        AND mm.metric_group = 'meter'
        AND mm.used = 'yes'
    WHERE d.dev_type_id = 17  -- Meters (dev_type_id 17 = Meter)
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window: re-process same date range
            AND p.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND p.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_plant_codes', none) %}
                -- Filter by specific plants
                AND d.plant_code = ANY(string_to_array('{{ var("reingest_plant_codes") }}', ',')::VARCHAR[])
            {% endif %}
            {% if var('reingest_device_ids', none) %}
                -- Filter by specific device IDs
                AND p.dev_id = ANY(string_to_array('{{ var("reingest_device_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            -- Default incremental
            AND p.timestamp > (
                SELECT MAX(timestamp) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            )
        {% endif %}
    {% endif %}
),

fusionsolar_smart_assistant AS (
    SELECT 
        p.timestamp,
        p.dev_id as device_ps_key,
        p.metric_id,
        p.metric_value,
        p.device_name,
        p.plant_name as site_name,
        d.dev_type_id as dev_type_id
    FROM {{ ref('stg_fusionsolar__perf_unpivoted') }} p
    JOIN {{ ref('stg_fusionsolar__devices') }} d ON p.dev_id = d.dev_id
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'fusionsolar'
        AND mm.metric_group = 'smart_assistant'
        AND mm.used = 'yes'
    WHERE d.dev_type_id = 23070  -- SmartAssistant
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            AND p.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
            AND p.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_plant_codes', none) %}
                AND d.plant_code = ANY(string_to_array('{{ var("reingest_plant_codes") }}', ',')::VARCHAR[])
            {% endif %}
            {% if var('reingest_device_ids', none) %}
                AND p.dev_id = ANY(string_to_array('{{ var("reingest_device_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            AND p.timestamp > (
                SELECT MAX(timestamp) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            )
        {% endif %}
    {% endif %}
),

unified_meters AS (
    SELECT 
        'isolarcloud' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        dev_type_id,
        CAST(metric_value AS NUMERIC) as metric_value
    FROM isolarcloud_meters
    WHERE metric_value IS NOT NULL
      AND CAST(metric_value AS TEXT) NOT IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity')
    
    UNION ALL
    
    SELECT 
        'fusionsolar' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        dev_type_id,
        CAST(metric_value AS NUMERIC) as metric_value
    FROM fusionsolar_meters
    WHERE metric_value IS NOT NULL
      AND CAST(metric_value AS TEXT) NOT IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity')

    UNION ALL

    SELECT 
        'fusionsolar' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        dev_type_id,
        CAST(metric_value AS NUMERIC) as metric_value
    FROM fusionsolar_smart_assistant
    WHERE metric_value IS NOT NULL
      AND CAST(metric_value AS TEXT) NOT IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity')
),

-- Apply meter ID consolidation and name override mapping
meters_with_mapping AS (
    SELECT 
        m.timestamp,
        m.system,
        -- Apply ID consolidation: use logical_device_id if exists, else use device_ps_key
        COALESCE(
            CASE 
                WHEN msm.mapping_type = 'ID_CONSOLIDATION'
                    AND (msm.effective_date_start IS NULL OR msm.effective_date_start::text = '' OR DATE(m.timestamp) >= msm.effective_date_start::date)
                    AND (
                        msm.effective_date_end IS NULL 
                        OR msm.effective_date_end::text = ''
                        OR (msm.effective_date_end IS NOT NULL AND msm.effective_date_end::text != '' AND DATE(m.timestamp) <= msm.effective_date_end::date)
                    )
                THEN msm.logical_device_id  -- Use consolidated ID
                ELSE NULL
            END,
            m.device_ps_key  -- Use original ID
        ) as device_ps_key,
        -- Apply name override: use logical_device_name if exists, else use device_name
        COALESCE(
            CASE 
                WHEN msm.mapping_type IN ('NAME_OVERRIDE', 'ID_CONSOLIDATION')
                    AND msm.logical_device_name IS NOT NULL
                    AND msm.logical_device_name::text != ''
                    AND (msm.effective_date_start IS NULL OR msm.effective_date_start::text = '' OR DATE(m.timestamp) >= msm.effective_date_start::date)
                    AND (
                        msm.effective_date_end IS NULL 
                        OR msm.effective_date_end::text = ''
                        OR (msm.effective_date_end IS NOT NULL AND msm.effective_date_end::text != '' AND DATE(m.timestamp) <= msm.effective_date_end::date)
                    )
                THEN msm.logical_device_name  -- Use corrected name
                ELSE NULL
            END,
            m.device_name  -- Use original name
        ) as device_name,
        m.site_name,
        m.metric_id,
        m.metric_value,
        m.dev_type_id
    FROM unified_meters m
    LEFT JOIN {{ ref('seed_meter_site_mapping') }} msm
        ON m.device_ps_key = msm.device_id  -- Match by original device_id
        AND m.system = 'isolarcloud'  -- Currently only for iSolarCloud, can extend for FusionSolar if needed
)

SELECT 
    m.timestamp,
    dd.date_key,
    (EXTRACT(HOUR FROM m.timestamp) * 60 + EXTRACT(MINUTE FROM m.timestamp))::int / 5 AS minute_key,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    m.metric_id,
    mm.unified_name as metric_name,
    mm.metric_group,
    mm.metric_unit,
    m.metric_value,
    mc.meter_type,
    mc.voltage_level,
    mc.dev_name as meter_dev_name
FROM meters_with_mapping m
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT(
        CASE 
            WHEN m.system = 'fusionsolar' THEN 'FS'
            WHEN m.system = 'isolarcloud' THEN 'ISO'
            ELSE UPPER(LEFT(m.system, 3))
        END, '_', m.device_ps_key
    ) = da.asset_id
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(m.timestamp)
LEFT JOIN {{ ref('seed_metric_mapper') }} mm 
    ON m.metric_id = mm.metric_id 
    AND mm.used = 'yes'
    AND mm.platform = CASE m.system
        WHEN 'isolarcloud' THEN 'isolarcloud'
        WHEN 'fusionsolar' THEN 'fusionsolar'
    END
    AND mm.device_type = m.dev_type_id
LEFT JOIN {{ ref('meter_config_reference') }} mc 
    ON m.device_ps_key = mc.esn_code
    AND (
        (m.system = 'isolarcloud' AND mc."Source" = 'iSolarCloud')
        OR (m.system = 'fusionsolar' AND mc."Source" = 'FusionSolar')
    )
WHERE m.metric_value IS NOT NULL
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: ensure we only process the re-ingest range
        AND m.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
        AND m.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
    {% else %}
        -- Default incremental: filtering already done per-system in CTEs above
        -- No additional filter needed here to avoid filtering out data from one system
        -- when the other system has newer timestamps
    {% endif %}
{% endif %}

