{{ config(
    materialized='incremental',
    tags=['intraday'],
    schema='mart',
    unique_key=['timestamp', 'asset_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp', 'asset_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'},
        {'columns': ['timestamp'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute inverter performance fact table
-- Built directly from staging (no intermediate layer)
-- Filters by metric_group = 'inverter' from seed_metric_mapper
WITH isolarcloud_inverters AS (
    SELECT 
        p.timestamp,
        p.device_ps_key,
        p.metric_id,
        p.metric_value,
        d.device_name,
        s.ps_name as site_name
    FROM {{ ref('stg_isolarcloud__perf_unpivoted') }} p
    JOIN {{ ref('stg_isolarcloud__devices') }} d ON p.device_ps_key = d.device_ps_key
    JOIN {{ ref('stg_isolarcloud__sites') }} s ON d.ps_id = s.ps_id
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'isolarcloud'
        AND mm.metric_group = 'inverter'
        AND mm.used = 'yes'
    WHERE d.device_type = 1  -- Inverters
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

fusionsolar_inverters AS (
    SELECT 
        p.timestamp,
        p.dev_id as device_ps_key,
        p.metric_id,
        p.metric_value,
        p.device_name,
        p.plant_name as site_name
    FROM {{ ref('stg_fusionsolar__perf_unpivoted') }} p
    JOIN {{ ref('stg_fusionsolar__devices') }} d ON p.dev_id = d.dev_id
    JOIN {{ ref('seed_metric_mapper') }} mm 
        ON p.metric_id = mm.metric_id 
        AND mm.platform = 'fusionsolar'
        AND mm.metric_group = 'inverter'
        AND mm.used = 'yes'
    WHERE d.dev_type_id = 1  -- Inverters
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

unified_inverters AS (
    SELECT 
        'isolarcloud' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        CAST(metric_value AS NUMERIC) as metric_value
    FROM isolarcloud_inverters
    WHERE metric_value IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'fusionsolar' as system,
        timestamp,
        device_ps_key,
        device_name,
        site_name,
        metric_id,
        CAST(metric_value AS NUMERIC) as metric_value
    FROM fusionsolar_inverters
    WHERE metric_value IS NOT NULL
),

inverters_with_metrics AS (
    SELECT 
        i.timestamp,
        i.system,
        i.device_ps_key,
        i.device_name,
        i.site_name,
        i.metric_id,
        i.metric_value,
        m.unified_name as metric_name,
        m.metric_group,
        m.metric_unit
    FROM unified_inverters i
    LEFT JOIN {{ ref('seed_metric_mapper') }} m 
        ON i.metric_id = m.metric_id 
        AND i.system = m.platform
        AND m.metric_group = 'inverter'
        AND m.used = 'yes'
    WHERE i.metric_value IS NOT NULL
),

-- Apply inverter ID consolidation and name override mapping
inverters_with_mapping AS (
    SELECT 
        i.timestamp,
        i.system,
        -- Apply ID consolidation: use logical_device_id if exists, else use device_ps_key
        COALESCE(
            CASE 
                WHEN ism.mapping_type = 'ID_CONSOLIDATION'
                    AND (ism.effective_date_start IS NULL OR DATE(i.timestamp) >= ism.effective_date_start)
                    AND (
                        ism.effective_date_end IS NULL 
                        OR ism.effective_date_end::text = ''
                        OR (ism.effective_date_end IS NOT NULL AND ism.effective_date_end::text != '' AND DATE(i.timestamp) <= ism.effective_date_end::date)
                    )
                THEN ism.logical_device_id  -- Use consolidated ID
                ELSE NULL
            END,
            i.device_ps_key  -- Use original ID
        ) as device_ps_key,
        -- Apply name override: use logical_device_name if exists, else use device_name
        COALESCE(
            CASE 
                WHEN ism.mapping_type IN ('NAME_OVERRIDE', 'ID_CONSOLIDATION')
                    AND ism.logical_device_name IS NOT NULL
                    AND ism.logical_device_name::text != ''
                    AND (ism.effective_date_start IS NULL OR DATE(i.timestamp) >= ism.effective_date_start)
                    AND (
                        ism.effective_date_end IS NULL 
                        OR ism.effective_date_end::text = ''
                        OR (ism.effective_date_end IS NOT NULL AND ism.effective_date_end::text != '' AND DATE(i.timestamp) <= ism.effective_date_end::date)
                    )
                THEN ism.logical_device_name  -- Use corrected name
                ELSE NULL
            END,
            i.device_name  -- Use original name
        ) as device_name,
        i.site_name,
        i.metric_id,
        i.metric_value,
        i.metric_name,
        i.metric_group,
        i.metric_unit
    FROM inverters_with_metrics i
    LEFT JOIN {{ ref('seed_inverter_site_mapping') }} ism
        ON i.device_ps_key = ism.device_id  -- Match by original device_id
        AND i.system = 'isolarcloud'  -- Currently only for iSolarCloud, can extend for FusionSolar if needed
)

SELECT 
    i.timestamp,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    i.metric_id,
    i.metric_name,
    i.metric_group,
    i.metric_unit,
    i.metric_value
FROM inverters_with_mapping i
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT(
        CASE 
            WHEN i.system = 'isolarcloud' THEN 'ISO'
            WHEN i.system = 'fusionsolar' THEN 'FS'
            ELSE UPPER(LEFT(i.system, 3))
        END, 
        '_', 
        i.device_ps_key
    ) = da.asset_id
    AND da.asset_level = 'Device'
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(i.timestamp)
WHERE i.metric_value IS NOT NULL
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: ensure we only process the re-ingest range
        AND i.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
        AND i.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
    {% else %}
        -- Default incremental: filtering already done per-system in CTEs above
        -- No additional filter needed here to avoid filtering out data from one system
        -- when the other system has newer timestamps
    {% endif %}
{% endif %}

