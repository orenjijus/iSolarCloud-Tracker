{{ config(
    materialized='incremental',
    unique_key=['timestamp', 'device_ps_key', 'metric_id'],
    meta={'hyperscale': true}
) }}

-- Unpivot iSolarCloud historical data from JSONB to long format
SELECT 
    h.timestamp,
    h.device_ps_key,
    d.device_name,
    d.ps_id,
    ps.ps_name,
    ps.ps_name_clean,
    metric_id,
    CASE 
        WHEN jsonb_typeof(metric_value) = 'string' THEN 
            CASE 
                WHEN (metric_value#>> '{}') IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity') THEN NULL
                ELSE (metric_value#>> '{}')::numeric
            END
        WHEN jsonb_typeof(metric_value) = 'number' THEN 
            CASE 
                WHEN metric_value::text IN ('nan', '-nan', 'infinity', '-infinity', 'Infinity', '-Infinity') THEN NULL
                ELSE metric_value::numeric
            END
        ELSE NULL
    END as metric_value
FROM (
    SELECT 
        timestamp,
        device_ps_key,
        -- Extract all keys from the JSONB object
        jsonb_object_keys(measurement_data) as metric_id,
        measurement_data->jsonb_object_keys(measurement_data) as metric_value
    FROM {{ source('raw', 'isolarcloud_historical_data') }} h
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window mode: re-ingest specific date range
            WHERE h.timestamp >= '{{ var("reingest_start_date") }}'::timestamp
              AND h.timestamp < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_ps_ids', none) %}
                -- Optional: filter by specific sites (comma-separated string)
                AND h.device_ps_key IN (
                    SELECT device_ps_key 
                    FROM {{ ref('stg_isolarcloud__devices') }}
                    WHERE ps_id = ANY(string_to_array('{{ var("reingest_ps_ids") }}', ',')::VARCHAR[])
                )
            {% endif %}
            {% if var('reingest_device_ids', none) %}
                -- Optional: filter by specific device IDs (comma-separated string)
                AND h.device_ps_key = ANY(string_to_array('{{ var("reingest_device_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            -- Default incremental: only new data
            WHERE h.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
) h
LEFT JOIN {{ ref('stg_isolarcloud__devices') }} d 
    ON h.device_ps_key = d.device_ps_key
LEFT JOIN {{ ref('stg_isolarcloud__sites') }} ps 
    ON d.ps_id = ps.ps_id
WHERE metric_value IS NOT NULL  -- Filter out null values
  AND (jsonb_typeof(metric_value) = 'string' OR jsonb_typeof(metric_value) = 'number')  -- Only numeric-like values

