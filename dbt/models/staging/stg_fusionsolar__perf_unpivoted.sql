{{ config(
    materialized='incremental',
    unique_key=['timestamp', 'dev_id', 'metric_id'],
    tags=['intraday'],
    meta={'hyperscale': true}
) }}

-- Unpivot FusionSolar historical data from JSONB to long format
SELECT 
    h.collect_time as timestamp,
    h.dev_id,
    d.dev_name as device_name,
    d.plant_code,
    p.plant_name,
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
        collect_time,
        dev_id,
        -- Extract all keys from the JSONB object
        jsonb_object_keys(measurement_data) as metric_id,
        measurement_data->jsonb_object_keys(measurement_data) as metric_value
    FROM {{ source('raw', 'fusionsolar_historical_data') }} h
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            -- Override window mode: re-ingest specific date range
            WHERE h.collect_time >= '{{ var("reingest_start_date") }}'::timestamp
              AND h.collect_time < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_plant_codes', none) %}
                -- Optional: filter by specific plants (comma-separated string)
                AND h.dev_id IN (
                    SELECT dev_id 
                    FROM {{ ref('stg_fusionsolar__devices') }}
                    WHERE plant_code = ANY(string_to_array('{{ var("reingest_plant_codes") }}', ',')::VARCHAR[])
                )
            {% endif %}
            {% if var('reingest_device_ids', none) %}
                -- Optional: filter by specific device IDs (comma-separated string)
                AND h.dev_id = ANY(string_to_array('{{ var("reingest_device_ids") }}', ',')::VARCHAR[])
            {% endif %}
        {% else %}
            -- Default incremental: only new data
            WHERE h.collect_time > (SELECT MAX(timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
) h
LEFT JOIN {{ ref('stg_fusionsolar__devices') }} d 
    ON h.dev_id = d.dev_id
LEFT JOIN {{ ref('stg_fusionsolar__sites') }} p 
    ON d.plant_code = p.plant_code
WHERE metric_value IS NOT NULL  -- Filter out null values
  AND (jsonb_typeof(metric_value) = 'string' OR jsonb_typeof(metric_value) = 'number')  -- Only numeric-like values

