{{ config(
    materialized='incremental',
    unique_key=['timestamp', 'dev_id', 'metric_id'],
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
    FROM {{ source('raw', 'fusionsolar_historical_data') }}
    {% if is_incremental() %}
        WHERE collect_time > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
) h
LEFT JOIN {{ ref('stg_fusionsolar__devices') }} d 
    ON h.dev_id = d.dev_id
LEFT JOIN {{ ref('stg_fusionsolar__sites') }} p 
    ON d.plant_code = p.plant_code
WHERE metric_value IS NOT NULL  -- Filter out null values
  AND (jsonb_typeof(metric_value) = 'string' OR jsonb_typeof(metric_value) = 'number')  -- Only numeric-like values

