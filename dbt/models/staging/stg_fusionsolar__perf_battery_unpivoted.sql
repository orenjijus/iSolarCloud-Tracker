{{ config(
    materialized='incremental',
    unique_key=['timestamp', 'dev_id', 'metric_id'],
    tags=['intraday'],
    meta={'hyperscale': true}
) }}

-- Unpivot FusionSolar historical data for battery devices (dev_type_id 39) only.
SELECT
    h.collect_time AS timestamp,
    h.dev_id,
    d.dev_name AS device_name,
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
    END AS metric_value
FROM (
    SELECT
        collect_time,
        dev_id,
        jsonb_object_keys(measurement_data) AS metric_id,
        measurement_data->jsonb_object_keys(measurement_data) AS metric_value
    FROM {{ source('raw', 'fusionsolar_historical_data') }} h
    {% if is_incremental() %}
        {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
            WHERE h.collect_time >= '{{ var("reingest_start_date") }}'::timestamp
              AND h.collect_time < ('{{ var("reingest_end_date") }}'::date + INTERVAL '1 day')
            {% if var('reingest_plant_codes', none) %}
                AND h.dev_id IN (
                    SELECT dev_id
                    FROM {{ ref('stg_fusionsolar__devices') }}
                    WHERE plant_code = ANY(string_to_array('{{ var("reingest_plant_codes") }}', ',')::VARCHAR[])
                      AND dev_type_id = 39
                )
            {% endif %}
        {% else %}
            WHERE h.collect_time > (SELECT COALESCE(MAX(timestamp), '1970-01-01'::timestamp) FROM {{ this }})
        {% endif %}
    {% endif %}
) h
INNER JOIN {{ ref('stg_fusionsolar__devices') }} d
    ON h.dev_id = d.dev_id
    AND d.dev_type_id = 39
LEFT JOIN {{ ref('stg_fusionsolar__sites') }} p
    ON d.plant_code = p.plant_code
WHERE metric_value IS NOT NULL
  AND (jsonb_typeof(metric_value) = 'string' OR jsonb_typeof(metric_value) = 'number')
