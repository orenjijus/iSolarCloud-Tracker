
      
  
    

  create  table "MMSR"."public"."stg_isolarcloud__perf_unpivoted__dbt_tmp"
  
  
    as
  
  (
    

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
    FROM "MMSR"."public"."isolarcloud_historical_data"
    
) h
LEFT JOIN "MMSR"."public"."stg_isolarcloud__devices" d 
    ON h.device_ps_key = d.device_ps_key
LEFT JOIN "MMSR"."public"."stg_isolarcloud__sites" ps 
    ON d.ps_id = ps.ps_id
WHERE metric_value IS NOT NULL  -- Filter out null values
  AND (jsonb_typeof(metric_value) = 'string' OR jsonb_typeof(metric_value) = 'number')  -- Only numeric-like values
  );
  
  