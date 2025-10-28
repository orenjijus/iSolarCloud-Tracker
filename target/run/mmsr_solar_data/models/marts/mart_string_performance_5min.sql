
  
    

  create  table "MMSR"."public"."mart_string_performance_5min__dbt_tmp"
  
  
    as
  
  (
    

-- 5-minute string-level performance fact table
SELECT 
    s.timestamp_5min,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    s.metric_id,
    s.measurement_type,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    s.metric_value
FROM "MMSR"."public"."int_strings_unified_5min" s
LEFT JOIN "MMSR"."public"."dim_assets" da 
    ON CONCAT(
        UPPER(LEFT(s.system, 3)), '_', s.device_ps_key
    ) = da.asset_id
LEFT JOIN "MMSR"."public"."dim_date_generated" dd 
    ON dd.date_key = DATE(s.timestamp_5min)
LEFT JOIN "MMSR"."public"."seed_metric_mapper" m 
    ON s.metric_id = m.metric_id 
    AND m.used = 'yes'
WHERE s.metric_value IS NOT NULL
  );
  