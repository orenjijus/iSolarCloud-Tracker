
      
  
    

  create  table "MMSR"."public"."mart_sensor_measurements_5min__dbt_tmp"
  
  
    as
  
  (
    

-- 5-minute sensor measurements fact table
SELECT 
    s.timestamp,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    s.metric_id,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    s.metric_value,
    sc.sensor_type,
    sc.dev_name as sensor_dev_name
FROM "MMSR"."public"."int_sensors_unified" s
LEFT JOIN "MMSR"."public"."dim_assets" da 
    ON CONCAT(
        CASE 
            WHEN s.system = 'fusionsolar' THEN 'FS'
            WHEN s.system = 'isolarcloud' THEN 'ISO'
            ELSE UPPER(LEFT(s.system, 3))
        END, '_', s.device_ps_key
    ) = da.asset_id
LEFT JOIN "MMSR"."public"."dim_date_generated" dd 
    ON dd.date_key = DATE(s.timestamp)
LEFT JOIN "MMSR"."public"."seed_metric_mapper" m 
    ON s.metric_id = m.metric_id 
    AND m.used = 'yes'
LEFT JOIN "MMSR"."public"."seed_sensor_config" sc 
    ON s.device_ps_key = sc.device_id
WHERE s.metric_value IS NOT NULL

  );
  
  