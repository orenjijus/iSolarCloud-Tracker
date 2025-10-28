
      
  
    

  create  table "MMSR"."public"."mart_meter_performance_5min__dbt_tmp"
  
  
    as
  
  (
    

-- 5-minute meter performance fact table
SELECT 
    m.timestamp,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    m.metric_id,
    mm.unified_name as metric_name,
    mm.metric_group,
    mm.metric_unit,
    m.metric_value,
    mc.meter_type,
    mc.voltage_level,
    mc.dev_name as meter_dev_name
FROM "MMSR"."public"."int_meters_unified" m
LEFT JOIN "MMSR"."public"."dim_assets" da 
    ON CONCAT(
        CASE 
            WHEN m.system = 'fusionsolar' THEN 'FS'
            WHEN m.system = 'isolarcloud' THEN 'ISO'
            ELSE UPPER(LEFT(m.system, 3))
        END, '_', m.device_ps_key
    ) = da.asset_id
LEFT JOIN "MMSR"."public"."dim_date_generated" dd 
    ON dd.date_key = DATE(m.timestamp)
LEFT JOIN "MMSR"."public"."seed_metric_mapper" mm 
    ON m.metric_id = mm.metric_id 
    AND mm.used = 'yes'
LEFT JOIN "MMSR"."public"."seed_meter_config" mc 
    ON m.device_ps_key = mc.esn_code
WHERE m.metric_value IS NOT NULL

  );
  
  