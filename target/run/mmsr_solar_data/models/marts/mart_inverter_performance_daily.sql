
  
    

  create  table "MMSR"."public"."mart_inverter_performance_daily__dbt_tmp"
  
  
    as
  
  (
    

-- Daily inverter-level aggregated performance
SELECT 
    dd.date_key,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    m.metric_id,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    AVG(i.metric_value) as avg_value,
    MAX(i.metric_value) as max_value,
    MIN(i.metric_value) as min_value,
    COUNT(*) as measurement_count
FROM "MMSR"."public"."mart_inverter_performance_5min" i
LEFT JOIN "MMSR"."public"."dim_assets" da ON i.asset_id = da.asset_id
LEFT JOIN "MMSR"."public"."dim_date_generated" dd ON dd.date_key = DATE(i.timestamp_5min)
LEFT JOIN "MMSR"."public"."seed_metric_mapper" m ON i.metric_id = m.metric_id AND m.used = 'yes'
GROUP BY dd.date_key, da.asset_id, da.asset_name, da.site_name, da.system, m.metric_id, m.unified_name, m.metric_group, m.metric_unit
  );
  