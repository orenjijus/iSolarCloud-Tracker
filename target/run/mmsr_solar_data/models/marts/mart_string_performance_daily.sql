
  
    

  create  table "MMSR"."public"."mart_string_performance_daily__dbt_tmp"
  
  
    as
  
  (
    

-- Daily string-level aggregated performance
SELECT 
    dd.date_key,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    s.measurement_type,
    m.metric_id,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    AVG(s.metric_value) as avg_value,
    MAX(s.metric_value) as max_value,
    MIN(s.metric_value) as min_value,
    COUNT(*) as measurement_count
FROM "MMSR"."public"."mart_string_performance_5min" s
LEFT JOIN "MMSR"."public"."dim_assets" da ON s.asset_id = da.asset_id
LEFT JOIN "MMSR"."public"."dim_date_generated" dd ON dd.date_key = DATE(s.timestamp_5min)
LEFT JOIN "MMSR"."public"."seed_metric_mapper" m ON s.metric_id = m.metric_id AND m.used = 'yes'
GROUP BY dd.date_key, da.asset_id, da.asset_name, da.site_name, da.system, s.measurement_type, m.metric_id, m.unified_name, m.metric_group, m.metric_unit
  );
  