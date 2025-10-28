
      
  
    

  create  table "MMSR"."public"."int_sensors_unified__dbt_tmp"
  
  
    as
  
  (
    

-- Unified sensor/weather station data
WITH isolarcloud_sensors AS (
    SELECT 
        p.timestamp,
        p.device_ps_key,
        p.metric_id,
        p.metric_value,
        d.device_name,
        s.ps_name as site_name
    FROM "MMSR"."public"."stg_isolarcloud__perf_unpivoted" p
    JOIN "MMSR"."public"."stg_isolarcloud__devices" d ON p.device_ps_key = d.device_ps_key
    JOIN "MMSR"."public"."stg_isolarcloud__sites" s ON d.ps_id = s.ps_id
    WHERE d.device_type = 5  -- Sensors/Weather stations (device_type 5 = Meteo Station)
    
),

fusionsolar_sensors AS (
    SELECT 
        p.timestamp,
        p.dev_id as device_ps_key,
        p.metric_id,
        p.metric_value,
        p.device_name,
        p.plant_name as site_name
    FROM "MMSR"."public"."stg_fusionsolar__perf_unpivoted" p
    JOIN "MMSR"."public"."stg_fusionsolar__devices" d ON p.dev_id = d.dev_id
    WHERE d.dev_type_id = 10  -- Sensors (dev_type_id 10 = Meteo Station)
    
)

SELECT 
    'isolarcloud' as system,
    timestamp,
    device_ps_key,
    device_name,
    site_name,
    metric_id,
    CAST(metric_value AS NUMERIC) as metric_value
FROM isolarcloud_sensors

UNION ALL

SELECT 
    'fusionsolar' as system,
    timestamp,
    device_ps_key,
    device_name,
    site_name,
    metric_id,
    CAST(metric_value AS NUMERIC) as metric_value
FROM fusionsolar_sensors
  );
  
  