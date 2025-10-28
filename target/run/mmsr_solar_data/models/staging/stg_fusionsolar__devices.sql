
  create view "MMSR"."public"."stg_fusionsolar__devices__dbt_tmp"
    
    
  as (
    

SELECT 
    dev_id,
    plant_code,
    dev_type_id,
    dev_name,
    inv_type,
    CASE 
        WHEN dev_type_id = 1 THEN 'Inverter'
        WHEN dev_type_id = 17 THEN 'Meter'
        WHEN dev_type_id = 10 THEN 'Meteo Station'
        ELSE 'Unknown'
    END as device_category
FROM "MMSR"."public"."fusionsolar_devices"
  );