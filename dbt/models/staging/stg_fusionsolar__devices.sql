{{ config(materialized='table') }}

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
FROM {{ source('raw', 'fusionsolar_devices') }}

