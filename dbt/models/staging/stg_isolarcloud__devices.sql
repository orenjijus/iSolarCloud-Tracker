{{ config(materialized='table') }}

SELECT 
    device_ps_key,
    ps_id,
    device_type,
    type_name,
    device_name,
    device_sn,
    CASE 
        WHEN device_type = 1 THEN 'Inverter'
        WHEN device_type = 7 THEN 'Meter'
        WHEN device_type = 5 THEN 'Meteo Station'
        ELSE 'Unknown'
    END as device_category
FROM {{ source('raw', 'isolarcloud_devices') }}

