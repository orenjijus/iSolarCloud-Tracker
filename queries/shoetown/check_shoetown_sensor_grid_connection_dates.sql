-- Check grid connection dates for Shoetown sensors to determine sensor repositioning
-- This will help identify which sensors override others based on their grid connection dates

SELECT 
    d.device_ps_key,
    d.device_name,
    d.grid_connection_date,
    d.device_type,
    d.type_name,
    sc.device_id,
    sc.dev_name,
    sc.sensor_type,
    sc.sensor_capacity
FROM raw.isolarcloud_devices d
LEFT JOIN staging.seed_sensor_config sc 
    ON CONCAT('ISO_', d.device_ps_key) = sc.device_id
WHERE d.ps_id = '1479456'
    AND (
        d.device_name LIKE '%IRR%' 
        OR d.device_name LIKE '%PYR%'
        OR d.device_name LIKE '%Meteo%'
        OR sc.sensor_type IN ('POA', 'GHI')
    )
ORDER BY 
    d.grid_connection_date NULLS LAST,
    d.device_name;

