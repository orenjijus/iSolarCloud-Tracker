-- Device Capacity Reference Table
-- Use this to get device capacities for Excel reference
-- Copy to Excel Sheet 3 for capacity lookup

SELECT 
    site_name as "Site",
    sensor_dev_name as "Device",
    asset_id as "Asset_ID",
    ROUND(sensor_capacity_kwp::numeric, 2) as "Capacity_kWp",
    sensor_type as "Sensor_Type"
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- ⚠️ CHANGE THIS to your target site
    AND date_key = (
        SELECT MAX(date_key) 
        FROM mart.mart_sensor_daily 
        WHERE sensor_type = 'POA' 
        AND site_name = 'Garuda Metalindo 1'  -- ⚠️ CHANGE THIS
    )
GROUP BY site_name, sensor_dev_name, asset_id, sensor_capacity_kwp, sensor_type
ORDER BY sensor_dev_name;

