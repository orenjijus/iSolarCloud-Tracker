-- ============================================
-- Check All Shoetown Devices in Raw Database
-- ============================================
-- This query checks all devices for Shoetown (1479456) in raw.isolarcloud_devices
-- to find devices that might not be in sensor_config yet
-- 
-- Note: Devices synced from isolarcloud harvester go to raw.isolarcloud_devices
-- but need to be manually added to seed_sensor_config.csv
-- ============================================

-- ============================================
-- 1. ALL DEVICES - Overview
-- ============================================
SELECT 
    d.device_ps_key,
    d.device_name,
    d.device_type,
    d.type_name,
    d.grid_connection_date,
    d.dev_status,
    d.ps_id,
    -- Check if device is already in sensor_config
    CASE 
        WHEN sc.device_id IS NOT NULL THEN '✅ IN_CONFIG'
        ELSE '❌ NOT_IN_CONFIG - NEEDS TO BE ADDED'
    END as config_status,
    sc.device_id as config_device_id,
    sc.sensor_type,
    sc.sensor_capacity,
    -- Suggest sensor type based on device name/type
    CASE 
        WHEN d.device_name LIKE '%IRR%' OR d.device_name LIKE '%irr%' THEN 'POA (suggested)'
        WHEN d.device_name LIKE '%PYR%' OR d.device_name LIKE '%pyr%' THEN 'GHI (suggested)'
        WHEN d.type_name = 'Meteo Station' THEN 'Check if has POA/GHI sensor'
        ELSE 'Unknown - check device'
    END as suggested_sensor_type
FROM raw.isolarcloud_devices d
LEFT JOIN staging.seed_sensor_config sc 
    ON CONCAT('ISO_', d.device_ps_key) = sc.device_id
WHERE d.ps_id = '1479456'
ORDER BY 
    CASE 
        WHEN sc.device_id IS NULL THEN 0  -- Show NOT_IN_CONFIG first
        ELSE 1
    END,
    d.device_type,
    d.grid_connection_date NULLS LAST,
    d.device_name;

-- ============================================
-- 2. METEO STATIONS (device_type = 5) - Focus on finding Meteo Station16
-- ============================================
SELECT 
    d.device_ps_key,
    d.device_name,
    d.device_type,
    d.type_name,
    d.grid_connection_date,
    d.dev_status,
    CASE 
        WHEN sc.device_id IS NOT NULL THEN '✅ IN_CONFIG'
        ELSE '❌ NOT_IN_CONFIG - NEEDS TO BE ADDED'
    END as config_status,
    sc.device_id as config_device_id,
    sc.sensor_type,
    sc.sensor_capacity,
    -- Check if this might be the replacement for SLI-IRR-3-F
    CASE 
        WHEN d.grid_connection_date >= '2025-10-03'::date 
            AND sc.device_id IS NULL 
            AND d.device_name LIKE '%16%' THEN '⚠️ POSSIBLE REPLACEMENT FOR SLI-IRR-3-F'
        WHEN d.grid_connection_date >= '2025-10-03'::date 
            AND sc.device_id IS NULL THEN '⚠️ NEW METEO STATION - CHECK IF REPLACES SLI-IRR-3-F'
        ELSE ''
    END as replacement_note
FROM raw.isolarcloud_devices d
LEFT JOIN staging.seed_sensor_config sc 
    ON CONCAT('ISO_', d.device_ps_key) = sc.device_id
WHERE d.ps_id = '1479456'
    AND d.device_type = 5  -- Meteo Station
ORDER BY 
    d.grid_connection_date NULLS LAST,
    d.device_name;

-- ============================================
-- Focus on IRR sensors (device_name contains IRR)
-- ============================================
SELECT 
    d.device_ps_key,
    d.device_name,
    d.device_type,
    d.type_name,
    d.grid_connection_date,
    d.dev_status,
    CASE 
        WHEN sc.device_id IS NOT NULL THEN 'IN_CONFIG'
        ELSE 'NOT_IN_CONFIG - NEEDS TO BE ADDED'
    END as config_status,
    sc.device_id as config_device_id,
    sc.sensor_type,
    sc.sensor_capacity
FROM raw.isolarcloud_devices d
LEFT JOIN staging.seed_sensor_config sc 
    ON CONCAT('ISO_', d.device_ps_key) = sc.device_id
WHERE d.ps_id = '1479456'
    AND (d.device_name LIKE '%IRR%' OR d.device_name LIKE '%irr%')
ORDER BY 
    d.grid_connection_date NULLS LAST,
    d.device_name;

-- ============================================
-- 3. SEARCH FOR METEO STATION16 OR DEVICES WITH "16" IN NAME
-- ============================================
SELECT 
    d.device_ps_key,
    d.device_name,
    d.device_type,
    d.type_name,
    d.grid_connection_date,
    d.dev_status,
    CASE 
        WHEN sc.device_id IS NOT NULL THEN '✅ IN_CONFIG'
        ELSE '❌ NOT_IN_CONFIG - NEEDS TO BE ADDED'
    END as config_status,
    sc.device_id as config_device_id,
    sc.sensor_type,
    sc.sensor_capacity
FROM raw.isolarcloud_devices d
LEFT JOIN staging.seed_sensor_config sc 
    ON CONCAT('ISO_', d.device_ps_key) = sc.device_id
WHERE d.ps_id = '1479456'
    AND (
        d.device_name LIKE '%16%' 
        OR d.device_name LIKE '%Station16%'
        OR d.device_name LIKE '%Meteo%16%'
        OR d.device_name LIKE '%station%16%'
    )
ORDER BY 
    d.grid_connection_date NULLS LAST,
    d.device_name;

-- ============================================
-- 4. DEVICES NOT IN CONFIG (PRIORITY - NEEDS ACTION)
-- ============================================
SELECT 
    d.device_ps_key,
    CONCAT('ISO_', d.device_ps_key) as suggested_device_id,
    d.device_name,
    d.device_type,
    d.type_name,
    d.grid_connection_date,
    d.dev_status,
    -- Suggest what to add to sensor_config
    CASE 
        WHEN d.device_name LIKE '%IRR%' OR d.device_name LIKE '%irr%' THEN 'POA'
        WHEN d.device_name LIKE '%PYR%' OR d.device_name LIKE '%pyr%' THEN 'GHI'
        WHEN d.type_name = 'Meteo Station' THEN 'Check device - might have POA/GHI sensor'
        ELSE 'Unknown - check device details'
    END as suggested_sensor_type,
    'Need to add capacity manually' as suggested_capacity_note
FROM raw.isolarcloud_devices d
LEFT JOIN staging.seed_sensor_config sc 
    ON CONCAT('ISO_', d.device_ps_key) = sc.device_id
WHERE d.ps_id = '1479456'
    AND sc.device_id IS NULL  -- Only devices NOT in config
ORDER BY 
    d.grid_connection_date NULLS LAST,
    d.device_name;

