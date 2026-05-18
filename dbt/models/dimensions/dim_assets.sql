{{ config(
    materialized='table',
    schema='dimensions',
    unique_key='asset_id'
) }}

-- Unified asset dimension combining sites and devices from both systems
WITH isolarcloud_assets AS (
    SELECT 
        CONCAT('ISO_', d.device_ps_key) as asset_id,
        'Device' as asset_level,
        d.device_name as asset_name,
        d.device_type as device_type_id,
        d.device_category,
        'isolarcloud' as system,
        s.ps_id as site_id,
        s.ps_name as site_name,
        s.ps_name_clean as site_name_clean,
        NULL as latitude,
        NULL as longitude
    FROM {{ ref('stg_isolarcloud__devices') }} d
    JOIN {{ ref('stg_isolarcloud__sites') }} s ON d.ps_id = s.ps_id
    
    UNION ALL
    
    SELECT 
        CONCAT('ISO_SITE_', s.ps_id) as asset_id,
        'Site' as asset_level,
        s.ps_name as asset_name,
        NULL as device_type_id,
        'Site' as device_category,
        'isolarcloud' as system,
        s.ps_id as site_id,
        s.ps_name as site_name,
        s.ps_name_clean as site_name_clean,
        s.latitude,
        s.longitude
    FROM {{ ref('stg_isolarcloud__sites') }} s
),

fusionsolar_assets AS (
    SELECT 
        CONCAT('FS_', d.dev_id) as asset_id,
        'Device' as asset_level,
        d.dev_name as asset_name,
        d.dev_type_id as device_type_id,
        d.device_category,
        'fusionsolar' as system,
        p.plant_code as site_id,
        p.plant_name as site_name,
        p.plant_name_clean as site_name_clean,
        NULL as latitude,
        NULL as longitude
    FROM {{ ref('stg_fusionsolar__devices') }} d
    JOIN {{ ref('stg_fusionsolar__sites') }} p ON d.plant_code = p.plant_code
    
    UNION ALL
    
    SELECT 
        CONCAT('FS_SITE_', p.plant_code) as asset_id,
        'Site' as asset_level,
        p.plant_name as asset_name,
        NULL as device_type_id,
        'Site' as device_category,
        'fusionsolar' as system,
        p.plant_code as site_id,
        p.plant_name as site_name,
        p.plant_name_clean as site_name_clean,
        p.latitude,
        p.longitude
    FROM {{ ref('stg_fusionsolar__sites') }} p
),

unified_assets AS (
    SELECT * FROM isolarcloud_assets
    UNION ALL
    SELECT * FROM fusionsolar_assets
)

SELECT 
    ua.asset_id,
    ua.asset_level,
    ua.asset_name,
    ua.device_type_id,
    ua.device_category,
    ua.system,
    ua.site_id,
    ua.site_name,
    ua.site_name_clean,
    ua.latitude,
    ua.longitude,
    -- Get capacity, tariff, and site_order from seed_site_config for Site-level assets
    -- Note: Capacity is in kW (despite user saying kWh, values are clearly kW)
    -- Using REPLACE to handle comma decimal separator
    CASE 
        WHEN ua.asset_level = 'Site' 
            AND sc.Capacity IS NOT NULL 
            AND TRIM(sc.Capacity::text) != ''
        THEN CAST(REPLACE(sc.Capacity::text, ',', '.') AS NUMERIC)
        ELSE NULL
    END as actual_capacity_kw,
    CASE 
        WHEN ua.asset_level = 'Site' 
            AND sc.Tariff IS NOT NULL 
            AND TRIM(sc.Tariff::text) != ''
        THEN CAST(REPLACE(sc.Tariff::text, ',', '.') AS NUMERIC)
        ELSE NULL
    END as tariff,
    CASE 
        WHEN ua.asset_level = 'Site' 
            AND sc.SiteOrder IS NOT NULL
        THEN sc.SiteOrder::INTEGER
        ELSE NULL
    END as site_order,
    -- Get calculation_start_date from seed_site_config for Site-level assets
    -- This date indicates when site performance calculations should start
    -- Sites with NULL calculation_start_date will be calculated for all dates
    CASE 
        WHEN ua.asset_level = 'Site' 
            AND sc.CalculationStartDate IS NOT NULL 
            AND TRIM(sc.CalculationStartDate::text) != ''
        THEN 
            CASE 
                WHEN sc.CalculationStartDate::text ~ '^\d{4}-\d{2}-\d{2}' THEN 
                    TO_DATE(sc.CalculationStartDate::text, 'YYYY-MM-DD')
                WHEN sc.CalculationStartDate::text ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN 
                    TO_DATE(sc.CalculationStartDate::text, 'FMMM/FMDD/YYYY')
                ELSE 
                    sc.CalculationStartDate::date
            END
        ELSE NULL
    END as calculation_start_date,
    -- Get total_inverters from seed_site_config for Site-level assets
    -- This is the fixed total number of inverters that should be reporting
    -- Used for accurate availability calculation (missing inverters = unavailable)
    CASE 
        WHEN ua.asset_level = 'Site' 
            AND sc.TotalInverters IS NOT NULL
        THEN sc.TotalInverters::INTEGER
        ELSE NULL
    END as total_inverters
FROM unified_assets ua
LEFT JOIN {{ ref('seed_site_config') }} sc 
    ON ua.asset_level = 'Site'
    AND (
        ua.site_name = sc.Site
        OR ua.site_name_clean = sc.Site
        OR LOWER(REPLACE(ua.site_name, ' ', '_')) = LOWER(REPLACE(sc.Site, ' ', '_'))
    )

