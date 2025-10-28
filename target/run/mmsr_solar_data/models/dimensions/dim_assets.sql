
  
    

  create  table "MMSR"."public"."dim_assets__dbt_tmp"
  
  
    as
  
  (
    

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
    FROM "MMSR"."public"."stg_isolarcloud__devices" d
    JOIN "MMSR"."public"."stg_isolarcloud__sites" s ON d.ps_id = s.ps_id
    
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
    FROM "MMSR"."public"."stg_isolarcloud__sites" s
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
    FROM "MMSR"."public"."stg_fusionsolar__devices" d
    JOIN "MMSR"."public"."stg_fusionsolar__sites" p ON d.plant_code = p.plant_code
    
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
    FROM "MMSR"."public"."stg_fusionsolar__sites" p
)

SELECT * FROM isolarcloud_assets
UNION ALL
SELECT * FROM fusionsolar_assets
  );
  