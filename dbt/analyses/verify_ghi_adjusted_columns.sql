-- Quick verification: Check if ghi_actual and ghi_adjusted columns exist
-- This query will fail if columns don't exist, helping us verify the model was built correctly

SELECT 
    date_key,
    site_name,
    daily_ghi_kwh_m2,
    ghi_actual,
    ghi_adjusted,
    EXTRACT(MONTH FROM date_key) as month,
    CASE 
        WHEN (site_name LIKE '%MMKI%Phase 2%' OR site_name LIKE '%MMKI%5.7%' 
              OR site_name LIKE '%MMKI%Phase 3%' OR site_name LIKE '%MMKI%4.292%')
            AND EXTRACT(MONTH FROM date_key) = 11
        THEN 'MMKI Group - November (should use FLN)'
        WHEN (site_name LIKE '%MMKI%Phase 2%' OR site_name LIKE '%MMKI%5.7%' 
              OR site_name LIKE '%MMKI%Phase 3%' OR site_name LIKE '%MMKI%4.292%')
            AND EXTRACT(MONTH FROM date_key) = 12
        THEN 'MMKI Group - December (should use WTST)'
        ELSE 'Other sites (should use ghi_actual)'
    END as site_category
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE date_key >= '2024-11-01'::date
    AND date_key <= '2024-12-31'::date
    AND (
        (site_name LIKE '%MMKI%Phase 2%' OR site_name LIKE '%MMKI%5.7%' 
         OR site_name LIKE '%MMKI%Phase 3%' OR site_name LIKE '%MMKI%4.292%')
        OR site_name IS NOT NULL  -- Include some non-MMKI sites for comparison
    )
ORDER BY site_name, date_key DESC
LIMIT 50;
