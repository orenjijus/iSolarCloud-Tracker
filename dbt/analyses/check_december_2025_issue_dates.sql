-- Check if December 2025 issue dates are loaded in seed table
SELECT 
    site_id,
    site_name,
    issue_date,
    is_active,
    CASE 
        WHEN issue_date::text ~ '^\d{4}-\d{2}-\d{2}' THEN 
            TO_DATE(issue_date::text, 'YYYY-MM-DD')
        WHEN issue_date::text ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN 
            TO_DATE(issue_date::text, 'FMMM/FMDD/YYYY')
        ELSE 
            issue_date::date
    END as parsed_issue_date
FROM staging.seed_issue_dates
WHERE issue_date::text LIKE '2025-12%'
    OR (issue_date::text ~ '^\d{4}-\d{2}-\d{2}' AND TO_DATE(issue_date::text, 'YYYY-MM-DD') >= '2025-12-01')
ORDER BY site_id, parsed_issue_date;

-- Check if December 2025 dates match with dim_assets
SELECT 
    id.site_id as seed_site_id,
    id.site_name as seed_site_name,
    id.issue_date,
    da.asset_id as dim_asset_id,
    da.site_name as dim_site_name,
    CASE 
        WHEN da.asset_id = id.site_id THEN 'MATCH'
        ELSE 'NO MATCH'
    END as match_status
FROM (
    SELECT 
        site_id,
        site_name,
        CASE 
            WHEN issue_date::text ~ '^\d{4}-\d{2}-\d{2}' THEN 
                TO_DATE(issue_date::text, 'YYYY-MM-DD')
            WHEN issue_date::text ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN 
                TO_DATE(issue_date::text, 'FMMM/FMDD/YYYY')
            ELSE 
                issue_date::date
        END as issue_date
    FROM staging.seed_issue_dates
    WHERE (issue_date::text LIKE '2025-12%'
        OR (issue_date::text ~ '^\d{4}-\d{2}-\d{2}' AND TO_DATE(issue_date::text, 'YYYY-MM-DD') >= '2025-12-01'))
        AND issue_date IS NOT NULL
        AND TRIM(issue_date::text) != ''
) id
LEFT JOIN dimensions.dim_assets da
    ON da.asset_id = id.site_id
    AND da.asset_level = 'Site'
ORDER BY id.site_id, id.issue_date;

-- Check if December 2025 issue dates appear in mart_site_performance_daily
SELECT 
    sm.date_key,
    sm.site_id,
    sm.site_name,
    sm.is_issue_date,
    id.issue_date as seed_issue_date,
    id.site_id as seed_site_id
FROM mart.mart_site_performance_daily sm
LEFT JOIN dimensions.dim_assets da 
    ON da.asset_id = sm.site_id
    AND da.asset_level = 'Site'
LEFT JOIN (
    SELECT 
        site_id,
        CASE 
            WHEN issue_date::text ~ '^\d{4}-\d{2}-\d{2}' THEN 
                TO_DATE(issue_date::text, 'YYYY-MM-DD')
            WHEN issue_date::text ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN 
                TO_DATE(issue_date::text, 'FMMM/FMDD/YYYY')
            ELSE 
                issue_date::date
        END as issue_date,
        CASE 
            WHEN is_active::boolean IS TRUE THEN TRUE
            WHEN UPPER(TRIM(is_active::text)) = 'TRUE' THEN TRUE
            WHEN is_active::text = '1' THEN TRUE
            ELSE FALSE
        END as is_active
    FROM staging.seed_issue_dates
    WHERE issue_date IS NOT NULL
        AND TRIM(issue_date::text) != ''
) id
    ON sm.date_key = id.issue_date
    AND da.asset_id = id.site_id
    AND id.is_active = TRUE
WHERE sm.date_key >= '2025-12-01'
    AND sm.date_key <= '2025-12-31'
ORDER BY sm.site_name, sm.date_key;
