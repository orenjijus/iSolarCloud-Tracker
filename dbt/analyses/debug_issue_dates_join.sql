-- Debug issue dates join
-- Check seed data
SELECT 
    site_id,
    issue_date,
    TO_DATE(issue_date, 'FMMM/FMDD/YYYY') as parsed_date,
    is_active
FROM staging.seed_issue_dates
LIMIT 10;

-- Check if dates can be parsed
SELECT 
    site_id,
    issue_date,
    TO_DATE(issue_date, 'FMMM/FMDD/YYYY') as parsed_date,
    CASE 
        WHEN TO_DATE(issue_date, 'FMMM/FMDD/YYYY') IS NULL THEN 'FAILED'
        ELSE 'OK'
    END as parse_status
FROM staging.seed_issue_dates
WHERE issue_date IS NOT NULL
    AND TRIM(issue_date) != ''
LIMIT 20;

-- Check site_id matching
SELECT DISTINCT
    da.asset_id as dim_asset_id,
    id.site_id as seed_site_id,
    da.site_name
FROM dimensions.dim_assets da
CROSS JOIN (
    SELECT DISTINCT site_id 
    FROM staging.seed_issue_dates
) id
WHERE da.asset_id = id.site_id
    AND da.asset_level = 'Site'
LIMIT 20;

-- Check actual join result
SELECT 
    sm.date_key,
    da.asset_id,
    id.site_id as issue_site_id,
    id.issue_date,
    CASE WHEN id.issue_date IS NOT NULL THEN TRUE ELSE FALSE END as is_issue_date
FROM mart.mart_site_performance_daily sm
LEFT JOIN dimensions.dim_assets da 
    ON da.site_name = sm.site_name 
    AND da.asset_level = 'Site'
LEFT JOIN (
    SELECT 
        site_id,
        TO_DATE(issue_date, 'FMMM/FMDD/YYYY') as issue_date,
        CASE 
            WHEN UPPER(TRIM(COALESCE(is_active, ''))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as is_active
    FROM staging.seed_issue_dates
    WHERE issue_date IS NOT NULL
        AND TRIM(issue_date) != ''
) id
    ON sm.date_key = id.issue_date
    AND da.asset_id = id.site_id
    AND id.is_active = TRUE
WHERE id.issue_date IS NOT NULL
LIMIT 20;

