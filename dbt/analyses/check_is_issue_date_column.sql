-- Check if is_issue_date column exists and has data
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'mart'
    AND table_name = 'mart_site_performance_daily'
    AND column_name = 'is_issue_date';

-- Check data distribution
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN is_issue_date = TRUE THEN 1 END) as issue_dates_count,
    COUNT(CASE WHEN is_issue_date = FALSE THEN 1 END) as non_issue_dates_count,
    COUNT(CASE WHEN is_issue_date IS NULL THEN 1 END) as null_count
FROM mart.mart_site_performance_daily;

-- Sample rows with issue dates
SELECT 
    date_key,
    site_id,
    site_name,
    is_issue_date,
    daily_energy_mwh
FROM mart.mart_site_performance_daily
WHERE is_issue_date = TRUE
ORDER BY site_name, date_key
LIMIT 10;

