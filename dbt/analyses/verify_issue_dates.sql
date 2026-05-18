-- Verify issue dates implementation
-- Check if is_issue_date column exists and has correct values

SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN is_issue_date = TRUE THEN 1 END) as issue_dates_count,
    COUNT(CASE WHEN is_issue_date = FALSE THEN 1 END) as non_issue_dates_count,
    COUNT(CASE WHEN is_issue_date IS NULL THEN 1 END) as null_count
FROM mart.mart_site_performance_daily;

-- Sample issue dates
SELECT 
    date_key,
    site_id,
    site_name,
    is_issue_date,
    daily_energy_mwh
FROM mart.mart_site_performance_daily
WHERE is_issue_date = TRUE
ORDER BY site_name, date_key
LIMIT 20;

-- Check specific sites with issue dates
SELECT 
    site_name,
    COUNT(*) as total_days,
    COUNT(CASE WHEN is_issue_date = TRUE THEN 1 END) as issue_days,
    ROUND(COUNT(CASE WHEN is_issue_date = TRUE THEN 1 END)::numeric / COUNT(*)::numeric * 100, 2) as issue_percentage
FROM mart.mart_site_performance_daily
GROUP BY site_name
HAVING COUNT(CASE WHEN is_issue_date = TRUE THEN 1 END) > 0
ORDER BY issue_days DESC;

