-- EDA Site PLTS: Issue dates proportion and impact on metrics
-- Purpose: Decide whether to exclude or flag is_issue_date in ML.

-- Proportion of rows with is_issue_date = true (global and per site)
SELECT
    is_issue_date,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM mart.mart_site_performance_daily
GROUP BY is_issue_date;

-- Per site: count issue vs non-issue
SELECT
    site_id,
    site_name,
    COUNT(*) FILTER (WHERE is_issue_date = true)  AS issue_days,
    COUNT(*) FILTER (WHERE is_issue_date = false OR is_issue_date IS NULL) AS non_issue_days,
    COUNT(*) AS total_days
FROM mart.mart_site_performance_daily
GROUP BY site_id, site_name
ORDER BY issue_days DESC, site_name;

-- Compare avg energy and PR on issue vs non-issue days (global)
SELECT
    COALESCE(is_issue_date::text, 'NULL') AS is_issue_date,
    COUNT(*) AS n,
    ROUND(AVG(daily_energy_mwh)::numeric, 4) AS avg_energy_mwh,
    ROUND(AVG(pr_ghi_actual)::numeric, 4) AS avg_pr_ghi,
    ROUND(AVG(availability_percent)::numeric, 4) AS avg_availability
FROM mart.mart_site_performance_daily
GROUP BY is_issue_date;
