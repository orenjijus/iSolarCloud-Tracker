-- EDA Site PLTS: Overview (count, date range, sites, rows per site/year/month)
-- Purpose: Understand data coverage for ML. Run against mart.mart_site_performance_daily.

-- 1. Global counts and date range
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT site_id) AS num_sites,
    MIN(date_key) AS min_date,
    MAX(date_key) AS max_date,
    MAX(date_key) - MIN(date_key) + 1 AS calendar_days_span
FROM mart.mart_site_performance_daily;

-- 2. Rows per site (days of data), first/last date
SELECT
    site_id,
    site_name,
    system,
    COUNT(*) AS days,
    MIN(date_key) AS first_date,
    MAX(date_key) AS last_date
FROM mart.mart_site_performance_daily
GROUP BY site_id, site_name, system
ORDER BY days DESC, site_name;

-- 3. Rows per (site, year)
SELECT
    site_id,
    site_name,
    EXTRACT(YEAR FROM date_key)::int AS year,
    COUNT(*) AS days
FROM mart.mart_site_performance_daily
GROUP BY site_id, site_name, EXTRACT(YEAR FROM date_key)
ORDER BY site_id, year;

-- 4. Rows per (site, year-month) sample (last 3 months for brevity)
SELECT
    site_id,
    site_name,
    TO_CHAR(date_key, 'YYYY-MM') AS year_month,
    COUNT(*) AS days
FROM mart.mart_site_performance_daily
WHERE date_key >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY site_id, site_name, TO_CHAR(date_key, 'YYYY-MM')
ORDER BY year_month DESC, site_name;

-- 5. (Optional) Same as 1–2 but EXCLUDE Samator & Klinik (untuk konsistensi dengan device 5min)
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT site_id) AS num_sites,
    MIN(date_key) AS min_date,
    MAX(date_key) AS max_date
FROM mart.mart_site_performance_daily
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%';

SELECT site_id, site_name, system, COUNT(*) AS days, MIN(date_key) AS first_date, MAX(date_key) AS last_date
FROM mart.mart_site_performance_daily
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
GROUP BY site_id, site_name, system
ORDER BY days DESC, site_name;
