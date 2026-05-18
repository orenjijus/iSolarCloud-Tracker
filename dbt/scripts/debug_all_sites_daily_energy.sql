-- Comprehensive diagnostic query to verify daily energy calculations for all sites
-- Checks for accumulation issues, negative values, and data quality problems

-- 1. Check for sites with suspiciously high daily energy (possible accumulation)
WITH daily_energy_stats AS (
    SELECT 
        site_name,
        date_key,
        daily_energy_kwh,
        actual_capacity_kw,
        -- Calculate expected max daily energy (capacity * 24 hours * 0.8 efficiency = rough max)
        actual_capacity_kw * 24 * 0.8 as expected_max_daily_kwh,
        -- Flag if daily energy is suspiciously high (> 2x expected max)
        CASE 
            WHEN daily_energy_kwh > (actual_capacity_kw * 24 * 0.8 * 2) THEN 'SUSPICIOUS_HIGH'
            WHEN daily_energy_kwh < 0 THEN 'NEGATIVE'
            WHEN daily_energy_kwh = 0 THEN 'ZERO'
            ELSE 'OK'
        END as data_quality_flag
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE daily_energy_kwh IS NOT NULL
        AND actual_capacity_kw IS NOT NULL
        AND actual_capacity_kw > 0
),

site_summary AS (
    SELECT 
        site_name,
        COUNT(*) as total_days,
        COUNT(CASE WHEN data_quality_flag = 'SUSPICIOUS_HIGH' THEN 1 END) as suspicious_high_count,
        COUNT(CASE WHEN data_quality_flag = 'NEGATIVE' THEN 1 END) as negative_count,
        COUNT(CASE WHEN data_quality_flag = 'ZERO' THEN 1 END) as zero_count,
        COUNT(CASE WHEN data_quality_flag = 'OK' THEN 1 END) as ok_count,
        AVG(daily_energy_kwh) as avg_daily_energy,
        MAX(daily_energy_kwh) as max_daily_energy,
        MIN(daily_energy_kwh) as min_daily_energy,
        STDDEV(daily_energy_kwh) as stddev_daily_energy
    FROM daily_energy_stats
    GROUP BY site_name
)

-- Show sites with potential issues
SELECT 
    '=== SITES WITH POTENTIAL ISSUES ===' as analysis_type,
    site_name,
    total_days,
    suspicious_high_count,
    negative_count,
    zero_count,
    ok_count,
    ROUND(avg_daily_energy::numeric, 2) as avg_daily_energy_kwh,
    ROUND(max_daily_energy::numeric, 2) as max_daily_energy_kwh,
    ROUND(min_daily_energy::numeric, 2) as min_daily_energy_kwh,
    ROUND(stddev_daily_energy::numeric, 2) as stddev_kwh,
    CASE 
        WHEN suspicious_high_count > 0 THEN '⚠️ SUSPICIOUS HIGH VALUES'
        WHEN negative_count > 0 THEN '⚠️ NEGATIVE VALUES'
        WHEN zero_count > total_days * 0.5 THEN '⚠️ TOO MANY ZEROS'
        ELSE '✓ OK'
    END as status
FROM site_summary
ORDER BY 
    CASE 
        WHEN suspicious_high_count > 0 THEN 1
        WHEN negative_count > 0 THEN 2
        WHEN zero_count > total_days * 0.5 THEN 3
        ELSE 4
    END,
    suspicious_high_count DESC,
    site_name;

-- 2. Check for accumulation pattern (daily energy increasing over time)
WITH daily_energy_trends AS (
    SELECT 
        site_name,
        date_key,
        daily_energy_kwh,
        LAG(daily_energy_kwh) OVER (PARTITION BY site_name ORDER BY date_key) as prev_day_energy,
        daily_energy_kwh - LAG(daily_energy_kwh) OVER (PARTITION BY site_name ORDER BY date_key) as day_over_day_change,
        -- Check if values are consistently increasing (accumulation pattern)
        CASE 
            WHEN daily_energy_kwh > LAG(daily_energy_kwh) OVER (PARTITION BY site_name ORDER BY date_key) * 1.1 
                AND LAG(daily_energy_kwh) OVER (PARTITION BY site_name ORDER BY date_key) > 0
            THEN 1
            ELSE 0
        END as is_increasing
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE daily_energy_kwh IS NOT NULL
        AND daily_energy_kwh > 0
),

accumulation_check AS (
    SELECT 
        site_name,
        COUNT(*) as total_days,
        SUM(is_increasing) as increasing_days,
        ROUND(100.0 * SUM(is_increasing) / COUNT(*)::numeric, 2) as pct_increasing,
        AVG(ABS(day_over_day_change)) as avg_day_over_day_change,
        MAX(ABS(day_over_day_change)) as max_day_over_day_change
    FROM daily_energy_trends
    WHERE prev_day_energy IS NOT NULL
    GROUP BY site_name
)

SELECT 
    '=== ACCUMULATION PATTERN CHECK ===' as analysis_type,
    site_name,
    total_days,
    increasing_days,
    pct_increasing,
    ROUND(avg_day_over_day_change::numeric, 2) as avg_day_over_day_change_kwh,
    ROUND(max_day_over_day_change::numeric, 2) as max_day_over_day_change_kwh,
    CASE 
        WHEN pct_increasing > 70 AND avg_day_over_day_change > 1000 THEN '⚠️ POSSIBLE ACCUMULATION'
        WHEN pct_increasing > 50 THEN '⚠️ CHECK NEEDED'
        ELSE '✓ OK'
    END as status
FROM accumulation_check
ORDER BY 
    CASE 
        WHEN pct_increasing > 70 AND avg_day_over_day_change > 1000 THEN 1
        WHEN pct_increasing > 50 THEN 2
        ELSE 3
    END,
    pct_increasing DESC;

-- 3. Show recent daily energy values for all sites (last 5 days)
SELECT 
    '=== RECENT DAILY ENERGY VALUES (LAST 5 DAYS) ===' as analysis_type,
    site_name,
    date_key,
    ROUND(daily_energy_kwh::numeric, 2) as daily_energy_kwh,
    actual_capacity_kw,
    ROUND((daily_energy_kwh / NULLIF(actual_capacity_kw, 0))::numeric, 2) as daily_capacity_factor,
    CASE 
        WHEN daily_energy_kwh > (actual_capacity_kw * 24 * 0.8 * 2) THEN '⚠️ TOO HIGH'
        WHEN daily_energy_kwh < 0 THEN '⚠️ NEGATIVE'
        WHEN daily_energy_kwh = 0 THEN '⚠️ ZERO'
        ELSE '✓ OK'
    END as status
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE date_key >= (SELECT MAX(date_key) - INTERVAL '5 days' FROM "MMSR"."mart"."mart_site_performance_daily")
    AND daily_energy_kwh IS NOT NULL
ORDER BY date_key DESC, site_name
LIMIT 100;

-- 4. Check Pusan Manis specifically to verify the fix
SELECT 
    '=== PUSAN MANIS VERIFICATION ===' as analysis_type,
    date_key,
    ROUND(daily_energy_kwh::numeric, 2) as daily_energy_kwh,
    LAG(ROUND(daily_energy_kwh::numeric, 2)) OVER (ORDER BY date_key) as prev_day_energy,
    ROUND((daily_energy_kwh - LAG(daily_energy_kwh) OVER (ORDER BY date_key))::numeric, 2) as day_over_day_change,
    actual_capacity_kw,
    ROUND((daily_energy_kwh / NULLIF(actual_capacity_kw, 0))::numeric, 2) as daily_capacity_factor,
    CASE 
        WHEN daily_energy_kwh > 50000 THEN '⚠️ STILL TOO HIGH'
        WHEN daily_energy_kwh < 0 THEN '⚠️ NEGATIVE'
        WHEN ABS(daily_energy_kwh - LAG(daily_energy_kwh) OVER (ORDER BY date_key)) > 20000 THEN '⚠️ LARGE CHANGE'
        ELSE '✓ OK'
    END as status
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name ILIKE '%Pusan Manis%'
ORDER BY date_key DESC
LIMIT 30;

