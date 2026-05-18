{{ config(
    materialized='table',
    schema='dimensions'
) }}

-- Generate date dimension table with monthly week numbers (reset every month)
-- Week logic matches Excel: Week increments on Monday, resets to 1 on first day of new month
WITH date_spine AS (
    SELECT 
        generate_series(
            '2020-01-01'::date,
            CURRENT_DATE + INTERVAL '2 years',
            '1 day'::interval
        )::date as date_key
),

dates_with_flags AS (
    SELECT 
        date_key,
        EXTRACT(ISODOW FROM date_key) as iso_dow,  -- Monday=1, Sunday=7
        EXTRACT(MONTH FROM date_key) as curr_month,
        LAG(EXTRACT(MONTH FROM date_key), 1, 0) OVER (ORDER BY date_key) as prev_month,
        -- Flag: 1 when it's time to increment week counter
        -- Increment happens on: 1) First day of month, 2) Monday of same month
        CASE 
            WHEN LAG(date_key) OVER (ORDER BY date_key) IS NULL THEN 1  -- First row
            WHEN EXTRACT(MONTH FROM date_key) != LAG(EXTRACT(MONTH FROM date_key)) OVER (ORDER BY date_key) THEN 1  -- New month
            WHEN EXTRACT(ISODOW FROM date_key) = 1  -- Monday (always increment, even if new month)
                 AND EXTRACT(MONTH FROM date_key) = LAG(EXTRACT(MONTH FROM date_key)) OVER (ORDER BY date_key) THEN 1
            ELSE 0
        END as week_increment_flag
    FROM date_spine
),

dates_with_week AS (
    SELECT 
        date_key,
        iso_dow,
        curr_month,
        prev_month,
        week_increment_flag,
        -- Sum the flags within each month to get week number
        SUM(week_increment_flag) OVER (
            PARTITION BY DATE_TRUNC('month', date_key)
            ORDER BY date_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as week_of_month
    FROM dates_with_flags
),

dates_with_week_adjusted AS (
    SELECT 
        d.date_key,
        d.iso_dow,
        d.curr_month,
        d.week_of_month,
        -- Check if this is the last day of month
        CASE 
            WHEN d.date_key = (DATE_TRUNC('month', d.date_key) + INTERVAL '1 month' - INTERVAL '1 day')::date 
            THEN 1 
            ELSE 0 
        END as is_last_day_of_month,
        -- Count days in week 6 for this month
        COUNT(*) FILTER (WHERE d.week_of_month = 6) OVER (
            PARTITION BY DATE_TRUNC('month', d.date_key)
        ) as week6_day_count
    FROM dates_with_week d
)

SELECT 
    date_key,
    EXTRACT(YEAR FROM date_key) as year,
    EXTRACT(MONTH FROM date_key) as month,
    EXTRACT(DAY FROM date_key) as day,
    -- Adjust week: if week 6 has only 1 day (last Monday of month), merge it to week 5
    CASE 
        WHEN week_of_month = 6 AND week6_day_count = 1 THEN 5
        ELSE week_of_month
    END as week_of_month,
    iso_dow as day_of_week,
    EXTRACT(DOY FROM date_key) as day_of_year,
    TO_CHAR(date_key, 'Day') as day_name,
    TO_CHAR(date_key, 'Month') as month_name,
    DATE_TRUNC('month', date_key) as month_key,
    DATE_TRUNC('quarter', date_key) as quarter_key,
    DATE_TRUNC('year', date_key) as year_key,
    CASE 
        -- Indonesia seasons: Dry season (April-September), Wet season (October-March)
        WHEN EXTRACT(MONTH FROM date_key) BETWEEN 4 AND 9 THEN 'Dry Season (Kemarau)'
        ELSE 'Wet Season (Hujan)'
    END as season,
    CASE 
        -- Weekend: Saturday=6, Sunday=7
        WHEN iso_dow IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type
FROM dates_with_week_adjusted

