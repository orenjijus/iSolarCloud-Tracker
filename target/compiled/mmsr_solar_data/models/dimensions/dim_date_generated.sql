

-- Generate date dimension table
WITH date_spine AS (
    SELECT 
        generate_series(
            '2020-01-01'::date,
            CURRENT_DATE + INTERVAL '2 years',
            '1 day'::interval
        )::date as date_key
)

SELECT 
    date_key,
    EXTRACT(YEAR FROM date_key) as year,
    EXTRACT(MONTH FROM date_key) as month,
    EXTRACT(DAY FROM date_key) as day,
    EXTRACT(DOW FROM date_key) as day_of_week,
    EXTRACT(DOY FROM date_key) as day_of_year,
    TO_CHAR(date_key, 'Day') as day_name,
    TO_CHAR(date_key, 'Month') as month_name,
    DATE_TRUNC('month', date_key) as month_key,
    DATE_TRUNC('quarter', date_key) as quarter_key,
    DATE_TRUNC('year', date_key) as year_key,
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date_key) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date_key) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM date_key) IN (9, 10, 11) THEN 'Fall'
    END as season,
    CASE 
        WHEN EXTRACT(DOW FROM date_key) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type
FROM date_spine