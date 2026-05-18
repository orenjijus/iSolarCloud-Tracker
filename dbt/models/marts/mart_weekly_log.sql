{{ config(
    materialized='table',
    schema='mart',
    description='Weekly log table for tracking site problems and corrective actions. Week/month/year generated from log_date using same logic as dim_date_generated.'
) }}

-- Input: log_date (tanggal saja), site, problem_identification, corrective_action, status
-- Week/month/year digenerate di mart dengan join ke dim_date_generated (algoritma sama seperti mart_site_performance_daily)
-- days_open = berapa hari sejak issue di-log (untuk prioritas: issue lama = prioritas tinggi)
-- is_open = 1 jika status Open/In Progress (untuk filter prioritas)
SELECT 
    w.log_date,
    w.site_id,
    w.site_name,
    w.problem_identification,
    w.corrective_action,
    w.status,
    w.created_at,
    w.updated_at,
    -- Berapa lama issue berjalan (hari sejak log_date) — untuk set prioritas decision
    (CURRENT_DATE - w.log_date)::INTEGER AS days_open,
    CASE WHEN UPPER(TRIM(COALESCE(w.status, ''))) IN ('OPEN', 'IN PROGRESS') THEN 1 ELSE 0 END AS is_open,
    -- Week/month/year dari dim_date_generated (algoritma sama dengan mart lain)
    dd.date_key,
    dd.year,
    dd.month,
    dd.month_name,
    dd.week_of_month,
    dd.day_of_week,
    dd.day_name,
    dd.day_type
FROM {{ ref('seed_weekly_log') }} w
LEFT JOIN {{ ref('dim_date_generated') }} dd ON dd.date_key = w.log_date
WHERE w.log_date IS NOT NULL
ORDER BY w.log_date DESC, w.site_name
