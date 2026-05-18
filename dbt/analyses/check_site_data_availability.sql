-- Check when sites start appearing in database vs Excel
-- This helps understand if sites are new or just missing from Excel
-- 
-- Usage: Run this to see data availability timeline

WITH db_first_last AS (
    SELECT 
        site_id,
        site_name,
        MIN(date_key) as first_date_in_db,
        MAX(date_key) as last_date_in_db,
        MIN(year * 100 + month) as first_month_in_db,
        MAX(year * 100 + month) as last_month_in_db,
        COUNT(DISTINCT date_key) as total_days_in_db
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE year >= 2024
    GROUP BY site_id, site_name
),

excel_first_last AS (
    SELECT 
        site_id,
        site_name,
        MIN(year * 100 + month) as first_month_in_excel,
        MAX(year * 100 + month) as last_month_in_excel,
        COUNT(DISTINCT year * 100 + month) as total_months_in_excel
    FROM "MMSR"."public"."site_monthly_performance_excel"
    WHERE year >= 2024
    GROUP BY site_id, site_name
),

monthly_db AS (
    SELECT 
        site_id,
        year * 100 + month as year_month,
        COUNT(*) as days_in_month
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE year >= 2024
    GROUP BY site_id, year, month
)

SELECT 
    COALESCE(db.site_id, excel.site_id) as site_id,
    COALESCE(db.site_name, excel.site_name) as site_name,
    
    -- Database info
    db.first_date_in_db,
    db.last_date_in_db,
    db.first_month_in_db,
    db.last_month_in_db,
    db.total_days_in_db,
    
    -- Excel info
    excel.first_month_in_excel,
    excel.last_month_in_excel,
    excel.total_months_in_excel,
    
    -- Comparison
    CASE 
        WHEN db.site_id IS NULL THEN 'ONLY_IN_EXCEL'
        WHEN excel.site_id IS NULL THEN 'ONLY_IN_DB'
        WHEN db.first_month_in_db < excel.first_month_in_excel THEN 'DB_STARTS_EARLIER'
        WHEN db.first_month_in_db > excel.first_month_in_excel THEN 'EXCEL_STARTS_EARLIER'
        ELSE 'SAME_START'
    END as availability_status,
    
    -- Months missing in Excel (for sites in DB)
    (
        SELECT STRING_AGG(
            TO_CHAR(TO_DATE(year_month::text, 'YYYYMM'), 'YYYY-MM'),
            ', '
            ORDER BY year_month
        )
        FROM monthly_db md
        WHERE md.site_id = COALESCE(db.site_id, excel.site_id)
            AND md.year_month >= 202501
            AND md.year_month <= 202511
            AND NOT EXISTS (
                SELECT 1 
                FROM "MMSR"."public"."site_monthly_performance_excel" e
                WHERE e.site_id = md.site_id
                    AND e.year * 100 + e.month = md.year_month
            )
    ) as months_missing_in_excel_2025

FROM db_first_last db
FULL OUTER JOIN excel_first_last excel
    ON db.site_id = excel.site_id

WHERE 
    -- Focus on problematic sites
    COALESCE(db.site_id, excel.site_id) IN (
        'FS_SITE_NE=58630782',  -- PT. MMKI 4.292 MWP - Phase 3
        'ISO_SITE_1680199',     -- PLTS Rooftop Sumatera Prima Fibreboard
        'ISO_SITE_1614122',     -- Charoen Pokphand Majalengka
        'ISO_SITE_1628909',     -- PLTS Frina Lestari Nusantara
        'ISO_SITE_1637095',     -- Charoen Pokphand Bandung
        'ISO_SITE_1637816',     -- Charoen Pokphand Madiun
        'FS_SITE_NE=51758766'   -- PT. MMKI 5.7 MWp - Phase 2
    )

ORDER BY 
    COALESCE(db.site_id, excel.site_id)

