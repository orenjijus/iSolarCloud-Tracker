-- Contoh Implementasi: Per-System Filter untuk mart_site_performance_daily
-- File ini menunjukkan perubahan yang diperlukan untuk fix incremental filter issue

-- ============================================================================
-- FILTER 1: daily_availability CTE (Line 478) - MOST IMPORTANT
-- ============================================================================
-- Ini adalah filter yang paling penting karena ini yang menyebabkan masalah
-- fsc sudah punya system column dari fact_site_calculations_5min

-- BEFORE (Problematic):
/*
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        AND fsc.date_key >= '{{ var("reingest_start_date") }}'::date
        AND fsc.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Default incremental: only new data
        AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
{% endif %}
*/

-- AFTER (Fixed - Per-System Filter):
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: re-process same date range
        AND fsc.date_key >= '{{ var("reingest_start_date") }}'::date
        AND fsc.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Default incremental: per-system filter
        -- Each system has its own MAX(date_key) check
        AND (
            (fsc.system = 'fusionsolar' AND fsc.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            ))
            OR
            (fsc.system = 'isolarcloud' AND fsc.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            ))
        )
    {% endif %}
{% endif %}

-- ============================================================================
-- FILTER 2: meter_daily_stats CTE (Line 75)
-- ============================================================================
-- m sudah punya system column dari mart_meter_performance_5min

-- BEFORE:
/*
AND m.date_key > (SELECT MAX(date_key) FROM {{ this }})
*/

-- AFTER:
AND (
    (m.system = 'fusionsolar' AND m.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'fusionsolar'
    ))
    OR
    (m.system = 'isolarcloud' AND m.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'isolarcloud'
    ))
)

-- ============================================================================
-- FILTER 3: daily_energy CTE (Line 236)
-- ============================================================================
-- e tidak punya system column, perlu join dengan dim_assets atau pass system dari upstream

-- BEFORE:
/*
AND date_key > (SELECT MAX(date_key) FROM {{ this }})
*/

-- AFTER (Option 1: Join with dim_assets):
AND EXISTS (
    SELECT 1 
    FROM {{ ref('dim_assets') }} da 
    WHERE da.site_name = e.site_name 
        AND da.asset_level = 'Site'
        AND (
            (da.system = 'fusionsolar' AND e.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            ))
            OR
            (da.system = 'isolarcloud' AND e.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            ))
        )
)

-- AFTER (Option 2: Pass system from meter_daily_stats):
-- Modify meter_daily_stats to include system column:
-- SELECT ..., m.system, ...
-- Then in daily_energy:
AND (
    (e.system = 'fusionsolar' AND e.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'fusionsolar'
    ))
    OR
    (e.system = 'isolarcloud' AND e.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'isolarcloud'
    ))
)

-- ============================================================================
-- FILTER 4: daily_ghi CTE (Line 347)
-- ============================================================================
-- g tidak punya system column, perlu join dengan dim_assets

-- BEFORE:
/*
AND msd.date_key > (SELECT MAX(date_key) FROM {{ this }})
*/

-- AFTER:
AND EXISTS (
    SELECT 1 
    FROM {{ ref('dim_assets') }} da 
    WHERE da.site_name = g.site_name 
        AND da.asset_level = 'Site'
        AND (
            (da.system = 'fusionsolar' AND g.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            ))
            OR
            (da.system = 'isolarcloud' AND g.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            ))
        )
)

-- ============================================================================
-- FILTER 5: daily_poa_per_sensor CTE (Line 413)
-- ============================================================================
-- Similar to daily_ghi

-- BEFORE:
/*
AND date_key > (SELECT MAX(date_key) FROM {{ this }})
*/

-- AFTER:
AND EXISTS (
    SELECT 1 
    FROM {{ ref('dim_assets') }} da 
    WHERE da.site_name = p.site_name 
        AND da.asset_level = 'Site'
        AND (
            (da.system = 'fusionsolar' AND p.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            ))
            OR
            (da.system = 'isolarcloud' AND p.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            ))
        )
)

-- ============================================================================
-- FILTER 6: Final SELECT (Line 866)
-- ============================================================================
-- da.system is available in final SELECT

-- BEFORE:
/*
AND sm.date_key > (SELECT MAX(date_key) FROM {{ this }})
*/

-- AFTER:
AND (
    (da.system = 'fusionsolar' AND sm.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'fusionsolar'
    ))
    OR
    (da.system = 'isolarcloud' AND sm.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'isolarcloud'
    ))
)

-- ============================================================================
-- ALTERNATIVE: Dynamic Approach (More Flexible)
-- ============================================================================
-- Jika ingin lebih fleksibel untuk sistem baru tanpa update filter:

-- For CTEs with system column:
AND fsc.date_key > (
    SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
    FROM {{ this }} 
    WHERE system = fsc.system
)

-- For CTEs without system column (need to get system first):
AND EXISTS (
    SELECT 1 
    FROM {{ ref('dim_assets') }} da 
    WHERE da.site_name = e.site_name 
        AND da.asset_level = 'Site'
        AND e.date_key > (
            SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
            FROM {{ this }} 
            WHERE system = da.system
        )
)

-- ============================================================================
-- PERFORMANCE OPTIMIZATION: Index Recommendation
-- ============================================================================
-- Untuk meningkatkan performance subquery, pastikan ada index:

CREATE INDEX IF NOT EXISTS idx_mart_site_performance_daily_system_date 
ON mart.mart_site_performance_daily (system, date_key);

-- Index ini akan membuat subquery MAX(date_key) WHERE system = 'xxx' sangat cepat

-- ============================================================================
-- TESTING QUERIES
-- ============================================================================

-- Test 1: Check current MAX(date_key) per system
SELECT 
    system,
    MAX(date_key) as max_date_key,
    COUNT(*) as total_rows
FROM mart.mart_site_performance_daily
GROUP BY system;

-- Test 2: Verify filter works correctly
-- Simulate filter logic:
SELECT 
    system,
    date_key,
    COUNT(*) as row_count
FROM mart.mart_site_performance_daily
WHERE (
    (system = 'fusionsolar' AND date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM mart.mart_site_performance_daily 
        WHERE system = 'fusionsolar'
    ))
    OR
    (system = 'isolarcloud' AND date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM mart.mart_site_performance_daily 
        WHERE system = 'isolarcloud'
    ))
)
GROUP BY system, date_key
ORDER BY system, date_key;

