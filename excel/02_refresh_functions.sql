-- ============================================================================
-- Refresh Functions: Materialized View Incremental Refresh
-- ============================================================================

-- ============================================================================
-- Function 1: Refresh single site per year
-- ============================================================================

CREATE OR REPLACE FUNCTION mart.refresh_mv_performance_5min_site_year(
    site_name_param TEXT,
    year_param INTEGER
)
RETURNS TABLE(
    refresh_status TEXT,
    rows_before BIGINT,
    rows_after BIGINT,
    refresh_time INTERVAL,
    mv_name TEXT
) AS $$
DECLARE
    mv_name TEXT;
    view_name TEXT;
    start_time TIMESTAMP;
    rows_before_count BIGINT;
    rows_after_count BIGINT;
    sql_text TEXT;
BEGIN
    -- Generate materialized view name
    mv_name := 'mv_performance_monitoring_5min_' || 
               LOWER(REPLACE(REPLACE(site_name_param, ' ', '_'), '-', '_')) || 
               '_' || year_param::TEXT;
    
    view_name := 'vw_performance_monitoring_5min_' || 
                 LOWER(REPLACE(REPLACE(site_name_param, ' ', '_'), '-', '_')) || 
                 '_' || year_param::TEXT;
    
    start_time := clock_timestamp();
    
    -- Check if materialized view exists
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_matviews 
        WHERE schemaname = 'mart' 
        AND matviewname = mv_name
    ) THEN
        RETURN QUERY SELECT 
            'ERROR: Materialized view does not exist'::TEXT,
            0::BIGINT,
            0::BIGINT,
            clock_timestamp() - start_time,
            mv_name;
        RAISE WARNING 'Materialized view does not exist: mart.%', mv_name;
        RETURN;
    END IF;
    
    -- Get row count before refresh
    sql_text := format('SELECT COUNT(*) FROM mart.%I', mv_name);
    EXECUTE sql_text INTO rows_before_count;
    
    -- Refresh CONCURRENTLY (tidak lock, bisa query sambil refresh)
    -- Note: CONCURRENTLY hanya bisa digunakan jika ada UNIQUE index
    sql_text := format('REFRESH MATERIALIZED VIEW CONCURRENTLY mart.%I', mv_name);
    EXECUTE sql_text;
    
    -- Get row count after refresh
    sql_text := format('SELECT COUNT(*) FROM mart.%I', mv_name);
    EXECUTE sql_text INTO rows_after_count;
    
    RETURN QUERY SELECT 
        'SUCCESS'::TEXT,
        rows_before_count,
        rows_after_count,
        clock_timestamp() - start_time,
        mv_name;
    
    RAISE NOTICE 'Refreshed MV: mart.% | Rows: % -> % | Time: %', 
        mv_name, rows_before_count, rows_after_count, clock_timestamp() - start_time;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Function 2: Refresh all sites for a specific year
-- ============================================================================

CREATE OR REPLACE FUNCTION mart.refresh_all_mv_performance_5min_year(year_param INTEGER)
RETURNS TABLE(
    site_name TEXT,
    refresh_status TEXT,
    rows_before BIGINT,
    rows_after BIGINT,
    refresh_time INTERVAL,
    mv_name TEXT
) AS $$
DECLARE
    site_record RECORD;
    refresh_result RECORD;
BEGIN
    FOR site_record IN 
        SELECT DISTINCT site_name 
        FROM dbt.dim_assets 
        WHERE asset_level = 'Site'
        ORDER BY site_name
    LOOP
        -- Check if MV exists for this site and year
        IF EXISTS (
            SELECT 1 
            FROM pg_matviews 
            WHERE schemaname = 'mart' 
            AND matviewname = 'mv_performance_monitoring_5min_' || 
                LOWER(REPLACE(REPLACE(site_record.site_name, ' ', '_'), '-', '_')) || 
                '_' || year_param::TEXT
        ) THEN
            RETURN QUERY
            SELECT 
                site_record.site_name,
                r.refresh_status,
                r.rows_before,
                r.rows_after,
                r.refresh_time,
                r.mv_name
            FROM mart.refresh_mv_performance_5min_site_year(
                site_record.site_name, 
                year_param
            ) r;
        ELSE
            -- Skip if MV doesn't exist
            RETURN QUERY SELECT 
                site_record.site_name,
                'SKIPPED: MV does not exist'::TEXT,
                0::BIGINT,
                0::BIGINT,
                '0 seconds'::INTERVAL,
                'mv_performance_monitoring_5min_' || 
                LOWER(REPLACE(REPLACE(site_record.site_name, ' ', '_'), '-', '_')) || 
                '_' || year_param::TEXT;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Function 3: Get refresh status for all MVs
-- ============================================================================

CREATE OR REPLACE FUNCTION mart.get_mv_performance_5min_status(year_param INTEGER DEFAULT NULL)
RETURNS TABLE(
    mv_name TEXT,
    site_name TEXT,
    year INTEGER,
    row_count BIGINT,
    size_pretty TEXT,
    size_bytes BIGINT,
    has_unique_index BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        mv.matviewname::TEXT,
        -- Extract site name from MV name
        REPLACE(
            REPLACE(
                SPLIT_PART(SPLIT_PART(mv.matviewname, '_', 5), '_', 1),
                '_', ' '
            ),
            '-', '-'
        ) as site_name,
        -- Extract year from MV name
        SPLIT_PART(mv.matviewname, '_', -1)::INTEGER as year,
        -- Get row count
        (SELECT COUNT(*) 
         FROM information_schema.tables t
         WHERE t.table_schema = 'mart'
         AND t.table_name = mv.matviewname)::BIGINT as row_count,
        -- Get size
        pg_size_pretty(pg_total_relation_size('mart.' || mv.matviewname)) as size_pretty,
        pg_total_relation_size('mart.' || mv.matviewname) as size_bytes,
        -- Check if has unique index (required for CONCURRENTLY)
        EXISTS (
            SELECT 1 
            FROM pg_indexes 
            WHERE schemaname = 'mart'
            AND tablename = mv.matviewname
            AND indexname LIKE '%unique%'
        ) as has_unique_index
    FROM pg_matviews mv
    WHERE mv.schemaname = 'mart'
        AND mv.matviewname LIKE 'mv_performance_monitoring_5min%'
        AND (year_param IS NULL OR mv.matviewname LIKE '%_' || year_param::TEXT)
    ORDER BY mv.matviewname;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Usage Examples:
-- ============================================================================

-- Refresh single site:
-- SELECT * FROM mart.refresh_mv_performance_5min_site_year('MMKI 1', 2025);

-- Refresh all sites for year 2025:
-- SELECT * FROM mart.refresh_all_mv_performance_5min_year(2025);

-- Check status:
-- SELECT * FROM mart.get_mv_performance_5min_status(2025);

