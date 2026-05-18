-- ============================================================================
-- Setup Auto-Refresh Schedule
-- ============================================================================

-- ============================================================================
-- Option 1: Using pg_cron extension (if available)
-- ============================================================================

-- Check if pg_cron is installed
SELECT * FROM pg_extension WHERE extname = 'pg_cron';

-- If pg_cron is available, schedule auto-refresh
-- Refresh setiap hari jam 02:00 (setelah daily ingestion selesai)

-- Schedule untuk tahun 2025
SELECT cron.schedule(
    'refresh-mv-performance-5min-2025',
    '0 2 * * *',  -- Setiap hari jam 02:00
    $$SELECT mart.refresh_all_mv_performance_5min_year(2025)$$
);

-- Schedule untuk tahun 2026 (jika sudah ada)
SELECT cron.schedule(
    'refresh-mv-performance-5min-2026',
    '0 2 * * *',  -- Setiap hari jam 02:00
    $$SELECT mart.refresh_all_mv_performance_5min_year(2026)$$
);

-- List all scheduled jobs
SELECT * FROM cron.job;

-- ============================================================================
-- Option 2: Manual call from Python scheduler or cron job
-- ============================================================================

-- Add this to your daily ingestion pipeline script (Python):
-- 
-- import psycopg2
-- conn = psycopg2.connect("postgresql://user:pass@host:port/dbname")
-- cur = conn.cursor()
-- cur.execute("SELECT * FROM mart.refresh_all_mv_performance_5min_year(2025)")
-- results = cur.fetchall()
-- conn.commit()
-- conn.close()

-- ============================================================================
-- Option 3: PostgreSQL Event Trigger (alternative)
-- ============================================================================

-- Create function to refresh after data insertion
CREATE OR REPLACE FUNCTION mart.trigger_refresh_mv_performance_5min()
RETURNS TRIGGER AS $$
DECLARE
    site_name_val TEXT;
    year_val INTEGER;
BEGIN
    -- Extract site name and year from inserted data
    -- This is a simplified example - adjust based on your table structure
    site_name_val := NEW.site_name;  -- Adjust based on your table
    year_val := EXTRACT(YEAR FROM NEW.timestamp);
    
    -- Refresh materialized view for this site and year
    PERFORM mart.refresh_mv_performance_5min_site_year(site_name_val, year_val);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Note: Event triggers on materialized views are complex
-- Better to use scheduled refresh instead

-- ============================================================================
-- Option 4: Windows Task Scheduler / Linux Cron
-- ============================================================================

-- Create a batch/shell script to call refresh function
-- 
-- Windows (refresh_mv.bat):
-- @echo off
-- psql -h 10.101.4.88 -p 5432 -U juice -d MMSR -c "SELECT * FROM mart.refresh_all_mv_performance_5min_year(2025);"
--
-- Linux (refresh_mv.sh):
-- #!/bin/bash
-- psql -h 10.101.4.88 -p 5432 -U juice -d MMSR -c "SELECT * FROM mart.refresh_all_mv_performance_5min_year(2025);"

-- Schedule with Windows Task Scheduler or Linux cron:
-- 0 2 * * * /path/to/refresh_mv.sh

-- ============================================================================
-- Recommended Approach
-- ============================================================================

-- Best practice: Add refresh call at the end of daily ingestion pipeline
-- This ensures MV is refreshed immediately after new data is loaded
-- 
-- In your Python daily pipeline script, add:
-- 
-- def refresh_materialized_views():
--     """Refresh materialized views after data ingestion"""
--     with get_db_connection() as conn:
--         with conn.cursor() as cur:
--             cur.execute("""
--                 SELECT * FROM mart.refresh_all_mv_performance_5min_year(
--                     EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER
--                 )
--             """)
--             results = cur.fetchall()
--             logger.info(f"Refreshed {len(results)} materialized views")
--             return results

