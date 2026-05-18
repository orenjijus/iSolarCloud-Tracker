-- ============================================================================
-- Quick Start: Create Materialized View untuk Satu Site
-- ============================================================================

-- Step 1: Replace placeholders dan execute
-- Example: MMKI 1 untuk tahun 2025

-- ============================================================================
-- CONFIGURATION: Ganti nilai ini sesuai kebutuhan
-- ============================================================================

\set site_name 'MMKI 1'
\set year_val 2025

-- ============================================================================
-- Generate MV name
-- ============================================================================

SELECT 
    'mv_performance_monitoring_5min_' || 
    LOWER(REPLACE(REPLACE(:'site_name', ' ', '_'), '-', '_')) || 
    '_' || :year_val::TEXT as mv_name;

-- ============================================================================
-- Step 2: Create Materialized View
-- ============================================================================
-- Copy query dari 01_create_materialized_view.sql
-- Replace {SITE_NAME} dengan :'site_name'
-- Replace {YEAR} dengan :year_val
-- Replace {YEAR+1} dengan :year_val + 1
-- Execute query

-- ============================================================================
-- Step 3: Test Query
-- ============================================================================

-- Test query performance
EXPLAIN ANALYZE
SELECT * 
FROM mart.mv_performance_monitoring_5min_mmki1_2025  -- Ganti dengan MV name yang dihasilkan
LIMIT 100;

-- ============================================================================
-- Step 4: Refresh Materialized View
-- ============================================================================

SELECT * FROM mart.refresh_mv_performance_5min_site_year(:'site_name', :year_val);

-- ============================================================================
-- Step 5: Check Status
-- ============================================================================

SELECT * FROM mart.get_mv_performance_5min_status(:year_val);

-- ============================================================================
-- Quick Reference Commands
-- ============================================================================

-- Create MV: Copy dari 01_create_materialized_view.sql dan replace placeholders
-- Refresh MV: SELECT * FROM mart.refresh_mv_performance_5min_site_year('MMKI 1', 2025);
-- Check Status: SELECT * FROM mart.get_mv_performance_5min_status(2025);
-- Test Query: SELECT * FROM mart.vw_performance_monitoring_5min_mmki1_2025 LIMIT 100;

