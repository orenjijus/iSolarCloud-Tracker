-- Run this as superuser to create schemas for dbt transformations
-- This keeps your raw data untouched in public schema

-- 1. Create separate schemas for dbt
CREATE SCHEMA IF NOT EXISTS dbt_staging;
CREATE SCHEMA IF NOT EXISTS dbt_intermediate;
CREATE SCHEMA IF NOT EXISTS dbt_dimensions;
CREATE SCHEMA IF NOT EXISTS dbt_marts;

-- 2. Grant permissions to juice user
GRANT ALL ON SCHEMA dbt_staging TO juice;
GRANT ALL ON SCHEMA dbt_intermediate TO juice;
GRANT ALL ON SCHEMA dbt_dimensions TO juice;
GRANT ALL ON SCHEMA dbt_marts TO juice;

-- 3. Allow juice to create objects in these schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_staging GRANT ALL ON TABLES TO juice;
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_staging GRANT ALL ON SEQUENCES TO juice;

ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_intermediate GRANT ALL ON TABLES TO juice;
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_intermediate GRANT ALL ON SEQUENCES TO juice;

ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_dimensions GRANT ALL ON TABLES TO juice;
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_dimensions GRANT ALL ON SEQUENCES TO juice;

ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_marts GRANT ALL ON TABLES TO juice;
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_marts GRANT ALL ON SEQUENCES TO juice;

-- 4. Verify schemas were created
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name LIKE 'dbt_%'
ORDER BY schema_name;

