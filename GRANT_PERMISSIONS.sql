-- Run these commands as database superuser (postgres user)
-- Connect to database as postgres superuser and run:

-- Option 1: Grant all permissions to juice user
GRANT ALL PRIVILEGES ON DATABASE MMSR TO juice;
GRANT ALL ON SCHEMA public TO juice;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO juice;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO juice;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO juice;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO juice;

-- Option 2: Or create a separate schema for dbt transformations
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS dimensions;

GRANT ALL ON SCHEMA staging TO juice;
GRANT ALL ON SCHEMA marts TO juice;
GRANT ALL ON SCHEMA dimensions TO juice;

-- Then update dbt_project.yml to use these schemas

