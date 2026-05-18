-- Drop seed_meter_site_mapping table if exists
-- This fixes "integer value out of range" error when reloading seed
-- The table will be recreated by dbt seed with correct column types

DROP TABLE IF EXISTS staging.seed_meter_site_mapping CASCADE;

