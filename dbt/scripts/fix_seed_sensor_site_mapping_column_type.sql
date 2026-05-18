-- Fix column type for effective_date_start and effective_date_end
-- These columns should be TEXT (or DATE) to handle date values and empty strings

-- First, check current column types
SELECT 
    column_name, 
    data_type,
    character_maximum_length
FROM information_schema.columns 
WHERE table_schema = 'staging' 
    AND table_name = 'seed_sensor_site_mapping'
ORDER BY ordinal_position;

-- Alter effective_date_start to TEXT if it's not already
ALTER TABLE staging.seed_sensor_site_mapping 
    ALTER COLUMN effective_date_start TYPE TEXT USING effective_date_start::TEXT;

-- Alter effective_date_end to TEXT if it's not already  
ALTER TABLE staging.seed_sensor_site_mapping 
    ALTER COLUMN effective_date_end TYPE TEXT USING effective_date_end::TEXT;

-- Verify the changes
SELECT 
    column_name, 
    data_type,
    character_maximum_length
FROM information_schema.columns 
WHERE table_schema = 'staging' 
    AND table_name = 'seed_sensor_site_mapping'
ORDER BY ordinal_position;

