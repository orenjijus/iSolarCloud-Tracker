-- Fix column type for seed_meter_site_mapping
-- Ensure device_id and logical_device_id are VARCHAR/TEXT, not INTEGER
-- This prevents "integer value out of range" errors when loading seed

-- First, check if table exists and current column types
SELECT 
    column_name, 
    data_type,
    character_maximum_length
FROM information_schema.columns 
WHERE table_schema = 'staging' 
    AND table_name = 'seed_meter_site_mapping'
ORDER BY ordinal_position;

-- If table exists, alter columns to ensure correct types
-- device_id and logical_device_id should be VARCHAR/TEXT
DO $$
BEGIN
    -- Check if table exists
    IF EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'staging' 
        AND table_name = 'seed_meter_site_mapping'
    ) THEN
        -- Alter device_id to VARCHAR if it's not already
        IF EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'staging' 
            AND table_name = 'seed_meter_site_mapping'
            AND column_name = 'device_id'
            AND data_type != 'character varying'
        ) THEN
            ALTER TABLE staging.seed_meter_site_mapping 
                ALTER COLUMN device_id TYPE VARCHAR(100) USING device_id::VARCHAR;
        END IF;

        -- Alter logical_device_id to VARCHAR if it's not already
        IF EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'staging' 
            AND table_name = 'seed_meter_site_mapping'
            AND column_name = 'logical_device_id'
            AND data_type != 'character varying'
        ) THEN
            ALTER TABLE staging.seed_meter_site_mapping 
                ALTER COLUMN logical_device_id TYPE VARCHAR(100) USING logical_device_id::VARCHAR;
        END IF;

        -- Alter effective_date_start to TEXT if it's not already
        IF EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'staging' 
            AND table_name = 'seed_meter_site_mapping'
            AND column_name = 'effective_date_start'
            AND data_type != 'text'
        ) THEN
            ALTER TABLE staging.seed_meter_site_mapping 
                ALTER COLUMN effective_date_start TYPE TEXT USING effective_date_start::TEXT;
        END IF;

        -- Alter effective_date_end to TEXT if it's not already  
        IF EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'staging' 
            AND table_name = 'seed_meter_site_mapping'
            AND column_name = 'effective_date_end'
            AND data_type != 'text'
        ) THEN
            ALTER TABLE staging.seed_meter_site_mapping 
                ALTER COLUMN effective_date_end TYPE TEXT USING effective_date_end::TEXT;
        END IF;
    END IF;
END $$;

-- Verify the changes
SELECT 
    column_name, 
    data_type,
    character_maximum_length
FROM information_schema.columns 
WHERE table_schema = 'staging' 
    AND table_name = 'seed_meter_site_mapping'
ORDER BY ordinal_position;

