-- ============================================
-- Cleaning Log Table Creation Script
-- ============================================
-- Script untuk membuat tabel log pembersihan sensor dan module
-- Tabel ini digunakan untuk tracking tanggal pembersihan
-- dan menghitung days since last cleaning di Power BI
-- ============================================

-- Create table if not exists
CREATE TABLE IF NOT EXISTS staging.seed_cleaning_log (
    id SERIAL PRIMARY KEY,
    asset_type VARCHAR(50) NOT NULL,  -- 'Sensor' or 'Module'
    asset_id VARCHAR(100),             -- asset_id (opsional, untuk tracking per asset jika diperlukan)
    site_id VARCHAR(100),              -- site_id (untuk FusionSolar sites seperti NE=50488260)
    site_name VARCHAR(200),            -- site_name (untuk MMKI dan sites lainnya, seperti pt._mmki_1.75_mwp_-_painting_building)
    cleaning_date DATE NOT NULL,       -- Tanggal pembersihan
    notes TEXT,                        -- Catatan tambahan (opsional)
    is_active BOOLEAN DEFAULT TRUE,    -- Flag untuk data aktif
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_cleaning_log UNIQUE(asset_type, COALESCE(site_id, ''), COALESCE(site_name, ''), cleaning_date),
    CONSTRAINT check_site_identifier CHECK (
        (site_id IS NOT NULL AND site_id != '') OR 
        (site_name IS NOT NULL AND site_name != '')
    )
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_cleaning_log_site_id 
    ON staging.seed_cleaning_log(asset_type, site_id) 
    WHERE site_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cleaning_log_site_name 
    ON staging.seed_cleaning_log(asset_type, site_name) 
    WHERE site_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cleaning_log_asset_id 
    ON staging.seed_cleaning_log(asset_id) 
    WHERE asset_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cleaning_log_date 
    ON staging.seed_cleaning_log(cleaning_date);

CREATE INDEX IF NOT EXISTS idx_cleaning_log_active 
    ON staging.seed_cleaning_log(is_active) 
    WHERE is_active = TRUE;

-- Add comments
COMMENT ON TABLE staging.seed_cleaning_log IS 
    'Log pembersihan sensor dan module. Digunakan untuk menghitung days since last cleaning di Power BI.';

COMMENT ON COLUMN staging.seed_cleaning_log.asset_type IS 
    'Tipe asset: Sensor atau Module';

COMMENT ON COLUMN staging.seed_cleaning_log.asset_id IS 
    'Asset ID: Identifier asset (opsional, untuk tracking per asset jika diperlukan)';

COMMENT ON COLUMN staging.seed_cleaning_log.site_id IS 
    'Site ID: Untuk FusionSolar sites (contoh: NE=50488260). Harus diisi jika site_name kosong.';

COMMENT ON COLUMN staging.seed_cleaning_log.site_name IS 
    'Site Name: Untuk MMKI dan sites lainnya (contoh: pt._mmki_1.75_mwp_-_painting_building). Harus diisi jika site_id kosong.';

COMMENT ON COLUMN staging.seed_cleaning_log.cleaning_date IS 
    'Tanggal pembersihan dilakukan';

COMMENT ON COLUMN staging.seed_cleaning_log.notes IS 
    'Catatan tambahan tentang pembersihan';

COMMENT ON COLUMN staging.seed_cleaning_log.is_active IS 
    'Flag untuk menandai apakah record ini masih aktif';

-- Verify table creation
SELECT 
    'Table created successfully' as status,
    COUNT(*) as existing_rows
FROM staging.seed_cleaning_log;

