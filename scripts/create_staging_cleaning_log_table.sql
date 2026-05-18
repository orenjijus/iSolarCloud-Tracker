-- ============================================
-- Tabel staging.cleaning_log (sesuai form)
-- ============================================
-- Untuk form input cleaning log. Kolom sesuai yang di-INSERT form.
-- ============================================

CREATE TABLE IF NOT EXISTS staging.cleaning_log (
    id SERIAL PRIMARY KEY,
    asset_type VARCHAR(50) NOT NULL,
    site_id VARCHAR(100),
    site_name VARCHAR(200),
    cleaning_date DATE NOT NULL,
    notes TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT chk_cleaning_log_site CHECK (
        (site_id IS NOT NULL AND site_id != '') OR (site_name IS NOT NULL AND site_name != '')
    )
);

CREATE INDEX IF NOT EXISTS idx_cleaning_log_site_id ON staging.cleaning_log(asset_type, site_id) WHERE site_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cleaning_log_site_name ON staging.cleaning_log(asset_type, site_name) WHERE site_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cleaning_log_date ON staging.cleaning_log(cleaning_date);
CREATE INDEX IF NOT EXISTS idx_cleaning_log_active ON staging.cleaning_log(is_active) WHERE is_active = 1;

COMMENT ON TABLE staging.cleaning_log IS 'Log pembersihan sensor/module per site. Input dari form cleaning_log_form.';
