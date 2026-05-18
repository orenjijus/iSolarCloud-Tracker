-- ============================================
-- Weekly Log Table Creation Script
-- ============================================
-- Script ini membuat tabel seed_weekly_log di database (opsional).
-- Input: log_date (tanggal saja), site, problem_identification, corrective_action, status.
-- Week digenerate di mart (mart_weekly_log) dari log_date via join ke dim_date_generated.
--
-- CATATAN: Jika menggunakan dbt seed, tabel akan otomatis dibuat saat menjalankan:
--   dbt seed --select seed_weekly_log
-- ============================================

-- Boleh lebih dari satu baris per (log_date, site_id) - satu site bisa punya beberapa masalah per hari
CREATE TABLE IF NOT EXISTS staging.seed_weekly_log (
    log_date DATE NOT NULL,
    site_id VARCHAR(100) NOT NULL,
    site_name VARCHAR(200) NOT NULL,
    problem_identification TEXT,
    corrective_action TEXT,
    status VARCHAR(50) DEFAULT 'Open',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weekly_log_log_date ON staging.seed_weekly_log(log_date);
CREATE INDEX IF NOT EXISTS idx_weekly_log_site ON staging.seed_weekly_log(site_id);
CREATE INDEX IF NOT EXISTS idx_weekly_log_status ON staging.seed_weekly_log(status);
CREATE INDEX IF NOT EXISTS idx_weekly_log_date_site ON staging.seed_weekly_log(log_date, site_id);

COMMENT ON TABLE staging.seed_weekly_log IS 'Weekly log: input log_date (tanggal saja), site, problem, corrective_action, status. Week generated in mart.';
COMMENT ON COLUMN staging.seed_weekly_log.log_date IS 'Tanggal kejadian/log (YYYY-MM-DD). Week tidak di-input; digenerate di mart.';
COMMENT ON COLUMN staging.seed_weekly_log.site_id IS 'Site identifier (matches dim_assets.asset_id)';
COMMENT ON COLUMN staging.seed_weekly_log.site_name IS 'Site name for display';
COMMENT ON COLUMN staging.seed_weekly_log.problem_identification IS 'Description of the problem (can be multi-line)';
COMMENT ON COLUMN staging.seed_weekly_log.corrective_action IS 'Description of corrective action taken or planned';
COMMENT ON COLUMN staging.seed_weekly_log.status IS 'Status: Open, In Progress, Resolved, Closed (input manual).';

-- ============================================
-- Usage Notes:
-- ============================================
-- 1. Load data using dbt seed:
--    dbt seed --select seed_weekly_log
--    dbt run --select mart_weekly_log
--
-- 2. Or insert data manually:
--    INSERT INTO staging.seed_weekly_log
--    (log_date, site_id, site_name, problem_identification, corrective_action, status)
--    VALUES
--    ('2025-12-09', 'ISO_SITE_1637095', 'CP Majalengka', 'Problem description', 'Action taken', 'Open');
--
-- 3. Query the mart table (week/month dari dim_date_generated):
--    SELECT * FROM mart.mart_weekly_log
--    WHERE log_date >= '2025-12-01'
--    ORDER BY log_date DESC, site_name;
-- ============================================
