-- ============================================
-- Insert Cleaning Log (untuk Tim Engineering)
-- ============================================
-- Langsung INSERT ke staging.seed_cleaning_log.
-- Tidak perlu edit CSV atau jalankan dbt seed.
-- ============================================

-- Format kolom: asset_type, asset_id (opsional), site_id (opsional), site_name (opsional), cleaning_date, notes (opsional), is_active (default TRUE)
-- Wajib: asset_type + cleaning_date + (site_id ATAU site_name)

-- Contoh 1: Site pakai site_id (iSolarCloud / FusionSolar)
INSERT INTO staging.seed_cleaning_log (asset_type, site_id, cleaning_date, notes)
VALUES ('module', '1680199', CURRENT_DATE, 'Pembersihan rutin')
ON CONFLICT ON CONSTRAINT unique_cleaning_log DO NOTHING;

-- Contoh 2: Site pakai site_name (MMKI atau nama site)
INSERT INTO staging.seed_cleaning_log (asset_type, site_name, cleaning_date, notes)
VALUES ('module', 'PLTS Rooftop Sumatera Prima Fibreboard', CURRENT_DATE, 'Pembersihan rutin')
ON CONFLICT ON CONSTRAINT unique_cleaning_log DO NOTHING;

-- Contoh 3: Dengan asset_id (jika dipakai)
INSERT INTO staging.seed_cleaning_log (asset_type, asset_id, site_id, site_name, cleaning_date, notes)
VALUES ('module', 'ISO_SITE_1680199', '1680199', 'PLTS Rooftop Sumatera Prima Fibreboard', '2025-12-01', 'Pembersihan rutin')
ON CONFLICT ON CONSTRAINT unique_cleaning_log DO NOTHING;

-- Contoh 4: Sensor
INSERT INTO staging.seed_cleaning_log (asset_type, site_id, cleaning_date)
VALUES ('sensor', '1680199', CURRENT_DATE)
ON CONFLICT ON CONSTRAINT unique_cleaning_log DO NOTHING;

-- ========== TEMPLATE COPY-PASTE (ganti nilai) ==========
-- INSERT INTO staging.seed_cleaning_log (asset_type, site_id, site_name, cleaning_date, notes)
-- VALUES ('module', NULL, 'NAMA_SITE_DISINI', 'YYYY-MM-DD', 'catatan opsional')
-- ON CONFLICT ON CONSTRAINT unique_cleaning_log DO NOTHING;
--
-- Jika pakai site_id (angka/NE=...): isi site_id, site_name bisa NULL.
-- Jika pakai site_name (MMKI/dll): isi site_name, site_id bisa NULL.
-- ============================================
