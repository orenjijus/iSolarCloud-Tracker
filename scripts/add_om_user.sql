-- =============================================================================
-- Script: Tambah user O&M ke role yang sama (role_om_readonly)
-- Pakai script ini untuk orang ke-2, ke-3, ... di tim O&M.
-- Role dan GRANT sudah ada dari create_om_role_readonly.sql — tidak perlu diulang.
-- =============================================================================
-- Cara pakai:
--   1. Ganti nama user dan password di bawah (misalnya mmsr_om_budi, mmsr_om_siti).
--   2. Jalankan sebagai superuser / pemilik database.
-- =============================================================================

-- Contoh: tambah user "mmsr_om_budi" dengan password sendiri
CREATE USER mmsr_om_budi WITH
  LOGIN
  PASSWORD 'GANTI_PASSWORD_BUDI'
  IN ROLE role_om_readonly;

-- Untuk orang berikutnya, ulangi dengan nama user dan password lain, misalnya:
-- CREATE USER mmsr_om_siti WITH LOGIN PASSWORD '...' IN ROLE role_om_readonly;
-- CREATE USER mmsr_om_andi WITH LOGIN PASSWORD '...' IN ROLE role_om_readonly;
