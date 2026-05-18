-- =============================================================================
-- Script: Buat Role & User O&M (Operations & Maintenance) - READ ONLY
-- Database: PostgreSQL (MMSR)
-- Deskripsi: Role dan user untuk tim O&M dengan akses SELECT ke semua skema/tabel.
--            Nanti jika ada tabel yang perlu write, bisa ditambah GRANT terpisah.
-- =============================================================================
-- Tim O&M beberapa orang?
--   - Role (role_om_readonly) CUKUP SATU — dipakai bersama.
--   - User: disarankan SATU PER ORANG (mmsr_om_budi, mmsr_om_siti, ...) supaya
--     audit trail jelas dan bisa cabut akses per orang. Tambah user pakai script
--     add_om_user.sql. Opsi: satu user bersama (mmsr_om) juga bisa, tapi tidak
--     bisa bedakan siapa yang akses.
-- =============================================================================
-- Cara pakai:
--   1. Jalankan sebagai superuser / pemilik database (misalnya postgres atau juice).
--   2. Ganti password '...PASSWORD...' dengan password yang kuat.
--   3. Untuk orang kedua dst, jangan buat role lagi — pakai add_om_user.sql.
-- =============================================================================

-- 1. Buat ROLE (group) read-only untuk O&M — SATU saja, dipakai semua user O&M
CREATE ROLE role_om_readonly NOLOGIN;

-- 2. Buat USER (login) pertama untuk tim O&M
--    Ganti password sebelum jalankan! Untuk user tambahan pakai add_om_user.sql
CREATE USER mmsr_om WITH
  LOGIN
  PASSWORD '...PASSWORD...'
  IN ROLE role_om_readonly;

-- 3. Izinkan koneksi ke database MMSR
GRANT CONNECT ON DATABASE "MMSR" TO role_om_readonly;

-- 4. Untuk tiap skema: USAGE + SELECT di semua tabel/view (existing + default ke depan)
--    Sesuaikan daftar skema jika ada yang tidak dipakai.

DO $$
DECLARE
  r RECORD;
  sch TEXT;
  schemas TEXT[] := ARRAY['raw', 'staging', 'mart', 'dimensions', 'public'];
BEGIN
  FOREACH sch IN ARRAY schemas
  LOOP
    -- Skip jika skema tidak ada
    IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = sch) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO role_om_readonly', sch);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO role_om_readonly', sch);
      EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO role_om_readonly', sch);
      -- Default: tabel/sequence baru di skema ini juga dapat SELECT
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO role_om_readonly',
        sch
      );
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON SEQUENCES TO role_om_readonly',
        sch
      );
    END IF;
  END LOOP;
END
$$;

-- 5. (Opsional) Agar role_om_readonly bisa baca tabel yang dibuat oleh user lain
--    Misalnya tabel dibuat oleh juice, maka default privileges oleh juice:
--    Uncomment dan ganti 'juice' jika perlu.
/*
ALTER DEFAULT PRIVILEGES FOR ROLE juice IN SCHEMA raw   GRANT SELECT ON TABLES TO role_om_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE juice IN SCHEMA staging GRANT SELECT ON TABLES TO role_om_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE juice IN SCHEMA mart    GRANT SELECT ON TABLES TO role_om_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE juice IN SCHEMA dimensions GRANT SELECT ON TABLES TO role_om_readonly;
*/

-- =============================================================================
-- Cek hasil (jalankan setelah script di atas)
-- =============================================================================
-- \du mmsr_om
-- \du role_om_readonly
-- SELECT grantee, table_schema, table_name, privilege_type
--   FROM information_schema.table_privileges
--   WHERE grantee = 'role_om_readonly'
--   ORDER BY table_schema, table_name;

-- =============================================================================
-- Nanti jika ada tabel yang perlu WRITE untuk O&M (contoh: tabel log O&M)
-- =============================================================================
-- GRANT INSERT, UPDATE ON mart.tabel_log_om TO role_om_readonly;
-- -- atau buat role terpisah: role_om_write untuk tabel tertentu saja
