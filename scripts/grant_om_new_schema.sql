-- =============================================================================
-- Script: Beri role O&M akses READ ke skema baru
-- Jalankan SEKALI setiap kali ada skema baru yang ingin diakses tim O&M.
-- Tabel baru di skema ini nanti otomatis dapat SELECT (ALTER DEFAULT PRIVILEGES).
-- =============================================================================
-- Cara pakai: ganti 'reporting' dengan nama skema Anda, lalu jalankan.
-- =============================================================================

GRANT USAGE ON SCHEMA reporting TO role_om_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO role_om_readonly;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA reporting TO role_om_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA reporting GRANT SELECT ON TABLES TO role_om_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA reporting GRANT SELECT ON SEQUENCES TO role_om_readonly;

-- Jika tabel di skema ini dibuat oleh user lain (misalnya juice/dbt), jalankan juga sekali (ganti juice jika perlu):
-- ALTER DEFAULT PRIVILEGES FOR ROLE juice IN SCHEMA reporting GRANT SELECT ON TABLES TO role_om_readonly;
