# Role & User O&M (Read-Only)

Panduan membuat user untuk tim **Operations & Maintenance (O&M)** dengan akses **hanya baca (SELECT)** ke database MMSR.

## Ringkasan

- **Role:** `role_om_readonly` — **satu role** dipakai bersama tim O&M (hak SELECT ke semua skema/tabel).
- **User login:** satu atau banyak user tergantung pilihan (lihat bagian "Tim Lebih dari Satu Orang" di bawah).
- **Database:** `MMSR` (PostgreSQL).
- **Skema yang diberi akses:** `raw`, `staging`, `mart`, `dimensions`, `public`.

## Tim Lebih dari Satu Orang: Satu User Bersama vs Satu User per Orang

| | Satu user dipakai bersama | Satu user per orang |
|---|---------------------------|----------------------|
| **Contoh** | Semua pakai `mmsr_om` + satu password | `mmsr_om_budi`, `mmsr_om_siti`, dll. |
| **Bisa?** | Bisa | Bisa (disarankan) |
| **Audit** | Tidak bisa bedakan siapa yang akses | Log/audit tahu siapa (user) yang query |
| **Cabut akses** | Harus ganti password → semua kena | Tinggal nonaktifkan/hapus user orang itu |
| **Cara setup** | Cukup jalankan `create_om_role_readonly.sql` sekali | Jalankan `create_om_role_readonly.sql` sekali, lalu `add_om_user.sql` per orang |

**Rekomendasi:** **satu user per orang**. Role tetap satu (`role_om_readonly`), GRANT cukup sekali ke role; tiap orang punya user sendiri yang dimasukkan ke role itu. Tambah anggota pakai script `scripts/add_om_user.sql`.

## Cara Membuat

1. **Jalankan script sebagai superuser atau pemilik database** (misalnya `postgres` atau `juice`):

   ```bash
   psql -h 10.101.4.88 -p 5432 -U postgres -d MMSR -f scripts/create_om_role_readonly.sql
   ```

2. **Edit script sebelum jalankan:**
   - Ganti `'GANTI_PASSWORD_AMAN'` dengan password yang kuat untuk user `mmsr_om`.
   - (Opsional) Ganti nama user/role jika perlu (`mmsr_om`, `role_om_readonly`).

3. **Tambah user O&M (orang ke-2, ke-3, ...):** pakai script `scripts/add_om_user.sql`. Edit nama user dan password, jalankan. User baru otomatis dapat hak yang sama (karena masuk ke `role_om_readonly`). Role dan GRANT tidak perlu diulang.

4. **Jika ada skema baru** (misalnya `reporting`), pakai script `scripts/grant_om_new_schema.sql` atau jalankan manual (lihat bagian "Skema & Tabel Baru" di bawah).

## Skema & Tabel Baru: Auto atau Harus Update?

| Situasi | Auto dapat akses? | Tindakan |
|--------|--------------------|----------|
| **Tabel baru di skema yang sudah ada** (raw, staging, mart, dimensions, public) | **Ya, auto** | Script sudah set `ALTER DEFAULT PRIVILEGES` — tabel baru di skema itu otomatis dapat SELECT oleh `role_om_readonly`. |
| **Tabel dibuat oleh user lain** (misalnya dbt/juice) | **Ya, auto** *jika* sudah pernah jalankan blok "FOR ROLE juice" di script (sekali saja). Lihat komentar di `create_om_role_readonly.sql` bagian (5). | Uncomment dan jalankan sekali: `ALTER DEFAULT PRIVILEGES FOR ROLE juice IN SCHEMA ... GRANT SELECT ON TABLES TO role_om_readonly;` |
| **Skema baru** (misalnya `reporting`) | **Tidak auto** | Jalankan sekali: `scripts/grant_om_new_schema.sql` (edit nama skema) atau perintah GRANT manual untuk skema baru itu. |

Ringkasnya: **tabel baru di skema yang sudah ada = auto**. **Skema baru = harus di-update sekali** (grant untuk skema baru + default privileges di skema itu).

## Contoh Koneksi dari Klien

- **Host:** `10.101.4.88`
- **Port:** `5432`
- **Database:** `MMSR`
- **User:** `mmsr_om`
- **Password:** (yang Anda set di script)

Contoh connection string:

```
postgresql://mmsr_om:PASSWORD@10.101.4.88:5432/MMSR
```

Contoh di **Power BI / Excel / DBeaver:** gunakan host, port, database, user, dan password di atas.

## Verifikasi

Setelah script dijalankan:

```sql
-- Cek role dan user
\du mmsr_om
\du role_om_readonly

-- Cek privilege per tabel (sample)
SELECT table_schema, table_name, privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'role_om_readonly'
ORDER BY table_schema, table_name;
```

Tes koneksi dengan user `mmsr_om`:

```bash
psql -h 10.101.4.88 -p 5432 -U mmsr_om -d MMSR -c "SELECT current_user; SELECT COUNT(*) FROM mart.mart_site_performance_daily LIMIT 1;"
```

## Nanti: Memberi Akses WRITE ke Tabel Tertentu

Jika nanti ada tabel yang perlu O&M **insert/update** (misalnya tabel log O&M):

```sql
-- Contoh: izinkan INSERT dan UPDATE di satu tabel
GRANT INSERT, UPDATE ON mart.tabel_log_om TO role_om_readonly;

-- Atau SELECT + INSERT saja
GRANT SELECT, INSERT ON mart.tabel_inspeksi_om TO role_om_readonly;
```

Tetap pakai user `mmsr_om`; tidak perlu buat user baru. Hanya tambah `GRANT` untuk tabel yang perlu write.

## Reset Password User O&M

```sql
-- Satu user
ALTER USER mmsr_om WITH PASSWORD 'password_baru_yang_kuat';

-- User lain (jika pakai satu user per orang)
ALTER USER mmsr_om_budi WITH PASSWORD 'password_baru';
```

## Cabut Akses Satu Orang (Hanya Hapus User)

Jika pakai satu user per orang dan satu orang keluar dari tim:

```sql
DROP USER mmsr_om_budi;   -- hanya user itu yang hilang akses, yang lain tidak terpengaruh
```

## Cabut Akses (Hapus Semua User & Role)

```sql
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw FROM role_om_readonly;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging FROM role_om_readonly;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA mart FROM role_om_readonly;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA dimensions FROM role_om_readonly;
REVOKE USAGE ON SCHEMA raw, staging, mart, dimensions FROM role_om_readonly;
REVOKE CONNECT ON DATABASE "MMSR" FROM role_om_readonly;
-- Hapus tiap user O&M (sesuaikan dengan daftar user Anda)
DROP USER IF EXISTS mmsr_om;
DROP USER IF EXISTS mmsr_om_budi;
-- DROP USER IF EXISTS mmsr_om_siti;  -- dst.
DROP ROLE IF EXISTS role_om_readonly;
```
