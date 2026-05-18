# Cleaning Log — Panduan Lengkap

Dokumen master untuk sistem log pembersihan sensor dan module: desain tabel, implementasi, langkah setelah seed, dan kolom `is_active`. Digabung dari: CLEANING_LOG_IMPLEMENTATION_GUIDE_ID, CLEANING_LOG_NEXT_STEPS, cleaning-log-is-active-explanation, cleaning-log-table-design.

---

## 1. Overview & Table Design

Tabel ini mencatat log pembersihan sensor dan module (panel PV) **per site**. Data dipakai di Power BI untuk "Days Since Last Cleaning" (DAX: `TODAY() - [TanggalPembersihan]`).

**Penting**: Cleaning log adalah **per site** (site_code / site_name dari **dim_site**). Satu site bisa punya **2 record** bila yang dibersihkan **keduanya**: satu record `asset_type = 'module'`, satu record `asset_type = 'sensor'`. **asset_id** tidak dipakai; yang penting **site_id** (site_code) dan **site_name**.

### 1.1 Database Schema

**Table**: `staging.cleaning_log` (untuk form; seed: `staging.seed_cleaning_log`)

```sql
CREATE TABLE staging.seed_cleaning_log (
    id SERIAL PRIMARY KEY,
    asset_type VARCHAR(50) NOT NULL,  -- 'Sensor' or 'Module'
    asset_id VARCHAR(100),             -- opsional
    site_id VARCHAR(100),              -- untuk FusionSolar (NE=...)
    site_name VARCHAR(200),            -- untuk MMKI dan lainnya
    cleaning_date DATE NOT NULL,
    notes TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_cleaning_log UNIQUE(asset_type, COALESCE(site_id,''), COALESCE(site_name,''), cleaning_date),
    CONSTRAINT check_site_identifier CHECK (
        (site_id IS NOT NULL AND site_id != '') OR (site_name IS NOT NULL AND site_name != '')
    )
);

CREATE INDEX idx_cleaning_log_site_id ON staging.seed_cleaning_log(asset_type, site_id) WHERE site_id IS NOT NULL;
CREATE INDEX idx_cleaning_log_site_name ON staging.seed_cleaning_log(asset_type, site_name) WHERE site_name IS NOT NULL;
CREATE INDEX idx_cleaning_log_date ON staging.seed_cleaning_log(cleaning_date);
CREATE INDEX idx_cleaning_log_active ON staging.seed_cleaning_log(is_active) WHERE is_active = TRUE;
```

### 1.2 Kolom utama

- **asset_type**: `'sensor'` atau `'module'` (satu site bisa punya 2 record: module + sensor)
- **site_id** / **site_name**: Dari **dim_site** (site_code → site_id, site_name). Salah satu wajib. **asset_id** tidak dipakai.
- **cleaning_date**: Tanggal pembersihan (YYYY-MM-DD)
- **is_active**: Flag aktif/nonaktif (soft delete). Detail di [Section 4](#4-kolom-is_active)

---

## 2. Implementation & File yang Dibuat

### 2.1 File

- **staging.cleaning_log** — Tabel untuk input form (create: `scripts/create_staging_cleaning_log_table.sql`)
- **dbt/seeds/seed_cleaning_log.csv** — Data log seed (format: asset_type;site_id;cleaning_date;notes;is_active)
- **scripts/create_cleaning_log_table.sql** — Create seed table + index
- **scripts/DAX_Cleaning_Log_Measures.txt** — Formula DAX siap pakai
- **docs/CLEANING_LOG.md** — Dokumen ini

### 2.2 Cara menggunakan

1. **Buat tabel**: `psql ... -f scripts/create_cleaning_log_table.sql`
2. **Load seed** (opsional): `dbt seed --select seed_cleaning_log`
3. **Tambah data**: pilih salah satu:
   - **Tim Engineering (disarankan)**: Langsung **INSERT** ke tabel — lihat [Section 6](#6-input-untuk-tim-engineering--direct-write-ke-tabel).
   - Atau edit CSV lalu jalankan lagi `dbt seed --select seed_cleaning_log`
4. **Power BI**: Import tabel, buat relationship (`seed_cleaning_log[site_id]` → referensi), buat measure dari DAX_Cleaning_Log_Measures.txt

### 2.3 DAX — Days Since Last Cleaning

Logika: **BENAR** menggunakan `TODAY() - [LastCleaningDate]`. Contoh (Sensor, filter `is_active = TRUE`):

```dax
Days Since Last Cleaning (Sensor) = 
VAR LastCleaningDate = 
    CALCULATE(
        MAX('seed_cleaning_log'[cleaning_date]),
        FILTER('seed_cleaning_log',
            'seed_cleaning_log'[asset_type] = "Sensor" &&
            ('seed_cleaning_log'[site_id] = RELATED('seed_sensor_config'[site_id]) OR 'seed_cleaning_log'[site_name] = RELATED('seed_site_config'[Site])) &&
            'seed_cleaning_log'[is_active] = TRUE()
        )
    )
RETURN IF(ISBLANK(LastCleaningDate), BLANK(), DATEDIFF(LastCleaningDate, TODAY(), DAY))
```

Formula lengkap Sensor/Module ada di `scripts/DAX_Cleaning_Log_Measures.txt`.

### 2.4 Relationship Power BI

- `seed_cleaning_log[site_id]` → `dim_site[site_code]` atau referensi lain (site_id)
- `seed_cleaning_log[site_name]` → `dim_site[site_name]` atau `seed_site_config[Site]` / `dim_assets[site_name]`

---

## 3. Next Steps Setelah dbt Seed

### 3.1 Verifikasi data

```sql
SELECT COUNT(*) FROM staging.seed_cleaning_log;
SELECT asset_type, COALESCE(site_id, site_name) as site_identifier, MAX(cleaning_date) as last_cleaning_date,
       CURRENT_DATE - MAX(cleaning_date) as days_since_cleaning
FROM staging.seed_cleaning_log
WHERE is_active = TRUE AND cleaning_date IS NOT NULL
GROUP BY asset_type, site_id, site_name ORDER BY days_since_cleaning DESC;
```

### 3.2 Perbaiki cleaning_date NULL

Update record yang `cleaning_date` NULL lewat SQL atau perbaiki CSV lalu `dbt seed` lagi.

### 3.3 Power BI

Refresh source → buat relationship → buat DAX measures dari DAX_Cleaning_Log_Measures.txt → visual (table + conditional formatting: hijau ≤30 hari, kuning 31–60, oranye 61–90, merah >90).

### 3.4 Troubleshooting

- **Measure BLANK**: Cek relationship, case (`sensor`/`Sensor`), `cleaning_date` tidak NULL, `is_active = TRUE`
- **Data tidak muncul**: Refresh source, cek filter visual

---

## 4. Kolom is_active

**Apa itu**: Flag boolean = record masih **aktif** atau **dinonaktifkan**.

**Kegunaan**:
- **Soft delete** — tidak hapus data, hanya tandai tidak aktif; audit trail tetap
- **Koreksi data** — set `is_active = FALSE` pada record salah, tambah record baru yang benar
- **Pembersihan batal** — set `is_active = FALSE` tanpa hapus record

**Contoh koreksi**:
```sql
UPDATE staging.seed_cleaning_log SET is_active = FALSE, updated_at = NOW() WHERE id = 123;
INSERT INTO staging.seed_cleaning_log (asset_type, site_id, cleaning_date, notes, is_active)
VALUES ('Sensor', 'NE=50488260', '2024-12-15', 'Koreksi tanggal', TRUE);
```

Semua query dan DAX memakai filter `WHERE is_active = TRUE`. Default = `TRUE`.

**Kapan pakai FALSE**: Data salah input, pembersihan dibatalkan, duplikat, data uji. **Jangan** pakai FALSE hanya karena pembersihan sudah selesai atau data lama yang valid.

---

## 4.5 Workflow status: Form → Database (cleaning log vs weekly log)

### Apakah staging.cleaning_log punya kolom status?

**Tidak.** Tabel **staging.cleaning_log** (yang dipakai form) **tidak punya kolom `status`**. Kolom yang ada: `id`, `asset_type`, `site_id`, `site_name`, `cleaning_date`, `notes`, **`is_active`**, `created_at`, `updated_at`.  
Yang mirip “status” di cleaning log hanya **`is_active`** (soft delete: aktif vs dinonaktifkan), bukan Open/Resolved seperti di weekly log.

Yang **punya kolom status** (Open, In Progress, Resolved, Closed) adalah **staging.seed_weekly_log** (weekly log), bukan cleaning log.

### Alur form → database untuk cleaning log

| Langkah | Yang terjadi |
|--------|----------------|
| 1. User isi form | Pilih site, tanggal, module/sensor, catatan |
| 2. Klik Simpan | Form **INSERT** satu atau dua baris ke `staging.cleaning_log` (satu baris per asset_type: module/sensor) |
| 3. Di database | Hanya **tambah record baru**; tidak ada “update status” karena tidak ada kolom status |
| 4. Koreksi / batal | **UPDATE** `is_active = 0` (soft delete) pada record yang salah/dibatalkan; bisa tambah record baru yang benar |

Jadi alurnya: **form hanya INSERT**, tidak buat double record “open + resolved”. Satu kejadian pembersihan = satu (atau dua) record dengan `cleaning_date` dan `is_active = 1`. Tidak ada workflow Open → Resolved di cleaning log.

### Workflow status umum (misalnya weekly log)

Kalau suatu tabel **punya kolom status** (Open, Resolved, dll), biasanya dipakai salah satu pola berikut.

**Pola 1: Satu record, update status (bukan double record)**  
- Satu baris per “item” (misalnya satu issue di weekly log).  
- Form **INSERT** sekali dengan `status = 'Open'`.  
- Nanti user **UPDATE** baris **yang sama**: `status = 'Resolved'`, `updated_at = NOW()`.  
- **Tidak** ada double record (satu Open, satu Resolved); tetap satu baris yang statusnya berubah.

**Pola 2: Double record (audit trail penuh)**  
- Satu baris saat dibuka (`status = 'Open'`), satu baris lagi saat ditutup (`status = 'Resolved'`).  
- Keduanya bisa dihubungkan dengan `issue_id` atau `parent_id`.  
- Cocok jika butuh history lengkap “kapan di-open” dan “kapan di-resolved” sebagai dua event terpisah.

Untuk **weekly log** (seed_weekly_log), pola yang wajar adalah **Pola 1**: satu record per issue, status di-update dari Open → Resolved di baris yang sama. Form bisa: (1) hanya INSERT (status awal Open), dan (2) halaman/tombol “Ubah status” yang melakukan UPDATE `status` dan `updated_at` pada record yang dipilih.

### Ringkas

| Tabel | Punya kolom status? | Alur form → DB |
|-------|--------------------|----------------|
| **staging.cleaning_log** | Tidak (hanya `is_active`) | Form **INSERT** saja; koreksi/batal pakai **UPDATE is_active** |
| **staging.seed_weekly_log** | Ya (Open, Resolved, dll) | Form **INSERT** (status awal) lalu **UPDATE** baris yang sama untuk ubah status (bukan double record open+resolved) |

---

## 5. Contoh Query SQL

**Last cleaning per site**:
```sql
SELECT asset_type, COALESCE(site_id, site_name) as site_identifier,
       MAX(cleaning_date) as last_cleaning_date,
       CURRENT_DATE - MAX(cleaning_date) as days_since_cleaning
FROM staging.seed_cleaning_log WHERE is_active = TRUE
GROUP BY asset_type, site_id, site_name ORDER BY days_since_cleaning DESC;
```

**Soft delete**:
```sql
UPDATE staging.seed_cleaning_log SET is_active = FALSE, updated_at = NOW() WHERE id = 123;
```

---

---

## 6. Input untuk Tim Engineering — Direct Write ke Tabel

Agar input log pembersihan **semudah mungkin**, tim engineering **tidak perlu** edit CSV atau jalankan dbt seed. Cukup **langsung INSERT** ke tabel `staging.seed_cleaning_log` dengan format yang sudah ada.

### 6.1 Format yang wajib

| Kolom          | Wajib | Keterangan |
|----------------|-------|------------|
| `asset_type`   | ✅    | `'sensor'` atau `'module'` (lowercase) |
| `cleaning_date`| ✅    | Tanggal pembersihan, format `YYYY-MM-DD` |
| `site_id` **atau** `site_name` | ✅ (salah satu) | Site pakai `site_id` (angka/NE=...) atau `site_name` (nama site) |
| `asset_id`     | ❌    | Opsional |
| `notes`        | ❌    | Opsional |
| `is_active`    | ❌    | Default `TRUE` |

### 6.2 Template INSERT (copy-paste, ganti nilai)

**Pakai site_id (angka atau NE=...):**
```sql
INSERT INTO staging.seed_cleaning_log (asset_type, site_id, cleaning_date, notes)
VALUES ('module', '1680199', '2025-12-01', 'Pembersihan rutin')
ON CONFLICT DO NOTHING;
```

**Pakai site_name (MMKI / nama site):**
```sql
INSERT INTO staging.seed_cleaning_log (asset_type, site_name, cleaning_date, notes)
VALUES ('module', 'PLTS Rooftop Sumatera Prima Fibreboard', '2025-12-01', 'Pembersihan rutin')
ON CONFLICT DO NOTHING;
```

**Hari ini (tanggal otomatis):**
```sql
INSERT INTO staging.seed_cleaning_log (asset_type, site_id, cleaning_date)
VALUES ('module', '1680199', CURRENT_DATE)
ON CONFLICT DO NOTHING;
```

### 6.3 Cara pakai (pilih salah satu)

| Cara | Keterangan |
|------|------------|
| **Form web (paling mudah)** | Jalankan form Streamlit → isi form → Simpan. Lihat [6.5 Form Streamlit](#65-form-streamlit-opsi-paling-mudah). |
| **SQL langsung** | DBeaver / pgAdmin / psql: paste template di atas, ganti nilai, jalankan. |
| **Script SQL** | Contoh lengkap di `scripts/insert_cleaning_log.sql`. |

Data langsung masuk ke tabel; Power BI cukup di-refresh untuk melihat "Days Since Last Cleaning" terbaru. Tidak perlu jalankan dbt seed untuk input baru.

### 6.5 Form Streamlit (opsi paling mudah)

Ada **form web** yang langsung INSERT ke tabel. **Site pakai dropdown (pilih, jangan ketik)** supaya tidak salah input.

**Jalankan:**
```bash
pip install streamlit sqlalchemy psycopg2-binary python-dotenv
streamlit run tools/cleaning_log_form.py
```
Browser terbuka di `http://localhost:8501`. **Pilih site dari dropdown** → Isi tanggal & catatan (opsional) → Simpan.

**Daftar site:** Diambil dari **dim_site** (prioritas), lalu fallback: `dim_assets` → `seed_cleaning_log` → `seed_site_config`. Cache 5 menit; tombol "Refresh daftar site" untuk muat ulang. **asset_id** tidak dipakai; yang dipakai **site_code** / **site_name**. Pilihan "Keduanya (module + sensor)" menyimpan **2 record** (satu module, satu sensor).

**Persiapan:** File `.env` di root project atau di `tools/` berisi `POSTGRES_*`. Detail di `tools/README.md`.

**Agar tim engineering lain bisa pakai:** (1) **Setiap orang run lokal**: clone/copy project, pip install, `.env`, jalankan `tools\run_cleaning_log_form.bat` (Windows) atau `./tools/run_cleaning_log_form.sh` (Linux/Mac), buka http://localhost:8501. (2) **Satu server 24 jam**: di server jalankan `tools\run_cleaning_log_form_shared.bat` (port 8502); jangan tutup jendela; buka port 8502 di firewall; device lain akses **http://IP_SERVER:8502**. Detail di `tools/README.md` → "Deploy di server (jalan 24 jam)".

### 6.4 Catatan

- `ON CONFLICT DO NOTHING` mencegah error duplikat (kombinasi asset_type + site_id/site_name + cleaning_date sudah ada).
- `site_id` dan `site_name` harus konsisten dengan referensi di Power BI (mis. sama dengan `seed_site_config` atau `dim_assets`).

---

*Dokumen master cleaning log. File asli: CLEANING_LOG_IMPLEMENTATION_GUIDE_ID, CLEANING_LOG_NEXT_STEPS, cleaning-log-is-active-explanation, cleaning-log-table-design — diarsipkan di docs/archive/.*
