# Cleaning Log Table Design

## Overview

Tabel ini digunakan untuk mencatat log pembersihan sensor dan module (panel PV). Data ini kemudian digunakan di Power BI untuk menghitung "Days Since Last Cleaning" menggunakan formula DAX.

## Database Schema

### Table: `staging.seed_cleaning_log`

```sql
CREATE TABLE staging.seed_cleaning_log (
    id SERIAL PRIMARY KEY,
    asset_type VARCHAR(50) NOT NULL,  -- 'Sensor' or 'Module'
    asset_id VARCHAR(100),             -- asset_id (opsional, untuk tracking per asset jika diperlukan)
    site_id VARCHAR(100),              -- site_id (untuk FusionSolar sites seperti NE=50488260)
    site_name VARCHAR(200),            -- site_name (untuk MMKI dan sites lainnya)
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

-- Index untuk performa query
CREATE INDEX idx_cleaning_log_site_id ON staging.seed_cleaning_log(asset_type, site_id) WHERE site_id IS NOT NULL;
CREATE INDEX idx_cleaning_log_site_name ON staging.seed_cleaning_log(asset_type, site_name) WHERE site_name IS NOT NULL;
CREATE INDEX idx_cleaning_log_asset_id ON staging.seed_cleaning_log(asset_id) WHERE asset_id IS NOT NULL;
CREATE INDEX idx_cleaning_log_date ON staging.seed_cleaning_log(cleaning_date);
CREATE INDEX idx_cleaning_log_active ON staging.seed_cleaning_log(is_active) WHERE is_active = TRUE;
```

## Data Model

### Kolom-kolom:

1. **asset_type**: Tipe asset yang dibersihkan
   - `'Sensor'` - Untuk semua sensor di site (GHI, POA, dll)
   - `'Module'` - Untuk semua module/panel PV di site

2. **asset_id**: Identifier asset (opsional)
   - Untuk tracking per asset individual jika diperlukan
   - Bisa dikosongkan jika cleaning dilakukan per site

3. **site_id**: Identifier site untuk FusionSolar
   - Digunakan untuk FusionSolar sites (contoh: `NE=50488260`, `NE=51758766`)
   - **PENTING**: Untuk MMKI, gunakan `site_name` bukan `site_id`
   - Harus diisi jika `site_name` kosong

4. **site_name**: Nama site untuk MMKI dan sites lainnya
   - Digunakan untuk MMKI dan sites yang tidak menggunakan `site_id`
   - Contoh: `pt._mmki_1.75_mwp_-_painting_building`, `pt._mmki_5.7_mwp_-_phase_2`
   - Harus diisi jika `site_id` kosong
   - **PENTING**: MMKI tidak menggunakan `site_id`, jadi gunakan `site_name`

5. **cleaning_date**: Tanggal pembersihan dilakukan (format: DATE)

6. **notes**: Catatan tambahan tentang pembersihan (opsional)

7. **is_active**: Flag untuk menandai apakah record ini masih aktif
   - `TRUE` (default) - Record aktif, digunakan dalam perhitungan
   - `FALSE` - Record dinonaktifkan (misalnya: data salah input, pembersihan dibatalkan)
   - **Kegunaan**: 
     * Soft delete - tidak menghapus data, hanya menonaktifkan
     * Audit trail - tetap simpan history tapi tandai sebagai tidak valid
     * Filter di query/DAX untuk hanya mengambil data yang aktif
   - **Contoh penggunaan**:
     * Jika ada kesalahan input tanggal pembersihan → set `is_active = FALSE`
     * Jika pembersihan dibatalkan → set `is_active = FALSE`
     * Semua query dan DAX formula sudah filter `WHERE is_active = TRUE`

## Power BI Integration

### DAX Formula untuk "Days Since Last Cleaning"

Logika yang Anda pikirkan **BENAR**! Formula DAX yang digunakan:

```dax
Days Since Last Cleaning = 
VAR LastCleaningDate = 
    CALCULATE(
        MAX('seed_cleaning_log'[cleaning_date]),
        FILTER(
            'seed_cleaning_log',
            'seed_cleaning_log'[asset_type] = "Sensor" &&  -- atau "Module"
            (
                ('seed_cleaning_log'[site_id] = RELATED('seed_sensor_config'[site_id]) && RELATED('seed_sensor_config'[site_id]) IS NOT NULL) ||
                ('seed_cleaning_log'[site_name] = RELATED('seed_site_config'[Site]) && RELATED('seed_site_config'[Site]) IS NOT NULL)
            ) &&
            'seed_cleaning_log'[is_active] = TRUE()
        )
    )
RETURN
    IF(
        ISBLANK(LastCleaningDate),
        BLANK(),
        DATEDIFF(LastCleaningDate, TODAY(), DAY)
    )
```

### Versi Lebih Sederhana (jika sudah ada relationship):

```dax
Days Since Last Cleaning = 
VAR LastCleaningDate = 
    MAX('seed_cleaning_log'[cleaning_date])
RETURN
    IF(
        ISBLANK(LastCleaningDate),
        BLANK(),
        TODAY() - LastCleaningDate
    )
```

### Untuk Tabel dengan Sensor dan Module Terpisah:

Jika Anda ingin membuat kolom terpisah untuk Sensor dan Module:

**Sensor (dengan site_id atau site_name):**
```dax
Days Since Last Cleaning (Sensor) = 
VAR LastCleaningDate = 
    CALCULATE(
        MAX('seed_cleaning_log'[cleaning_date]),
        FILTER(
            'seed_cleaning_log',
            'seed_cleaning_log'[asset_type] = "Sensor" &&
            (
                ('seed_cleaning_log'[site_id] = RELATED('seed_sensor_config'[site_id]) && RELATED('seed_sensor_config'[site_id]) IS NOT NULL) ||
                ('seed_cleaning_log'[site_name] = RELATED('seed_site_config'[Site]) && RELATED('seed_site_config'[Site]) IS NOT NULL)
            ) &&
            'seed_cleaning_log'[is_active] = TRUE()
        )
    )
RETURN
    IF(
        ISBLANK(LastCleaningDate),
        BLANK(),
        DATEDIFF(LastCleaningDate, TODAY(), DAY)
    )
```

**Module (dengan site_id atau site_name):**
```dax
Days Since Last Cleaning (Module) = 
VAR LastCleaningDate = 
    CALCULATE(
        MAX('seed_cleaning_log'[cleaning_date]),
        FILTER(
            'seed_cleaning_log',
            'seed_cleaning_log'[asset_type] = "Module" &&
            (
                ('seed_cleaning_log'[site_id] = RELATED('dim_assets'[site_id]) && RELATED('dim_assets'[site_id]) IS NOT NULL) ||
                ('seed_cleaning_log'[site_name] = RELATED('seed_site_config'[Site]) && RELATED('seed_site_config'[Site]) IS NOT NULL)
            ) &&
            'seed_cleaning_log'[is_active] = TRUE()
        )
    )
RETURN
    IF(
        ISBLANK(LastCleaningDate),
        BLANK(),
        DATEDIFF(LastCleaningDate, TODAY(), DAY)
    )
```

## Cara Menggunakan

### 1. Menambahkan Data Pembersihan Baru

Tambahkan baris baru ke file `dbt/seeds/seed_cleaning_log.csv`:

```csv
asset_type;asset_id;site_id;site_name;cleaning_date;notes;is_active
Sensor;;NE=50488260;;2024-12-20;Pembersihan rutin bulanan semua sensor;
Module;;NE=50488260;;2024-12-18;Pembersihan setelah hujan semua modul;
Sensor;;;pt._mmki_1.75_mwp_-_painting_building;2024-12-20;Pembersihan rutin semua sensor;
Module;;;pt._mmki_1.75_mwp_-_painting_building;2024-12-18;Pembersihan semua modul;
```

### 2. Load ke Database

Jalankan dbt seed:

```bash
dbt seed --select seed_cleaning_log
```

### 3. Refresh Power BI

Setelah data di-refresh di Power BI, formula DAX akan otomatis menghitung days since last cleaning.

## Catatan Penting

1. **Multiple Cleaning Dates**: Jika satu asset dibersihkan beberapa kali, tabel akan menyimpan semua history. DAX formula menggunakan `MAX()` untuk mengambil tanggal terakhir.

2. **Site ID Consistency**: Pastikan `site_id` di `seed_cleaning_log` sesuai dengan `site_id` di tabel referensi (`seed_sensor_config`, `seed_site_config`, `dim_assets`, dll).

3. **Date Format**: Gunakan format `YYYY-MM-DD` untuk `cleaning_date`.

4. **Performance**: Index sudah dibuat untuk mempercepat query berdasarkan asset_type, asset_id, dan cleaning_date.

## Relationship di Power BI

Pastikan relationship sudah dibuat antara:
- `seed_cleaning_log[site_id]` → `seed_sensor_config[site_id]` (untuk FusionSolar sites)
- `seed_cleaning_log[site_name]` → `seed_site_config[Site]` (untuk MMKI dan sites lainnya)
- `seed_cleaning_log[site_id]` → `dim_assets[site_id]` (alternatif untuk Module)
- `seed_cleaning_log[site_name]` → `dim_assets[site_name]` (alternatif untuk Module)

**Catatan Penting**: 
- Cleaning log adalah **per site**, jadi satu record mewakili pembersihan semua sensor atau semua module di site tersebut.
- **Untuk MMKI**: Gunakan `site_name`, bukan `site_id`
- **Untuk FusionSolar**: Gunakan `site_id`
- Pastikan salah satu dari `site_id` atau `site_name` harus diisi

## Contoh Query SQL

### Melihat Last Cleaning Date per Site:

```sql
SELECT 
    asset_type,
    COALESCE(site_id, site_name) as site_identifier,
    site_id,
    site_name,
    MAX(cleaning_date) as last_cleaning_date,
    CURRENT_DATE - MAX(cleaning_date) as days_since_cleaning
FROM staging.seed_cleaning_log
WHERE is_active = TRUE
GROUP BY asset_type, site_id, site_name
ORDER BY days_since_cleaning DESC;
```

### Melihat History Pembersihan:

```sql
SELECT 
    asset_type,
    asset_id,
    COALESCE(site_id, site_name) as site_identifier,
    site_id,
    site_name,
    cleaning_date,
    notes,
    CURRENT_DATE - cleaning_date as days_ago
FROM staging.seed_cleaning_log
WHERE is_active = TRUE
ORDER BY cleaning_date DESC;
```

### Menonaktifkan Record (Soft Delete):

```sql
-- Contoh: Jika ada data yang salah input, nonaktifkan tanpa menghapus
UPDATE staging.seed_cleaning_log
SET is_active = FALSE,
    updated_at = NOW()
WHERE id = 123;  -- ID record yang salah

-- Atau berdasarkan kondisi tertentu
UPDATE staging.seed_cleaning_log
SET is_active = FALSE,
    updated_at = NOW()
WHERE site_id = 'NE=50488260' 
  AND cleaning_date = '2024-12-01'
  AND asset_type = 'Sensor';
```

### Melihat Semua Record (Termasuk yang Dinonaktifkan):

```sql
SELECT 
    asset_type,
    site_id,
    cleaning_date,
    notes,
    is_active,
    CASE WHEN is_active THEN 'Aktif' ELSE 'Dinonaktifkan' END as status
FROM staging.seed_cleaning_log
ORDER BY cleaning_date DESC;
```

