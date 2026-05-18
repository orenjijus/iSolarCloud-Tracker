# Kenapa dim_assets Tidak Ikut dalam dbt run?

## 🔍 Masalah

Dari log terminal (lines 998-1009), terlihat:
- ✅ `dim_date_generated` di-run (line 998)
- ✅ `mart_site_kpi_monthly` di-run (line 1000)
- ✅ Staging views di-run (lines 1002-1009)
- ❌ `dim_assets` **TIDAK muncul** di log

## 🎯 Root Cause

### 1. Materialization Type: TABLE

`dim_assets` menggunakan `materialized='table'`:

```sql
{{ config(
    materialized='table',
    schema='dimensions',
    unique_key='asset_id'
) }}
```

**Perilaku dbt dengan TABLE:**
- dbt akan **skip** model jika:
  - Dependencies tidak berubah
  - Tabel sudah ada di database
  - Tidak ada perubahan di source data

### 2. Dependencies adalah VIEW

`dim_assets` bergantung pada:
- `stg_isolarcloud__sites` (VIEW)
- `stg_isolarcloud__devices` (VIEW)
- `stg_fusionsolar__sites` (VIEW)
- `stg_fusionsolar__devices` (VIEW)
- `seed_site_config` (seed)

**Masalah:**
- VIEW tidak dianggap sebagai "perubahan" oleh dbt
- VIEW selalu fresh (query-time), jadi dbt menganggap tidak ada perubahan
- dbt skip `dim_assets` karena menganggap dependencies tidak berubah

### 3. Execution Order

Dari dependency graph:
1. Staging views di-run (lines 1002-1009)
2. `dim_assets` **seharusnya** di-run setelah staging views
3. Tapi dbt skip karena menganggap tidak perlu refresh

## ✅ Solusi

### Solusi 1: Explicit Select dim_assets (Recommended)

Selalu include `dim_assets` secara eksplisit:

```bash
# Include dimensions folder
dbt run --select dimensions

# Atau include dim_assets secara spesifik
dbt run --select dimensions.dim_assets

# Atau dengan tag
dbt run --select tag:daily_dimensions
```

### Solusi 2: Gunakan Selector `+` pada Staging

Gunakan selector `+` untuk otomatis include downstream:

```bash
# Setelah sync plant/devices
dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

Ini akan otomatis include `dim_assets` karena dependency.

### Solusi 3: Ubah ke Incremental (Not Recommended)

Ubah `dim_assets` menjadi incremental:

```sql
{{ config(
    materialized='incremental',
    unique_key='asset_id',
    schema='dimensions'
) }}
```

**Tapi ini tidak recommended karena:**
- `dim_assets` adalah dimension table (bukan time-series)
- Dimension tables biasanya full refresh, bukan incremental
- Incremental akan kompleks untuk dimension dengan SCD logic

### Solusi 4: Force Refresh dengan Full-Refresh

Force refresh `dim_assets`:

```bash
dbt run --full-refresh --select dimensions.dim_assets
```

**Tapi ini akan:**
- Drop dan recreate tabel
- Kehilangan data sementara
- Tidak praktis untuk daily run

## 📊 Verifikasi

### Cek Apakah dim_assets Akan Di-run

```bash
# Preview models yang akan di-run
dbt list --select dimensions

# Atau dengan tag
dbt list --select tag:daily_dimensions

# Atau dengan selector +
dbt list --select stg_isolarcloud__sites+ stg_fusionsolar__sites+
```

Jika `dim_assets` muncul dalam list, berarti akan di-run.

### Cek Apakah dim_assets Sudah Up-to-date

```sql
-- Cek jumlah rows
SELECT COUNT(*) FROM dimensions.dim_assets;

-- Cek last update (jika ada updated_at column)
SELECT MAX(updated_at) FROM dimensions.dim_assets;

-- Cek apakah ada site/device baru
SELECT system, COUNT(*) 
FROM dimensions.dim_assets 
GROUP BY system;
```

## 🎯 Best Practice

### Normal Daily Run

**Selalu include dimensions:**

```bash
# Option 1: Run semua (termasuk dim_assets)
dbt run

# Option 2: Explicit include dimensions
dbt run --select staging dimensions marts

# Option 3: Dengan tag
dbt run --select tag:daily_staging tag:daily_dimensions tag:daily_marts_measurement_5min
```

### Setelah Sync Plant/Devices

**Gunakan selector `+` untuk otomatis include:**

```bash
dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

Ini akan otomatis:
1. ✅ Refresh staging views
2. ✅ Refresh `dim_assets` (karena dependency)
3. ✅ Refresh semua downstream models

## 🔍 Debug: Kenapa dbt Skip dim_assets?

dbt skip model jika:
1. ✅ Model sudah ada di database
2. ✅ Dependencies tidak berubah (VIEW tidak dianggap perubahan)
3. ✅ Tidak ada perubahan di source data

**Untuk force run:**
- Explicit select: `dbt run --select dimensions.dim_assets`
- Full refresh: `dbt run --full-refresh --select dimensions.dim_assets`
- Use selector `+`: `dbt run --select stg_*__sites+`

## 📝 Kesimpulan

**Kenapa `dim_assets` tidak ikut:**
- `dim_assets` adalah TABLE (bukan incremental)
- Dependencies adalah VIEW (tidak dianggap perubahan)
- dbt skip karena menganggap tidak perlu refresh

**Solusi:**
- ✅ **Selalu include dimensions secara eksplisit** dalam daily run
- ✅ **Gunakan selector `+`** setelah sync plant/devices
- ✅ **Verifikasi dengan `dbt list`** sebelum run

**Tidak disarankan:**
- ❌ Ubah ke incremental (tidak sesuai untuk dimension)
- ❌ Full refresh setiap run (tidak efisien)

