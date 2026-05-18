# Quick Reference: Memastikan dim_assets Terikut dalam dbt run

## ✅ Solusi yang Sudah Diimplementasikan

`dim_assets` sekarang memiliki tag `daily_dimensions` dan akan otomatis terikut dalam beberapa skenario.

## 🎯 Cara Memastikan dim_assets Terikut

### 1. Setelah Sync Plant/Devices (RECOMMENDED)

```bash
# Gunakan selector + untuk otomatis include dependencies
dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

**Ini akan otomatis include:**
- ✅ Staging models (sites/devices)
- ✅ `dim_assets` (karena depend pada staging)
- ✅ Semua downstream models yang depend pada `dim_assets`

### 2. Normal Daily Run

```bash
# Option 1: Run semua (termasuk dim_assets)
dbt run

# Option 2: Include dimensions dengan tag
dbt run --select tag:daily_staging tag:daily_dimensions tag:daily_marts_measurement_5min

# Option 3: Include dimensions folder
dbt run --select staging dimensions marts
```

### 3. Hanya Refresh dim_assets

```bash
# Dengan tag
dbt run --select tag:daily_dimensions

# Atau dengan nama model
dbt run --select dimensions.dim_assets
```

## 🔍 Verifikasi dim_assets Akan Terikut

Sebelum run, cek apakah `dim_assets` akan terikut:

```bash
# Preview models yang akan di-run
dbt list --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

Jika `dim_assets` muncul dalam list, berarti akan terikut dalam run.

## ⚠️ Catatan Penting

- **`dim_assets` HARUS di-refresh** setiap kali staging sites/devices berubah
- **`dim_date_generated` TIDAK perlu** di-refresh (static table)
- Gunakan selector `+` untuk otomatis include dependencies
- `dim_assets` sangat cepat (~0.25s), tidak masalah include dalam setiap run

## 📝 Contoh Workflow Lengkap

### Setelah Sync Plant/Devices

```bash
# 1. Sync plant/devices (Python script)
# ... sync script dijalankan ...

# 2. Refresh staging + dim_assets + downstream
dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

### Normal Daily ETL

```bash
# Run semua models (termasuk dim_assets)
dbt run
```

atau jika menggunakan pipeline dengan tags:

```bash
# Include dimensions dalam daily run
dbt run --select tag:daily_staging tag:daily_dimensions tag:daily_marts_measurement_5min tag:daily_facts_calculation_5min tag:daily_marts_daily
```

