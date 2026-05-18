# Best Practice: Refresh dim_assets

## Masalah

Ketika melakukan `sync plant -> devices`, data di schema `raw` berubah, tetapi `dim_assets` tidak otomatis ter-refresh. Ini menyebabkan:

1. **Data tidak konsisten**: `dim_assets` masih menggunakan data lama
2. **Downstream models terpengaruh**: 10+ model bergantung pada `dim_assets`:
   - `fact_inverter_calculations_5min`
   - `fact_sensor_calculations_5min`
   - `fact_site_calculations_5min`
   - `mart_inverter_performance_5min`
   - `mart_meter_performance_5min`
   - `mart_sensor_measurements_5min`
   - `mart_site_performance_daily`
   - `mart_inverter_yield_daily`
   - `mart_simulation_targets_daily`
   - `mart_inverter_performance_daily`

## Perbedaan dengan dim_date_generated

| Model | Dependencies | Refresh Frequency | Reason |
|-------|--------------|-------------------|--------|
| `dim_date_generated` | Tidak ada (static) | Jarang (hanya saat perlu extend range) | Generate tanggal, tidak bergantung pada raw data |
| `dim_assets` | `stg_*__sites`, `stg_*__devices` | **Setiap kali staging berubah** | Bergantung pada metadata sites/devices yang bisa berubah |

## Solusi: Selalu Include dim_assets

### ✅ Solusi yang Sudah Diimplementasikan

`dim_assets` sekarang memiliki tag `daily_dimensions` dan akan otomatis terikut ketika:
1. Menjalankan `dbt run` tanpa selector (run semua)
2. Menjalankan dengan selector `dimensions` atau `tag:dimensions`
3. Menjalankan staging models dengan selector `+` (karena dependency)

### Opsi 1: Gunakan Selector `+` (Recommended)

Gunakan selector `+` untuk otomatis include downstream dependencies:

```bash
# Setelah sync plant/devices, refresh staging + dim_assets + downstream
dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

**Keuntungan:**
- ✅ Otomatis include `dim_assets` (karena depend pada staging)
- ✅ Otomatis include semua downstream models yang depend pada `dim_assets`
- ✅ Tidak perlu manual run `dim_assets` terpisah
- ✅ Memastikan konsistensi data

### Opsi 2: Explicit Include dengan Tag

Gunakan tag untuk include `dim_assets`:

```bash
# Include semua dimensions (termasuk dim_assets)
dbt run --select tag:dimensions

# Atau include daily_dimensions (hanya dim_assets, bukan dim_date_generated)
dbt run --select tag:daily_dimensions
```

### Opsi 3: Explicit Include Model

Include `dim_assets` secara eksplisit:

```bash
# Normal run - include dimensions
dbt run --select staging dimensions

# Atau setelah sync
dbt run --select stg_isolarcloud__sites stg_fusionsolar__sites stg_isolarcloud__devices stg_fusionsolar__devices dimensions.dim_assets
```

### Opsi 4: Full Run (Simple)

Jika tidak masalah dengan waktu eksekusi:

```bash
# Run semua (termasuk dim_assets)
dbt run
```

**Catatan:** `dim_assets` sangat cepat (~0.25s, 523 rows), jadi tidak masalah include dalam setiap run.

## Workflow yang Disarankan

### Setelah Sync Plant/Devices

```bash
# 1. Refresh staging models untuk sites/devices
dbt run --select stg_isolarcloud__sites stg_fusionsolar__sites stg_isolarcloud__devices stg_fusionsolar__devices

# 2. Refresh dim_assets (karena bergantung pada staging di atas)
dbt run --select dimensions.dim_assets

# 3. Refresh downstream models yang bergantung pada dim_assets
dbt run --select dimensions.dim_assets+
```

**Atau dalam satu command:**

```bash
dbt run --select stg_isolarcloud__sites stg_fusionsolar__sites stg_isolarcloud__devices stg_fusionsolar__devices dimensions.dim_assets+
```

### Normal Daily Run

```bash
# Include dimensions untuk memastikan dim_assets selalu up-to-date
dbt run --select staging dimensions marts
```

## Kesimpulan

✅ **`dim_assets` HARUS selalu ikut dalam model run** karena:
- Bergantung pada staging models yang berubah saat sync
- Banyak downstream models bergantung padanya
- Eksekusi sangat cepat (~0.25s)
- Memastikan konsistensi data

❌ **`dim_date_generated` TIDAK perlu selalu refresh** karena:
- Static table (generate dates)
- Tidak bergantung pada raw data
- Hanya perlu refresh saat extend date range

## Rekomendasi Final

### Setelah Sync Plant/Devices

**Gunakan selector `+` untuk otomatis include dependencies:**

```bash
# Setelah sync plant/devices
dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

Ini akan otomatis:
1. ✅ Refresh staging models
2. ✅ Refresh `dim_assets` (karena depend pada staging)
3. ✅ Refresh semua downstream models yang depend pada `dim_assets`

**Tidak perlu manual run `dbt run dim_assets` lagi!**

### Normal Daily Run

Untuk daily run normal, include dimensions:

```bash
# Option 1: Run semua
dbt run

# Option 2: Include dimensions dengan tag
dbt run --select tag:daily_staging tag:daily_dimensions tag:daily_marts_measurement_5min

# Option 3: Include dimensions folder
dbt run --select staging dimensions marts
```

### Verifikasi dim_assets Terikut

Untuk memastikan `dim_assets` terikut dalam run:

```bash
# Preview models yang akan di-run (tanpa execute)
dbt list --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+

# Atau dengan tag
dbt list --select tag:daily_dimensions
```

Jika `dim_assets` muncul dalam list, berarti akan terikut dalam run.

