# Langkah-langkah Refresh Setelah Mengubah Capacity di seed_site_config

## Ringkasan Dependensi

Ketika capacity di `seed_site_config.csv` diubah, nilai ini mempengaruhi:

1. **`dim_assets`** (table) - membaca `actual_capacity_kw` dari seed
2. **`mart_site_performance_daily`** (incremental table) - menggunakan `actual_capacity_kw` untuk menghitung:
   - `pr_ghi_actual` = (daily_energy_mwh * 1000) / (daily_ghi_kwh_m2) / actual_capacity_kw
   - `pr_poa_actual` = (daily_energy_mwh * 1000) / (daily_poa_weighted_kwh_m2) / actual_capacity_kw
3. **`mart_site_performance_monthly`** (view) - agregasi dari daily, otomatis update

## Langkah-langkah Refresh

### 1. Re-seed file seed_site_config
```bash
dbt seed --select seed_site_config
```

### 2. Rebuild dim_assets (karena table dan membaca dari seed)
```bash
dbt run --select dim_assets
```

### 3. Full refresh mart_site_performance_daily (PENTING!)
Karena `mart_site_performance_daily` adalah **incremental table** dan menggunakan `actual_capacity_kw` untuk perhitungan PR, semua data historis perlu di-recalculate dengan capacity yang baru.

```bash
# Option 1: Full refresh dengan --full-refresh flag
dbt run --select mart_site_performance_daily --full-refresh

# Option 2: Atau drop table dulu, lalu run
dbt run --select mart_site_performance_daily --full-refresh
```

**Mengapa perlu full refresh?**
- PR (Performance Ratio) dihitung menggunakan `actual_capacity_kw`
- Jika capacity berubah, semua PR historis perlu dihitung ulang
- Incremental mode hanya akan update data baru, tidak akan recalculate data lama

### 4. mart_site_performance_monthly (otomatis update)
Karena `mart_site_performance_monthly` adalah **VIEW** yang membaca dari `mart_site_performance_daily`, tidak perlu di-refresh manual. Akan otomatis menggunakan data terbaru setelah step 3 selesai.

## Command Lengkap (Sekaligus)

```bash
# Step 1: Re-seed
dbt seed --select seed_site_config

# Step 2: Rebuild dim_assets
dbt run --select dim_assets

# Step 3: Full refresh daily performance (ini yang paling penting!)
dbt run --select mart_site_performance_daily --full-refresh
```

## Verifikasi

Setelah refresh, verifikasi bahwa:
1. `dim_assets` memiliki `actual_capacity_kw` yang benar untuk site yang diubah
2. `mart_site_performance_daily` memiliki PR values yang masuk akal dengan capacity baru
3. `mart_site_performance_monthly` otomatis menunjukkan nilai yang benar

## Catatan Penting

- **JANGAN** hanya run `mart_site_performance_daily` tanpa `--full-refresh` karena akan skip data historis
- Jika ada banyak data historis, full refresh bisa memakan waktu lama
- Pastikan backup data penting sebelum full refresh (jika diperlukan)

