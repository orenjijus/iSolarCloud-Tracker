# Langkah-langkah Reload SPF Data untuk 1-5 Oktober 2025

## Tujuan
Menghapus data SPF (Sumatera Prima Fibreboard) untuk tanggal 1-5 Oktober 2025 dari site performance daily karena target tidak dimulai dari tanggal 1 tapi tanggal 6 Oktober.

## Langkah-langkah

### 1. Reload Seed Daily Target
Reload seed file yang sudah diupdate (data 1-5 Oktober sudah dihapus):

```bash
dbt seed --select seed_daily_simulation_target
```

### 2. Rebuild mart_simulation_targets_daily
Karena `mart_simulation_targets_daily` adalah table dan sudah ada filter WHERE untuk exclude data 1-5 Oktober, rebuild table ini:

```bash
dbt run --select mart_simulation_targets_daily
```

**Catatan:** Karena ini adalah table (bukan incremental), akan otomatis rebuild seluruh table dengan data terbaru dari seed.

### 3. Reingest mart_site_performance_daily untuk tanggal 1-5 Oktober
Reingest mart_site_performance_daily untuk menghapus data SPF di tanggal 1-5 Oktober:

```bash
dbt run --select mart_site_performance_daily \
  --vars '{
    "reingest_start_date": "2025-10-01",
    "reingest_end_date": "2025-10-05"
  }'
```

**Penjelasan:**
- `reingest_start_date`: 2025-10-01 (tanggal mulai)
- `reingest_end_date`: 2025-10-05 (tanggal akhir)
- Karena `mart_site_performance_daily` menggunakan `mart_simulation_targets_daily`, data SPF untuk 1-5 Oktober akan otomatis tidak muncul karena sudah di-exclude di `mart_simulation_targets_daily`

## Command Lengkap (Sekaligus)

```bash
# Step 1: Reload seed
dbt seed --select seed_daily_simulation_target

# Step 2: Rebuild mart_simulation_targets_daily
dbt run --select mart_simulation_targets_daily

# Step 3: Reingest mart_site_performance_daily untuk 1-5 Oktober
dbt run --select mart_site_performance_daily \
  --vars '{
    "reingest_start_date": "2025-10-01",
    "reingest_end_date": "2025-10-05"
  }'
```

## Verifikasi

Setelah selesai, verifikasi bahwa data SPF untuk 1-5 Oktober sudah tidak ada:

```sql
SELECT 
    date_key,
    site_name,
    site_code,
    energy_target_mwh,
    energy_kpi_daily_mwh
FROM mart.mart_simulation_targets_daily
WHERE (site_code = '1680199' OR site_name LIKE '%Sumatera Prima Fibreboard%')
    AND date_key >= '2025-10-01'
    AND date_key <= '2025-10-05'
ORDER BY date_key;
```

Query di atas seharusnya tidak mengembalikan data untuk tanggal 1-5 Oktober.

```sql
SELECT 
    date_key,
    site_name,
    site_id,
    energy_target_mwh,
    energy_kpi_daily_mwh
FROM mart.mart_site_performance_daily
WHERE (site_id = 'ISO_SITE_1680199' OR site_name LIKE '%Sumatera Prima Fibreboard%')
    AND date_key >= '2025-10-01'
    AND date_key <= '2025-10-05'
ORDER BY date_key;
```

Query di atas juga seharusnya tidak mengembalikan data untuk tanggal 1-5 Oktober.

