# Troubleshooting: Data FusionSolar Tidak Sampai ke mart_site_performance_daily

## Masalah

Setelah menjalankan `dbt run`, data FusionSolar untuk tanggal 2025-12-17 tidak muncul di `mart_site_performance_daily`, padahal data sudah ada di staging layer.

## Penyebab Umum

### 1. **Incremental Filter Skip Data**

Jika sudah ada data dengan `date_key > 2025-12-17` di `mart_site_performance_daily`, maka incremental filter akan skip tanggal 2025-12-17:

```sql
-- Filter di mart_site_performance_daily
AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
```

**Solusi:** Gunakan `--vars` untuk force re-process tanggal tersebut:

```bash
dbt run --select mart_site_performance_daily+ \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

### 2. **Data Tidak Ada di Layer Intermediate**

Data mungkin tidak sampai ke layer intermediate (`fact_site_calculations_5min`, `mart_sensor_daily`, dll).

**Diagnosis:** Jalankan script diagnostic:

```sql
-- Lihat file: dbt/scripts/check_fusionsolar_data_2025-12-17.sql
```

Atau query manual:

```sql
-- Check setiap layer
SELECT 'stg_fusionsolar__perf_unpivoted' as layer, COUNT(*) 
FROM staging.stg_fusionsolar__perf_unpivoted 
WHERE DATE(timestamp) = '2025-12-17';

SELECT 'mart_meter_performance_5min' as layer, COUNT(*) 
FROM mart.mart_meter_performance_5min 
WHERE system = 'fusionsolar' AND date_key = '2025-12-17';

SELECT 'fact_site_calculations_5min' as layer, COUNT(*) 
FROM mart.fact_site_calculations_5min 
WHERE system = 'fusionsolar' AND date_key = '2025-12-17';

SELECT 'mart_site_performance_daily' as layer, COUNT(*) 
FROM mart.mart_site_performance_daily 
WHERE system = 'fusionsolar' AND date_key = '2025-12-17';
```

### 3. **Missing Revenue Meters**

`mart_site_performance_daily` membutuhkan **Revenue Meters** untuk menghitung energy. Jika tidak ada revenue meter yang terkonfigurasi untuk site FusionSolar, data tidak akan muncul.

**Cek:**
```sql
-- Cek apakah ada revenue meter untuk FusionSolar sites
SELECT DISTINCT m.site_name, m.asset_id, mc.meter_type
FROM mart.mart_meter_performance_5min m
JOIN staging.seed_meter_config mc 
    ON m.asset_id = CONCAT('FS_', mc.esn_code)
WHERE m.system = 'fusionsolar' 
    AND m.date_key = '2025-12-17'
    AND mc.meter_type = 'Revenue';
```

### 4. **Missing Sensor Data**

Jika tidak ada sensor data (GHI/POA), data akan tetap muncul tapi dengan `daily_ghi_kwh_m2 = NULL` dan `daily_poa_weighted_kwh_m2 = NULL`.

**Cek:**
```sql
-- Cek sensor data untuk FusionSolar
SELECT COUNT(*) 
FROM mart.mart_sensor_daily 
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17';
```

### 5. **Missing fact_site_calculations_5min**

`mart_site_performance_daily` membutuhkan `fact_site_calculations_5min` untuk availability calculation. Jika tidak ada, data tidak akan muncul.

**Cek:**
```sql
-- Cek fact_site_calculations_5min
SELECT COUNT(*), COUNT(DISTINCT site_name)
FROM mart.fact_site_calculations_5min
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17';
```

### 6. **calculation_start_date Filter**

Jika site FusionSolar memiliki `calculation_start_date` yang lebih besar dari 2025-12-17, data akan di-filter out.

**Cek:**
```sql
-- Cek calculation_start_date untuk FusionSolar sites
SELECT site_name, calculation_start_date, actual_capacity_kw
FROM dimensions.dim_assets
WHERE system = 'fusionsolar' 
    AND asset_level = 'Site';
```

## Solusi Step-by-Step

### Step 1: Diagnose Masalah

Jalankan script diagnostic:

```bash
cd dbt
psql -h <host> -U <user> -d MMSR -f scripts/check_fusionsolar_data_2025-12-17.sql
```

Atau query manual di database.

### Step 2: Re-run dengan Re-ingest Vars

Jika data sudah ada di staging tapi tidak sampai ke final layer, force re-process:

```bash
# Re-process semua layer untuk tanggal 2025-12-17
dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

Atau jika hanya ingin re-process final layer:

```bash
# Re-process hanya mart_site_performance_daily
dbt run --select mart_site_performance_daily \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

### Step 3: Verifikasi Data

Setelah re-run, verifikasi:

```sql
SELECT 
    date_key,
    site_name,
    system,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    availability_percent
FROM mart.mart_site_performance_daily
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
ORDER BY site_name;
```

## Checklist

- [ ] Data ada di `stg_fusionsolar__perf_unpivoted` untuk 2025-12-17
- [ ] Data ada di `mart_meter_performance_5min` untuk 2025-12-17
- [ ] Data ada di `mart_sensor_measurements_5min` untuk 2025-12-17
- [ ] Data ada di `mart_inverter_performance_5min` untuk 2025-12-17
- [ ] Data ada di `fact_inverter_calculations_5min` untuk 2025-12-17
- [ ] Data ada di `fact_site_calculations_5min` untuk 2025-12-17
- [ ] Data ada di `mart_sensor_daily` untuk 2025-12-17
- [ ] Revenue meters terkonfigurasi untuk FusionSolar sites
- [ ] `calculation_start_date` tidak menghalangi tanggal 2025-12-17
- [ ] Sudah re-run dengan `--vars` untuk force re-process

## Catatan Penting

1. **Incremental models** hanya memproses data baru (date_key > MAX(date_key)). Untuk re-process tanggal lama, gunakan `--vars` dengan `reingest_start_date` dan `reingest_end_date`.

2. **Full refresh** tidak disarankan untuk tabel besar. Gunakan `--vars` untuk re-process tanggal tertentu saja.

3. **Dependency chain** harus lengkap:
   - Staging → Mart 5min → Facts → Mart Daily
   - Jika salah satu layer missing, downstream tidak akan ada data.

