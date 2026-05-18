# Issue: Data FusionSolar Ter-skip saat dbt run

## 🔍 Masalah

Ketika menjalankan `dbt run` untuk tanggal 2025-12-17:
- ✅ Data iSolarCloud ter-process dan muncul di `mart_site_performance_daily`
- ❌ Data FusionSolar **tidak ter-process** dan tidak muncul di `mart_site_performance_daily`
- ✅ Data FusionSolar **baru ter-process** ketika re-run model staging FusionSolar secara manual

## 🎯 Root Cause

**Masalah BUKAN di filter incremental** (filter sudah system-specific), tapi kemungkinan di:

### 1. **Execution Order Issue**

dbt menjalankan model berdasarkan dependency graph. Jika:
- iSolarCloud models di-run **dulu** (karena dependency order)
- FusionSolar models di-run **kemudian**
- Dan ada issue dengan **execution timing** atau **transaction isolation**

Maka data FusionSolar mungkin ter-skip.

### 2. **Staging Data Availability**

Kemungkinan saat `dbt run` dijalankan:
- Data iSolarCloud sudah ada di `raw.isolarcloud_historical_data` untuk 2025-12-17
- Data FusionSolar **belum ada** di `raw.fusionsolar_historical_data` untuk 2025-12-17
- Setelah Python ETL FusionSolar dijalankan, data baru muncul
- Tapi `dbt run` sudah selesai, jadi FusionSolar tidak ter-process

### 3. **Incremental Filter Logic (Unlikely but possible)**

Meskipun filter sudah system-specific, ada kemungkinan edge case:
- Jika `MAX(timestamp)` untuk FusionSolar di staging = NULL (tabel kosong)
- Dan data FusionSolar di raw punya timestamp yang lebih kecil dari iSolarCloud
- Maka filter mungkin tidak bekerja dengan benar

## ✅ Solusi

### Solusi 1: Verifikasi Data Raw Tersedia

**Sebelum** menjalankan `dbt run`, pastikan data untuk kedua system sudah tersedia:

```sql
-- Cek data iSolarCloud
SELECT 
    'iSolarCloud' as system,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp
FROM raw.isolarcloud_historical_data
WHERE DATE(timestamp) = '2025-12-17';

-- Cek data FusionSolar
SELECT 
    'FusionSolar' as system,
    COUNT(*) as row_count,
    MIN(collect_time) as min_timestamp,
    MAX(collect_time) as max_timestamp
FROM raw.fusionsolar_historical_data
WHERE DATE(collect_time) = '2025-12-17';
```

**Jika data FusionSolar belum ada**, jalankan Python ETL FusionSolar terlebih dahulu.

### Solusi 2: Run dengan Explicit Selection

Untuk memastikan kedua system ter-process, run dengan explicit selection:

```bash
# Run staging untuk kedua system
dbt run --select stg_isolarcloud__perf_unpivoted stg_fusionsolar__perf_unpivoted

# Kemudian run downstream
dbt run --select stg_isolarcloud__perf_unpivoted+ stg_fusionsolar__perf_unpivoted+
```

### Solusi 3: Force Re-process dengan Vars

Jika data sudah ada di raw tapi tidak ter-process, force re-process:

```bash
# Re-process untuk tanggal tertentu
dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

### Solusi 4: Check Execution Order

Untuk melihat urutan eksekusi dbt:

```bash
# Preview execution plan
dbt list --select staging+ --output name

# Atau dengan graph
dbt docs generate
dbt docs serve
```

## 🔍 Diagnosis

### Step 1: Cek Data di Raw Tables

```sql
-- Cek apakah data sudah ada di raw tables
SELECT 
    'iSolarCloud' as system,
    COUNT(*) as count,
    MIN(timestamp) as min_ts,
    MAX(timestamp) as max_ts
FROM raw.isolarcloud_historical_data
WHERE DATE(timestamp) = '2025-12-17'
UNION ALL
SELECT 
    'FusionSolar' as system,
    COUNT(*) as count,
    MIN(collect_time) as min_ts,
    MAX(collect_time) as max_ts
FROM raw.fusionsolar_historical_data
WHERE DATE(collect_time) = '2025-12-17';
```

### Step 2: Cek Data di Staging

```sql
-- Cek apakah data sudah ter-process di staging
SELECT 
    'iSolarCloud' as system,
    COUNT(*) as count,
    MIN(timestamp) as min_ts,
    MAX(timestamp) as max_ts
FROM staging.stg_isolarcloud__perf_unpivoted
WHERE DATE(timestamp) = '2025-12-17'
UNION ALL
SELECT 
    'FusionSolar' as system,
    COUNT(*) as count,
    MIN(timestamp) as min_ts,
    MAX(timestamp) as max_ts
FROM staging.stg_fusionsolar__perf_unpivoted
WHERE DATE(timestamp) = '2025-12-17';
```

### Step 3: Cek MAX Timestamp di Staging

```sql
-- Cek MAX timestamp untuk masing-masing system
SELECT 
    'iSolarCloud' as system,
    MAX(timestamp) as max_timestamp
FROM staging.stg_isolarcloud__perf_unpivoted
UNION ALL
SELECT 
    'FusionSolar' as system,
    MAX(timestamp) as max_timestamp
FROM staging.stg_fusionsolar__perf_unpivoted;
```

Jika MAX timestamp FusionSolar < data baru di raw, berarti incremental filter akan skip data tersebut.

### Step 4: Cek Execution Log

Cek log dbt untuk melihat:
- Urutan model yang di-run
- Apakah `stg_fusionsolar__perf_unpivoted` di-run?
- Berapa banyak rows yang di-insert?

```bash
# Cek log terakhir
tail -n 100 dbt/logs/dbt.log | grep -i fusionsolar
```

## 📋 Checklist

Sebelum menjalankan `dbt run`:

- [ ] Data iSolarCloud sudah ada di `raw.isolarcloud_historical_data` untuk tanggal target
- [ ] Data FusionSolar sudah ada di `raw.fusionsolar_historical_data` untuk tanggal target
- [ ] Python ETL untuk kedua system sudah dijalankan
- [ ] Tidak ada data gap atau missing data di raw tables

Setelah `dbt run`:

- [ ] Cek apakah `stg_fusionsolar__perf_unpivoted` di-run (lihat log)
- [ ] Cek apakah ada rows yang di-insert untuk FusionSolar
- [ ] Verifikasi data muncul di `mart_site_performance_daily` untuk kedua system

## 🎯 Best Practice

1. **Selalu verifikasi data raw tersedia** sebelum menjalankan `dbt run`
2. **Jalankan Python ETL untuk kedua system** sebelum `dbt run`
3. **Monitor execution log** untuk memastikan semua model ter-run
4. **Gunakan `--vars` untuk force re-process** jika ada data yang ter-skip

## 🔧 Potential Fix (Future)

Jika masalah ini sering terjadi, pertimbangkan:

1. **Add explicit dependency** antara staging models untuk memastikan execution order
2. **Add validation checks** di model untuk memastikan data dari kedua system ter-process
3. **Add warning/error** jika data dari satu system tidak ter-process

