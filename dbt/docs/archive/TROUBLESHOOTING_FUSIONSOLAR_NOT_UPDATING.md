# Troubleshooting: FusionSolar Data Tidak Terupdate

## 🔍 Masalah

Ketika menjalankan `dbt run`, data iSolarCloud terupdate tetapi FusionSolar tidak terupdate (INSERT 0 rows).

## 📊 Diagnosa

Jalankan query diagnostik untuk memeriksa:

```bash
# Jalankan query diagnostik
psql -h <host> -U <user> -d MMSR -f queries/diagnose_fusionsolar_not_updating.sql
```

Atau jalankan query SQL langsung di database.

## 🔎 Kemungkinan Penyebab

### 1. Tidak Ada Data Baru di Raw Table

**Gejala:**
- MAX(timestamp) di raw = MAX(timestamp) di staging
- Query #6 menunjukkan 0 rows yang harus di-pickup

**Solusi:**
- Pastikan Python harvester FusionSolar sudah dijalankan
- Cek apakah ada data baru di `raw.fusionsolar_historical_data`
- Verifikasi ETL pipeline FusionSolar berjalan dengan benar

### 2. Data Sudah Ada di Staging (Duplicate)

**Gejala:**
- Raw table punya data baru
- Tapi staging sudah punya data tersebut (mungkin dari run sebelumnya)
- Unique key constraint mencegah duplicate insert

**Solusi:**
- Cek apakah data sudah ada di staging dengan query:
```sql
SELECT COUNT(*) 
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted"
WHERE timestamp >= 'YYYY-MM-DD'::timestamp;
```

### 3. Masalah dengan Filter Incremental

**Gejala:**
- Raw table punya data baru
- Tapi filter incremental tidak mengambil data tersebut

**Solusi:**
- Cek MAX(timestamp) di staging vs MAX(collect_time) di raw
- Jika raw lebih baru, incremental seharusnya mengambil
- Jika tidak, mungkin ada masalah dengan timezone atau format timestamp

### 4. Data di Raw Table Tidak Valid

**Gejala:**
- Raw table punya data
- Tapi setelah transformasi (unpivot, filter NULL), tidak ada rows yang valid

**Solusi:**
- Cek apakah `measurement_data` JSONB valid
- Cek apakah ada metric_value yang bisa di-convert ke numeric
- Query untuk cek:
```sql
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE measurement_data IS NULL) as null_data,
    COUNT(*) FILTER (WHERE jsonb_typeof(measurement_data) != 'object') as invalid_jsonb
FROM "MMSR"."raw"."fusionsolar_historical_data"
WHERE collect_time > (SELECT MAX(timestamp) FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted");
```

## ✅ Solusi

### Solusi 1: Force Re-ingest Date Range

Jika ada data baru di raw tapi tidak ter-pickup, force re-ingest:

```bash
# Re-ingest date range tertentu
dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-01-18",
    "reingest_end_date": "2025-01-18"
  }'
```

### Solusi 2: Full Refresh (Hati-hati!)

**⚠️ WARNING:** Full refresh akan menghapus semua data dan rebuild dari awal. Hanya gunakan jika benar-benar diperlukan.

```bash
# Full refresh staging model
dbt run --select stg_fusionsolar__perf_unpivoted --full-refresh
```

### Solusi 3: Cek dan Update Raw Data

Pastikan Python harvester FusionSolar sudah dijalankan dan data sudah masuk ke raw table:

```bash
# Jalankan FusionSolar harvester
python fusionsolar/fusionsolar_harvester_src/fusionsolar_data_processing.py
```

### Solusi 4: Verifikasi Timezone

Cek apakah ada masalah timezone antara raw dan staging:

```sql
-- Cek timezone di raw
SELECT 
    collect_time,
    collect_time AT TIME ZONE 'UTC' as utc_time,
    collect_time AT TIME ZONE 'Asia/Jakarta' as jakarta_time
FROM "MMSR"."raw"."fusionsolar_historical_data"
ORDER BY collect_time DESC
LIMIT 5;

-- Cek timezone di staging
SELECT 
    timestamp,
    timestamp AT TIME ZONE 'UTC' as utc_time,
    timestamp AT TIME ZONE 'Asia/Jakarta' as jakarta_time
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted"
ORDER BY timestamp DESC
LIMIT 5;
```

## 🔄 Workflow yang Disarankan

### Setelah Sync Plant/Devices

```bash
# 1. Pastikan raw data sudah ter-update
# (Jalankan Python harvester jika perlu)

# 2. Refresh staging dengan force re-ingest jika perlu
dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "YYYY-MM-DD",
    "reingest_end_date": "YYYY-MM-DD"
  }'

# 3. Atau gunakan normal incremental (jika ada data baru)
dbt run --select stg_fusionsolar__perf_unpivoted+
```

### Normal Daily Run

```bash
# Run semua (incremental akan otomatis ambil data baru)
dbt run
```

## 📝 Checklist Troubleshooting

- [ ] Cek MAX(timestamp) di raw vs staging
- [ ] Cek apakah ada data baru di raw table
- [ ] Cek apakah Python harvester sudah dijalankan
- [ ] Cek apakah data sudah ada di staging (duplicate)
- [ ] Cek apakah filter incremental bekerja dengan benar
- [ ] Cek apakah data di raw table valid (JSONB format)
- [ ] Cek timezone consistency

## 🆘 Jika Masih Bermasalah

1. **Cek Logs:**
   - Python harvester logs
   - dbt logs (`dbt/logs/dbt.log`)

2. **Cek Database:**
   - Raw table row counts
   - Staging table row counts
   - Compare timestamps

3. **Force Re-ingest:**
   - Gunakan re-ingest dengan date range spesifik
   - Monitor apakah data masuk

4. **Full Refresh (Last Resort):**
   - Hanya jika semua solusi lain gagal
   - Backup data penting sebelum full refresh

