# Quick Fix: FusionSolar Tidak Terupdate

## 🔍 Masalah

Dari output `dbt run`, terlihat:
```
stg_fusionsolar__perf_unpivoted [INSERT 0 1483080]
```

Ini berarti model di-run tapi **INSERT 0 rows** = tidak ada data baru yang masuk.

## ✅ Solusi Cepat

### 1. Cek Apakah Ada Data Baru di Raw Table

Jalankan query ini di database:

```sql
-- Cek MAX timestamp di raw vs staging
SELECT 
    'Raw MAX' as source,
    MAX(collect_time) as max_timestamp
FROM "MMSR"."raw"."fusionsolar_historical_data"
UNION ALL
SELECT 
    'Staging MAX' as source,
    MAX(timestamp) as max_timestamp
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted";
```

**Jika Raw MAX = Staging MAX:**
- ✅ Tidak ada data baru → Normal, tidak perlu update
- ❌ Jika seharusnya ada data baru → Python harvester belum dijalankan

**Jika Raw MAX > Staging MAX:**
- Ada data baru yang belum di-pickup → Gunakan Solusi 2

### 2. Force Re-ingest Date Range Tertentu

Jika ada data baru di raw tapi tidak ter-pickup oleh incremental:

```bash
# Re-ingest date range tertentu (contoh: hari ini)
dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-01-18",
    "reingest_end_date": "2025-01-18"
  }'
```

**Ganti tanggal sesuai dengan tanggal data yang ingin di-re-ingest.**

### 3. Pastikan Python Harvester Sudah Dijalankan

Jika tidak ada data baru di raw table, pastikan Python harvester FusionSolar sudah dijalankan:

```bash
# Jalankan FusionSolar harvester
python fusionsolar/fusionsolar_harvester_src/fusionsolar_data_processing.py
```

### 4. Cek Data Terbaru di Raw Table

```sql
-- Cek data terbaru di raw (7 hari terakhir)
SELECT 
    DATE(collect_time) as date,
    COUNT(*) as row_count,
    MAX(collect_time) as max_time
FROM "MMSR"."raw"."fusionsolar_historical_data"
WHERE collect_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(collect_time)
ORDER BY date DESC;
```

## 🎯 Workflow Lengkap

### Setelah Sync Plant/Devices

```bash
# 1. Pastikan raw data sudah ter-update
# (Jalankan Python harvester jika perlu)

# 2. Refresh staging dengan force re-ingest
dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-01-18",
    "reingest_end_date": "2025-01-18"
  }'
```

### Normal Daily Run

```bash
# Run semua (incremental akan otomatis ambil data baru jika ada)
dbt run
```

## 📊 Perbandingan dengan iSolarCloud

Dari output terminal, iSolarCloud terupdate karena:
- Ada data baru di raw table
- Incremental filter mengambil data baru tersebut

FusionSolar tidak terupdate karena:
- Tidak ada data baru di raw table, ATAU
- Data sudah ada di staging (incremental filter skip)

## ⚠️ Catatan Penting

- **INSERT 0 rows** tidak selalu berarti error
- Bisa berarti tidak ada data baru (normal untuk incremental)
- Cek MAX(timestamp) di raw vs staging untuk konfirmasi

## 🔍 Query Diagnostik Lengkap

Untuk diagnosa lebih detail, jalankan:
```sql
-- File: queries/diagnose_fusionsolar_not_updating.sql
```

atau lihat dokumentasi lengkap di:
- `dbt/docs/TROUBLESHOOTING_FUSIONSOLAR_NOT_UPDATING.md`

