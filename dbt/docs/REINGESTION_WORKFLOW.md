# Re-ingestion Workflow

## 📋 Overview

Dokumentasi ini menjelaskan cara melakukan **re-ingestion** data untuk date range tertentu dengan opsi filter per site. Fitur ini memungkinkan Anda untuk:

- ✅ Re-process data untuk date range tertentu tanpa full refresh seluruh tabel
- ✅ Filter re-ingestion per site (iSolarCloud) atau per plant (FusionSolar)
- ✅ Cascading refresh otomatis ke semua downstream models
- ✅ Tetap menggunakan incremental mode untuk daily runs normal

---

## 🎯 Kapan Perlu Re-ingest?

Re-ingestion diperlukan ketika:

1. **Raw data corrections dari API** - Data di raw table diperbaiki/update
2. **Missing data backfill** - Data yang sebelumnya missing sekarang tersedia
3. **Data quality issues** - Ditemukan masalah kualitas data yang perlu diperbaiki
4. **Perubahan konfigurasi** - Device/site configuration berubah yang mempengaruhi transformasi
5. **Metric mapping updates** - Perubahan di `seed_metric_mapper` yang perlu di-apply ke historical data

---

## 🔧 Cara Re-ingest

### 1. Normal Daily Run (Incremental Biasa)

Untuk daily run normal, **TIDAK perlu** specify vars apapun:

```bash
dbt run
```

Ini akan menjalankan incremental load normal (hanya data baru).

---

### 2. Re-ingest Semua Site (Date Range)

Re-ingest semua site untuk date range tertentu:

```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-05",
    "reingest_end_date": "2025-11-02"
  }'
```

**Penjelasan:**
- `stg_isolarcloud__perf_unpivoted+` = staging model + semua downstream dependencies
- `reingest_start_date` = tanggal mulai (inclusive)
- `reingest_end_date` = tanggal akhir (inclusive)
- Tanpa `reingest_ps_ids` = semua site akan di-re-process

---

### 3. Re-ingest 1 Site Tertentu

Re-ingest hanya untuk 1 site:

```bash
# Cek ps_id dulu (jika belum tahu)
# Query: SELECT ps_id, ps_name FROM raw.isolarcloud_power_stations;

dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-05",
    "reingest_end_date": "2025-11-02",
    "reingest_ps_ids": "PS001"
  }'
```

**Format `reingest_ps_ids`:**
- Comma-separated string (tanpa spasi)
- Contoh: `"PS001,PS002,PS003"`

---

### 4. Re-ingest Beberapa Site

Re-ingest beberapa site sekaligus:

```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-05",
    "reingest_end_date": "2025-11-02",
    "reingest_ps_ids": "PS001,PS002,PS003"
  }'
```

---

### 5. Re-ingest FusionSolar

Untuk FusionSolar, gunakan `reingest_plant_codes`:

```bash
# Cek plant_code dulu
# Query: SELECT plant_code, plant_name FROM raw.fusionsolar_plants;

dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-05",
    "reingest_end_date": "2025-11-02",
    "reingest_plant_codes": "PLANT001,PLANT002"
  }'
```

---

### 6. Re-ingest Kedua Platform Sekaligus

Re-ingest iSolarCloud dan FusionSolar dalam satu run:

```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ stg_fusionsolar__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-05",
    "reingest_end_date": "2025-11-02",
    "reingest_ps_ids": "PS001,PS002",
    "reingest_plant_codes": "PLANT001"
  }'
```

---

## 📊 Yang Ter-refresh Otomatis

Ketika Anda menjalankan re-ingestion dengan `+` selector, semua downstream models akan otomatis ter-refresh:

### Staging Layer
- ✅ `stg_isolarcloud__perf_unpivoted` (iSolarCloud)
- ✅ `stg_fusionsolar__perf_unpivoted` (FusionSolar)

### Mart Layer (5-minute)
- ✅ `mart_inverter_performance_5min`
- ✅ `mart_meter_performance_5min`
- ✅ `mart_sensor_measurements_5min`

### Mart Layer (Daily)
- ✅ `mart_site_performance_daily` (jika depend pada 5min marts)
- ✅ `mart_inverter_performance_daily` (jika depend pada 5min marts)
- ✅ `mart_string_performance_daily` (jika depend pada 5min marts)

**Catatan:** Daily marts akan ter-refresh hanya jika mereka depend pada 5min marts yang di-re-ingest.

---

## 🔍 Cara Cek Site/Plant IDs

### iSolarCloud - Cek ps_id

```sql
SELECT 
    ps_id, 
    ps_name,
    install_date,
    online_status
FROM raw.isolarcloud_power_stations
ORDER BY ps_name;
```

### FusionSolar - Cek plant_code

```sql
SELECT 
    plant_code, 
    plant_name,
    install_date
FROM raw.fusionsolar_plants
ORDER BY plant_name;
```

---

## ⚙️ Parameter yang Tersedia

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `reingest_start_date` | String | Yes* | Tanggal mulai re-ingestion (YYYY-MM-DD) | `"2025-10-05"` |
| `reingest_end_date` | String | Yes* | Tanggal akhir re-ingestion (YYYY-MM-DD) | `"2025-11-02"` |
| `reingest_ps_ids` | String | No | Comma-separated list of ps_id (iSolarCloud) | `"PS001,PS002"` |
| `reingest_plant_codes` | String | No | Comma-separated list of plant_code (FusionSolar) | `"PLANT001"` |

*Required hanya jika ingin melakukan re-ingestion. Jika tidak specify, akan menggunakan incremental mode normal.

---

## 🛡️ Safety & Best Practices

### 1. Unique Key Protection
Semua models menggunakan `unique_key` yang memastikan:
- Data lama di-overwrite (bukan duplikasi)
- Re-ingestion aman untuk correction/update

### 2. Date Range Validation
Pastikan:
- `reingest_start_date <= reingest_end_date`
- Format date: `YYYY-MM-DD`
- End date adalah inclusive (data sampai end_date 23:59:59)

### 3. Performance Considerations
- **Re-ingest per site** lebih cepat daripada semua site
- Untuk date range besar (>30 hari), pertimbangkan batch per site
- Monitor execution time untuk optimasi

### 4. Testing
Sebelum re-ingest production data:
1. Test dengan date range kecil (1-2 hari)
2. Verify hasil di staging model dulu
3. Check row counts sebelum/after

---

## 📝 Contoh Use Cases

### Use Case 1: Fix Missing Data untuk 1 Site

**Scenario:** Site PS001 missing data untuk tanggal 10-15 Oktober 2025. Data sudah di-backfill di raw table.

**Solution:**
```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-10",
    "reingest_end_date": "2025-10-15",
    "reingest_ps_ids": "PS001"
  }'
```

### Use Case 2: Correct Data Quality Issue untuk Multiple Sites

**Scenario:** Ditemukan data quality issue untuk 3 sites dalam periode tertentu.

**Solution:**
```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-09-01",
    "reingest_end_date": "2025-09-30",
    "reingest_ps_ids": "PS001,PS002,PS003"
  }'
```

### Use Case 3: Full Month Re-ingest (All Sites)

**Scenario:** Perlu re-process seluruh data untuk bulan Oktober karena perubahan metric mapping.

**Solution:**
```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ stg_fusionsolar__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-01",
    "reingest_end_date": "2025-10-31"
  }'
```

---

## 🐛 Troubleshooting

### Error: "relation does not exist"
**Cause:** Model belum pernah di-run sebelumnya (tidak ada table untuk incremental comparison)

**Solution:** Run full refresh pertama kali:
```bash
dbt run --select stg_isolarcloud__perf_unpivoted --full-refresh
```

### Error: "invalid input syntax for type timestamp"
**Cause:** Format date salah

**Solution:** Pastikan format `YYYY-MM-DD` (contoh: `"2025-10-05"`)

### Re-ingestion tidak ter-apply
**Cause:** Vars tidak ter-pass dengan benar

**Solution:** 
- Check syntax JSON di `--vars`
- Pastikan tidak ada typo di parameter names
- Test dengan `dbt debug` untuk verify vars

### Data tidak ter-update
**Cause:** Unique key mismatch atau filter tidak match

**Solution:**
- Verify `ps_id` atau `plant_code` benar
- Check date range overlap dengan data existing
- Query staging model untuk verify filter bekerja

---

## 📚 Related Documentation

- [dbt Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt Vars](https://docs.getdbt.com/reference/dbt-jinja-functions/var)
- [dbt Selectors](https://docs.getdbt.com/reference/node-selection/syntax)

---

## ✅ Summary

**Normal Run:**
```bash
dbt run
```

**Re-ingest All Sites:**
```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{"reingest_start_date": "YYYY-MM-DD", "reingest_end_date": "YYYY-MM-DD"}'
```

**Re-ingest Per Site:**
```bash
dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "YYYY-MM-DD",
    "reingest_end_date": "YYYY-MM-DD",
    "reingest_ps_ids": "PS001,PS002"
  }'
```

---

**Last Updated:** 2025-01-XX  
**Maintained By:** Data Engineering Team

