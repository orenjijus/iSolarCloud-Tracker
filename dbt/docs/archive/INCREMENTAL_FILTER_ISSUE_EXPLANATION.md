# Penjelasan: Kenapa Data FusionSolar Tidak Masuk saat dbt run Normal

## 🔍 Masalah yang Terjadi

Ketika menjalankan `dbt run` normal:
- ✅ Data **iSolarCloud** untuk 2025-12-17 **masuk** ke `mart_site_performance_daily`
- ❌ Data **FusionSolar** untuk 2025-12-17 **tidak masuk** ke `mart_site_performance_daily`
- ✅ Data staging untuk **kedua sistem** berhasil di-run

Tapi ketika re-run dengan `--vars`:
- ✅ Data **kedua sistem** masuk ke `mart_site_performance_daily`

## 🎯 Root Cause

Masalahnya ada di **incremental filter** di `mart_site_performance_daily.sql`:

```sql
-- Line 478
AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
```

### Bagaimana Filter Ini Bekerja

1. **Saat `dbt run` normal:**
   - Filter menggunakan `MAX(date_key)` dari tabel `mart_site_performance_daily`
   - Jika sudah ada data dengan `date_key > 2025-12-17` (misalnya 2025-12-18), maka:
     - `MAX(date_key)` = 2025-12-18 (atau lebih baru)
     - Filter: `date_key > 2025-12-18`
     - **Semua data dengan date_key <= 2025-12-18 akan di-SKIP**

2. **Kenapa iSolarCloud bisa masuk tapi FusionSolar tidak?**

   **Kemungkinan 1: Execution Order**
   - dbt menjalankan models berdasarkan dependency graph
   - iSolarCloud mungkin di-run lebih dulu dalam dependency chain
   - Saat iSolarCloud di-run, `MAX(date_key)` masih < 2025-12-17, jadi data masuk
   - FusionSolar di-run setelahnya, tapi saat itu `MAX(date_key)` sudah >= 2025-12-17, jadi di-skip

   **Kemungkinan 2: Data Existing**
   - Sudah ada data iSolarCloud dengan date_key yang lebih kecil sebelumnya
   - Tapi tidak ada data FusionSolar dengan date_key yang lebih kecil
   - Saat run, iSolarCloud bisa masuk karena ada "gap" di data, tapi FusionSolar tidak

   **Kemungkinan 3: Data di Intermediate Layer**
   - Data FusionSolar mungkin tidak sampai ke `fact_site_calculations_5min` karena filter incremental di layer intermediate
   - Jika `fact_site_calculations_5min` tidak punya data FusionSolar untuk 2025-12-17, maka `mart_site_performance_daily` juga tidak akan punya

## 📊 Bagaimana dbt run Bekerja

### Execution Order

dbt menjalankan models berdasarkan **dependency graph**, bukan urutan file:

```
1. dimensions (dim_date_generated, dim_assets)
2. staging (stg_fusionsolar__perf_unpivoted, stg_isolarcloud__perf_unpivoted)
   ↓
3. mart 5min (mart_inverter_performance_5min, mart_meter_performance_5min, mart_sensor_measurements_5min)
   ↓
4. facts 5min (fact_sensor_calculations_5min, fact_inverter_calculations_5min, fact_site_calculations_5min)
   ↓
5. mart daily (mart_sensor_daily, mart_site_performance_daily)
```

**Penting:** dbt bisa menjalankan models yang tidak depend satu sama lain secara **parallel** atau dalam urutan yang berbeda, tergantung dependency graph.

### Incremental Filter Behavior

Setiap model incremental memiliki filter sendiri:

```sql
-- stg_fusionsolar__perf_unpivoted
WHERE h.collect_time > (SELECT MAX(timestamp) FROM {{ this }})

-- mart_meter_performance_5min
-- Filter di CTE per-system, tidak di final SELECT

-- fact_site_calculations_5min
WHERE f.timestamp > (SELECT MAX(timestamp) FROM {{ this }})

-- mart_site_performance_daily
AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
```

**Masalah:** Filter di `mart_site_performance_daily` menggunakan `MAX(date_key)` yang **global** (semua sistem), bukan per-system.

## 🔧 Solusi

### Solusi 1: Gunakan --vars untuk Re-process (Recommended)

Untuk memastikan data masuk, gunakan `--vars`:

```bash
dbt run --select mart_site_performance_daily+ \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

Ini akan override filter incremental dan force re-process tanggal tersebut.

### Solusi 2: Fix Incremental Filter (Long-term)

Filter incremental di `mart_site_performance_daily` seharusnya menggunakan filter per-system atau menggunakan `timestamp` instead of `date_key`:

**Current (Problematic):**
```sql
AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
```

**Better Approach:**
```sql
-- Option 1: Per-system filter
AND (
    (fsc.system = 'fusionsolar' AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }} WHERE system = 'fusionsolar'))
    OR
    (fsc.system = 'isolarcloud' AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }} WHERE system = 'isolarcloud'))
)

-- Option 2: Use timestamp instead of date_key
AND fsc.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
```

Tapi ini memerlukan perubahan model, jadi lebih aman gunakan `--vars` untuk re-process.

## 📝 Checklist untuk Debug

Jika data tidak masuk, cek:

1. **Staging Layer:**
   ```sql
   SELECT COUNT(*) FROM staging.stg_fusionsolar__perf_unpivoted 
   WHERE DATE(timestamp) = '2025-12-17';
   ```

2. **Mart 5min Layer:**
   ```sql
   SELECT COUNT(*) FROM mart.mart_meter_performance_5min 
   WHERE system = 'fusionsolar' AND date_key = '2025-12-17';
   ```

3. **Fact Layer:**
   ```sql
   SELECT COUNT(*) FROM mart.fact_site_calculations_5min 
   WHERE system = 'fusionsolar' AND date_key = '2025-12-17';
   ```

4. **Final Layer:**
   ```sql
   SELECT COUNT(*) FROM mart.mart_site_performance_daily 
   WHERE system = 'fusionsolar' AND date_key = '2025-12-17';
   ```

5. **Check MAX(date_key):**
   ```sql
   SELECT MAX(date_key) as max_date, system, COUNT(*) 
   FROM mart.mart_site_performance_daily 
   GROUP BY system;
   ```

## 🎯 Kesimpulan

**Masalah:** Filter incremental di `mart_site_performance_daily` menggunakan `MAX(date_key)` global, yang bisa skip data jika sudah ada data dengan date_key lebih besar.

**Solusi Immediate:** Gunakan `--vars` untuk re-process tanggal tertentu:
```bash
dbt run --select mart_site_performance_daily+ \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

**Solusi Long-term:** Perbaiki filter incremental untuk menggunakan per-system filter atau timestamp-based filter.

## 📚 Related Documentation

- [TROUBLESHOOTING_FUSIONSOLAR_DATA.md](./TROUBLESHOOTING_FUSIONSOLAR_DATA.md)
- [REINGESTION_WORKFLOW.md](./REINGESTION_WORKFLOW.md)

