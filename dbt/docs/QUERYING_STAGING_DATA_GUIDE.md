# Panduan Query Data dari Staging Tables

## 1. Dari Mana DATA_FLOW_SUMMARY.md Mendapat Jumlah Data per Device?

File `DATA_FLOW_SUMMARY.md` mendapatkan jumlah data per device type (inverter, sensor, meter) dengan cara **query staging tables yang di-join dengan device tables** untuk filter berdasarkan `device_type` atau `dev_type_id`.

### Contoh Query untuk Mendapat Jumlah Data per Device Type

#### A. FusionSolar - Hitung Data per Device Type

```sql
-- Hitung jumlah data inverter di staging
SELECT 
    'Inverter' as device_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT p.dev_id) as unique_devices,
    COUNT(DISTINCT p.metric_id) as unique_metrics,
    MIN(p.timestamp) as min_timestamp,
    MAX(p.timestamp) as max_timestamp
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
    ON p.dev_id = d.dev_id
WHERE d.dev_type_id = 1  -- Inverter

UNION ALL

-- Hitung jumlah data sensor di staging
SELECT 
    'Sensor' as device_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT p.dev_id) as unique_devices,
    COUNT(DISTINCT p.metric_id) as unique_metrics,
    MIN(p.timestamp) as min_timestamp,
    MAX(p.timestamp) as max_timestamp
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
    ON p.dev_id = d.dev_id
WHERE d.dev_type_id = 10  -- Sensor/Meteo Station

UNION ALL

-- Hitung jumlah data meter di staging
SELECT 
    'Meter' as device_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT p.dev_id) as unique_devices,
    COUNT(DISTINCT p.metric_id) as unique_metrics,
    MIN(p.timestamp) as min_timestamp,
    MAX(p.timestamp) as max_timestamp
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
    ON p.dev_id = d.dev_id
WHERE d.dev_type_id = 17  -- Meter
```

#### B. iSolarCloud - Hitung Data per Device Type

```sql
-- Hitung jumlah data inverter di staging
SELECT 
    'Inverter' as device_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT p.device_ps_key) as unique_devices,
    COUNT(DISTINCT p.metric_id) as unique_metrics,
    MIN(p.timestamp) as min_timestamp,
    MAX(p.timestamp) as max_timestamp
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_isolarcloud__devices" d 
    ON p.device_ps_key = d.device_ps_key
WHERE d.device_type = 1  -- Inverter

UNION ALL

-- Hitung jumlah data sensor di staging
SELECT 
    'Sensor' as device_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT p.device_ps_key) as unique_devices,
    COUNT(DISTINCT p.metric_id) as unique_metrics,
    MIN(p.timestamp) as min_timestamp,
    MAX(p.timestamp) as max_timestamp
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_isolarcloud__devices" d 
    ON p.device_ps_key = d.device_ps_key
WHERE d.device_type = 5  -- Sensor/Meteo Station

UNION ALL

-- Hitung jumlah data meter di staging
SELECT 
    'Meter' as device_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT p.device_ps_key) as unique_devices,
    COUNT(DISTINCT p.metric_id) as unique_metrics,
    MIN(p.timestamp) as min_timestamp,
    MAX(p.timestamp) as max_timestamp
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_isolarcloud__devices" d 
    ON p.device_ps_key = d.device_ps_key
WHERE d.device_type = 7  -- Meter
```

---

## 2. Cara Query Inverter Temperature untuk Device di Sebuah Site

Karena staging tables **tidak memiliki kolom device_type langsung**, kita perlu **JOIN dengan device tables** untuk filter berdasarkan device type.

### Device Type Codes Reference

#### FusionSolar
- `dev_type_id = 1`: Inverter
- `dev_type_id = 10`: Sensor/Meteo Station
- `dev_type_id = 17`: Meter

#### iSolarCloud
- `device_type = 1`: Inverter
- `device_type = 5`: Sensor/Meteo Station
- `device_type = 7`: Meter

### Metric ID Reference

Untuk mengetahui `metric_id` yang benar untuk setiap metric, cek file `dbt/seeds/seed_metric_mapper.csv`:

```sql
-- Contoh: Cari semua temperature metrics untuk inverter
SELECT 
    platform,
    metric_id,
    metric_name,
    device_type,
    metric_group,
    metric_unit
FROM "MMSR"."staging"."seed_metric_mapper"
WHERE metric_name ILIKE '%temperature%'
    AND device_type = 1  -- Inverter
ORDER BY platform, metric_id;
```

**Temperature Metrics:**
- **FusionSolar Inverter**: `metric_id = 'temperature'`
- **iSolarCloud Inverter**: `metric_id IN ('p4', 'p33', 'p34', 'p35', 'p36', 'p37', 'p38')`
  - `p4`: Internal Air Temperature
  - `p33-p38`: Module 1-6 Temperature

### Contoh Query: Inverter Temperature untuk Site Tertentu

#### A. FusionSolar - Inverter Temperature per Site

```sql
-- Query inverter temperature untuk semua device di sebuah site
SELECT 
    p.timestamp,
    p.dev_id as device_id,
    d.dev_name as device_name,
    p.plant_name as site_name,
    p.metric_id,
    p.metric_value as temperature_celsius
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
    ON p.dev_id = d.dev_id
WHERE d.dev_type_id = 1  -- Filter: Hanya Inverter
    AND p.metric_id = 'temperature'  -- Filter: Hanya metric temperature
    AND p.plant_code = 'YOUR_PLANT_CODE'  -- Ganti dengan plant_code site yang diinginkan
    AND p.timestamp >= '2025-01-01'::timestamp  -- Sesuaikan tanggal
    AND p.timestamp < '2025-01-02'::timestamp
ORDER BY p.timestamp, d.dev_name;
```

#### B. iSolarCloud - Inverter Temperature per Site

```sql
-- Query inverter temperature untuk semua device di sebuah site
-- Note: iSolarCloud memiliki beberapa temperature metrics:
--   - p4: Internal Air Temperature
--   - p33-p38: Module 1-6 Temperature
SELECT 
    p.timestamp,
    p.device_ps_key as device_id,
    d.device_name,
    s.ps_name as site_name,
    p.metric_id,
    CASE 
        WHEN p.metric_id = 'p4' THEN 'Internal Air Temperature'
        WHEN p.metric_id = 'p33' THEN 'Module 1 Temperature'
        WHEN p.metric_id = 'p34' THEN 'Module 2 Temperature'
        WHEN p.metric_id = 'p35' THEN 'Module 3 Temperature'
        WHEN p.metric_id = 'p36' THEN 'Module 4 Temperature'
        WHEN p.metric_id = 'p37' THEN 'Module 5 Temperature'
        WHEN p.metric_id = 'p38' THEN 'Module 6 Temperature'
    END as temperature_type,
    p.metric_value as temperature_celsius
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_isolarcloud__devices" d 
    ON p.device_ps_key = d.device_ps_key
JOIN "MMSR"."staging"."stg_isolarcloud__sites" s 
    ON d.ps_id = s.ps_id
WHERE d.device_type = 1  -- Filter: Hanya Inverter
    AND p.metric_id IN ('p4', 'p33', 'p34', 'p35', 'p36', 'p37', 'p38')  -- Filter: Temperature metrics untuk iSolarCloud inverter
    AND s.ps_id = 'YOUR_PS_ID'  -- Ganti dengan ps_id site yang diinginkan
    AND p.timestamp >= '2025-01-01'::timestamp  -- Sesuaikan tanggal
    AND p.timestamp < '2025-01-02'::timestamp
ORDER BY p.timestamp, d.device_name, p.metric_id;
```

#### C. Unified Query - Gabungkan FusionSolar dan iSolarCloud

```sql
-- Query unified untuk inverter temperature dari kedua platform
WITH fusionsolar_temp AS (
    SELECT 
        'fusionsolar' as system,
        p.timestamp,
        p.dev_id as device_id,
        d.dev_name as device_name,
        p.plant_name as site_name,
        p.metric_value as temperature_celsius
    FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
    JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
        ON p.dev_id = d.dev_id
    WHERE d.dev_type_id = 1  -- Inverter
        AND p.metric_id = 'temperature'
        AND p.plant_code = 'YOUR_PLANT_CODE'  -- Ganti dengan plant_code
        AND p.timestamp >= '2025-01-01'::timestamp
        AND p.timestamp < '2025-01-02'::timestamp
),
isolarcloud_temp AS (
    SELECT 
        'isolarcloud' as system,
        p.timestamp,
        p.device_ps_key as device_id,
        d.device_name,
        s.ps_name as site_name,
        p.metric_value as temperature_celsius
    FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
    JOIN "MMSR"."staging"."stg_isolarcloud__devices" d 
        ON p.device_ps_key = d.device_ps_key
    JOIN "MMSR"."staging"."stg_isolarcloud__sites" s 
        ON d.ps_id = s.ps_id
    WHERE d.device_type = 1  -- Inverter
        AND p.metric_id IN ('p4', 'p33', 'p34', 'p35', 'p36', 'p37', 'p38')  -- Temperature metrics untuk iSolarCloud inverter
        AND s.ps_id = 'YOUR_PS_ID'  -- Ganti dengan ps_id
        AND p.timestamp >= '2025-01-01'::timestamp
        AND p.timestamp < '2025-01-02'::timestamp
)
SELECT * FROM fusionsolar_temp
UNION ALL
SELECT * FROM isolarcloud_temp
ORDER BY system, timestamp, device_name;
```

---

## 3. Apakah Perlu Menambahkan device_type ke Staging?

### Rekomendasi: **TIDAK PERLU** menambahkan device_type ke staging tables

**Alasan:**

1. **Normalisasi Database**: Device type sudah ada di device tables (`stg_fusionsolar__devices` dan `stg_isolarcloud__devices`). Menambahkan device_type ke staging akan menyebabkan **data redundancy** dan melanggar prinsip normalisasi.

2. **Staging sudah memiliki JOIN yang efisien**: Staging tables sudah memiliki foreign key ke device tables:
   - `stg_fusionsolar__perf_unpivoted.dev_id` → `stg_fusionsolar__devices.dev_id`
   - `stg_isolarcloud__perf_unpivoted.device_ps_key` → `stg_isolarcloud__devices.device_ps_key`

3. **Query dengan JOIN sudah optimal**: PostgreSQL optimizer sudah cukup baik untuk handle JOIN ini, terutama jika ada index yang tepat.

4. **Konsistensi dengan Architecture**: Model intermediate dan mart juga menggunakan pattern yang sama (JOIN dengan device tables), jadi konsisten dengan arsitektur yang ada.

5. **Tidak semua metric masuk mart**: Memang benar tidak semua metric masuk mart (hanya yang `used = 'yes'` di `seed_metric_mapper`), tapi ini bukan alasan untuk menambahkan device_type ke staging. Query langsung dari staging tetap bisa dilakukan dengan JOIN.

### Kapan Boleh Menambahkan device_type ke Staging?

Hanya jika:
- Query performance menjadi bottleneck yang signifikan
- Volume data sangat besar dan JOIN menjadi sangat lambat
- Ada requirement untuk query staging tanpa JOIN (tapi ini jarang terjadi)

**Tapi untuk sekarang, pattern JOIN sudah cukup baik dan mengikuti best practices.**

---

## 4. Contoh Query Lainnya

### Query: Semua Metric Inverter untuk Site Tertentu

```sql
-- FusionSolar: Semua metric inverter untuk site tertentu
SELECT 
    p.timestamp,
    p.dev_id,
    d.dev_name,
    p.plant_name as site_name,
    p.metric_id,
    p.metric_value
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
    ON p.dev_id = d.dev_id
WHERE d.dev_type_id = 1  -- Inverter
    AND p.plant_code = 'YOUR_PLANT_CODE'
    AND p.timestamp >= '2025-01-01'::timestamp
    AND p.timestamp < '2025-01-02'::timestamp
ORDER BY p.timestamp, d.dev_name, p.metric_id;
```

### Query: List Semua Metric yang Tersedia untuk Inverter

```sql
-- FusionSolar: List semua metric_id yang ada untuk inverter
SELECT DISTINCT
    p.metric_id,
    COUNT(*) as record_count,
    COUNT(DISTINCT p.dev_id) as device_count
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
    ON p.dev_id = d.dev_id
WHERE d.dev_type_id = 1  -- Inverter
GROUP BY p.metric_id
ORDER BY record_count DESC;
```

### Query: Average Temperature per Device per Day

```sql
-- FusionSolar: Rata-rata temperature per device per hari
SELECT 
    DATE(p.timestamp) as date_key,
    p.dev_id,
    d.dev_name,
    p.plant_name as site_name,
    AVG(p.metric_value) as avg_temperature_celsius,
    MIN(p.metric_value) as min_temperature_celsius,
    MAX(p.metric_value) as max_temperature_celsius,
    COUNT(*) as reading_count
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_fusionsolar__devices" d 
    ON p.dev_id = d.dev_id
WHERE d.dev_type_id = 1  -- Inverter
    AND p.metric_id = 'temperature'
    AND p.plant_code = 'YOUR_PLANT_CODE'
    AND p.timestamp >= '2025-01-01'::timestamp
    AND p.timestamp < '2025-02-01'::timestamp
GROUP BY DATE(p.timestamp), p.dev_id, d.dev_name, p.plant_name
ORDER BY date_key, d.dev_name;
```

---

## 5. Tips Query Performance

1. **Gunakan Index**: Pastikan ada index pada:
   - `stg_fusionsolar__perf_unpivoted.dev_id`
   - `stg_fusionsolar__perf_unpivoted.timestamp`
   - `stg_fusionsolar__perf_unpivoted.metric_id`
   - `stg_isolarcloud__perf_unpivoted.device_ps_key`
   - `stg_isolarcloud__perf_unpivoted.timestamp`
   - `stg_isolarcloud__perf_unpivoted.metric_id`

2. **Filter Early**: Selalu filter berdasarkan `device_type` dan `metric_id` di WHERE clause, bukan di SELECT.

3. **Limit Date Range**: Untuk query ad-hoc, selalu batasi date range untuk performa yang lebih baik.

4. **Gunakan Mart Tables untuk Reporting**: Jika data sudah ada di mart tables, gunakan mart tables karena sudah di-aggregate dan di-enrich.

---

## Summary

- ✅ **DATA_FLOW_SUMMARY.md** mendapatkan jumlah data dari query staging yang di-join dengan device tables
- ✅ **Query inverter temperature** perlu JOIN dengan device tables untuk filter `device_type = 1` (iSolarCloud) atau `dev_type_id = 1` (FusionSolar)
- ✅ **TIDAK PERLU** menambahkan device_type ke staging - pattern JOIN sudah optimal dan mengikuti best practices
- ✅ **Query langsung dari staging** tetap bisa dilakukan untuk metric yang tidak masuk mart, dengan JOIN ke device tables
