# Data Flow Summary - Sensor, Meter & Inverter

## Status: SUCCESS ✅

Data sensor, meter, dan inverter sudah berhasil masuk dari staging → intermediate → mart tables.

---

## Data Flow Analysis

### 1. Staging Layer
- **stg_fusionsolar__perf_unpivoted**: 328.98M rows
  - Inverter data: 271.58M rows (82 assets, 88 metrics)
  - Sensor data: 13.36M rows
  - Meter data: 46.27M rows
  
- **stg_isolarcloud__perf_unpivoted**: 177.87M rows
  - Inverter data: 158.36M rows (90 assets, 52 metrics)
  - Sensor data: 8.38M rows
  - Meter data: 11.11M rows

### 2. Intermediate Layer
- **int_inverters_unified_5min**: Materialized as INCREMENTAL
  - iSolarCloud: 158.36M rows (90 assets, 52 metrics)
  - FusionSolar: 271.58M rows (82 assets, 88 metrics)
  - Combines both sources with 5-minute aggregation
  - Device type filtering: device_type = 1 (iSolarCloud), dev_type_id = 1 (FusionSolar)

- Tidak ada intermediate khusus string (diklasifikasikan di mart via mapper)

- **int_sensors_unified**: 21.75M rows
  - FusionSolar: 13.36M rows
  - iSolarCloud: 8.38M rows
  
- **int_meters_unified**: 55.15M rows
  - FusionSolar: 46.27M rows
  - iSolarCloud: 11.11M rows

### 3. Mart Layer
- **mart_inverter_performance_5min**: 433.16M rows
  - iSolarCloud: 158.36M rows
    - 90 unique assets
    - 52 unique metrics
    - Period: 2024-10-10 16:15 → 2025-09-17 23:55
  
  - FusionSolar: 274.8M rows
    - 88 unique metrics
    - Period: 2024-05-01 05:10 → 2025-09-18 06:55

- **mart_inverter_performance_daily**: 604.48K rows
  - 90 unique assets
  - 110 unique metrics
  - Period: 2024-05-01 → 2025-09-18

- **mart_string_performance_5min**: Extracted from inverter data
  - String-level voltage and current measurements (metric_id LIKE 'p%')

- **mart_sensor_measurements_5min**: 21.75M rows
  - FusionSolar: 13.36M rows
    - 45 unique assets
    - 6 unique metrics
    - Period: 2024-04-30 23:00 → 2025-09-18 06:55
  
  - iSolarCloud: 8.38M rows
    - 48 unique assets
    - 7 unique metrics
    - Period: 2024-10-13 12:10 → 2025-09-17 23:55

- **mart_meter_performance_5min**: 57.38M rows
  - FusionSolar: 46.27M rows
    - 30 unique assets
    - 20 unique metrics
    - Period: 2024-04-30 23:00 → 2025-09-18 06:55
  
  - iSolarCloud: 11.11M rows
    - 54 unique assets
    - 6 unique metrics
    - Period: 2024-10-10 16:15 → 2025-09-17 23:55

---

## Issues Fixed

### 1. Device Type Mismatch
**Problem**: Device type codes salah di intermediate models
- **Sensors**: `device_type = 2` (wrong) → `device_type = 5` (correct for iSolarCloud)
- **Sensors**: `dev_type_id = 2` (wrong) → `dev_type_id = 10` (correct for FusionSolar)  
- **Meters**: `dev_type_id = 7` (wrong) → `dev_type_id = 17` (correct for FusionSolar)
- **Inverters**: No issues, correctly filtered with device_type = 1 (iSolarCloud) and dev_type_id = 1 (FusionSolar)

### 2. Invalid Values (NaN)
**Problem**: Data dengan nilai -nan, nan, infinity menyebabkan error casting
**Solution**: Added filter di staging models untuk skip invalid values sebelum casting ke numeric

### 3. Asset ID Mismatch
**Problem**: JOIN ke dim_assets gagal karena asset_id format berbeda
- **dim_assets** menggunakan: `FS_` untuk FusionSolar dan `ISO_` untuk iSolarCloud
- **mart** sebelumnya menggunakan: `FUS_` untuk FusionSolar
**Solution**: Update JOIN logic di mart models untuk gunakan prefix yang benar

---

## Database Location

Semua data tersimpan di database PostgreSQL dengan schema `public`:

### Tables
- `stg_fusionsolar__perf_unpivoted`
- `stg_isolarcloud__perf_unpivoted`
- `int_inverters_unified_5min`
- `int_sensors_unified`
- `int_meters_unified`
- `mart_inverter_performance_5min`
- `mart_inverter_performance_daily`
- `mart_string_performance_5min`
- `mart_sensor_measurements_5min`
- `mart_meter_performance_5min`
- `dim_assets`
- `dim_date_generated`

### Data Volume
- **Total Staging**: ~506M rows
- **Total Intermediate**: ~456M rows  
- **Total Mart**: ~512M rows

---

## Next Steps

1. Data sudah siap untuk reporting/visualization
2. Monitor incremental load performance
3. Consider partitioning jika volume data terus bertambah

---

## Summary

✅ **Staging**: 506M rows  
✅ **Intermediate**: 456M rows  
✅ **Mart**: 512M rows (includes inverter, sensor, meter, and string data)  
✅ **System field**: Berfungsi dengan benar  
✅ **All joins**: Berhasil  


**Total Execution Time**: ~52 minutes untuk full rebuild

---

## Inverter Data Flow (End‑to‑End)

### Source → Staging
- Raw JSONB (FusionSolar, iSolarCloud) → unpivot di staging:
  - `stg_fusionsolar__perf_unpivoted`
  - `stg_isolarcloud__perf_unpivoted`
- Karakteristik: incremental, unpivot JSONB → long format (`timestamp, device_ps_key, metric_id, metric_value`)

### Staging → Intermediate (Unify dua platform)
- `int_inverters_unified_5min` (incremental):
  - Filter perangkat inverter: `device_type = 1` (iSolarCloud), `dev_type_id = 1` (FusionSolar)
  - Join device/site untuk atribut (`device_name`, `site_name`)
  - Normalisasi waktu: `DATE_TRUNC('minute', timestamp)` → `timestamp_5min`
  - Agregasi 5‑menit: `AVG(metric_value)` per `timestamp_5min, device_ps_key, metric_id`
  - Tambah kolom `system` untuk asal data

### Intermediate → Mart
- `mart_inverter_performance_5min` (incremental):
  - Konsumsi dari `int_inverters_unified_5min`
  - Enrichment & klasifikasi metrik via `seed_metric_mapper` untuk menentukan metrik level inverter vs string
  - JOIN `dim_assets`, `dim_date_generated`; pembersihan nilai invalid/NULL sesuai mapper

### Catatan Volume (acuan sizing terbaru)
- `mart_inverter_performance_5min`: ~433M rows, ~52 GB

---

## String Data Flow (End‑to‑End)

### Sumber & Klasifikasi
- String‑level akan ditentukan oleh `seed_metric_mapper` (bukan asumsi prefix). Mapper memetakan `metric_id` ke kategori level metrik (inverter vs string) dan properti lain (nama/unit).

### Tanpa Intermediate Khusus
- Tidak ada lagi `int_strings_unified_5min`. String akan diturunkan langsung di mart dari `int_inverters_unified_5min` menggunakan mapping.

### Mart
- `mart_string_performance_5min` (incremental/table sesuai konfigurasi berjalan):
  - Sumber: `int_inverters_unified_5min`
  - Filter metrik string menggunakan `seed_metric_mapper` (kategori = string)
  - Enrichment: JOIN `dim_assets`, `dim_date_generated`, mapper (unified naming/unit)

### Batasan Penting (kondisi & rencana)
- Kondisi saat ini: `mart_inverter_performance_5min` memuat sebagian metrik string.
- Rencana: setelah seed mapper baru dimuat, mart inverter akan hanya memuat metrik kategori inverter; mart string hanya memuat metrik kategori string.

### Catatan Volume (acuan sizing terbaru)
- `mart_string_performance_5min`: ~333M rows, ~46 GB

---

## Objek yang Terlibat (Ringkasan)

- Staging:
  - `stg_fusionsolar__perf_unpivoted`, `stg_isolarcloud__perf_unpivoted`
- Intermediate (dipertahankan sesuai arahan audit):
  - `int_inverters_unified_5min`, `int_meters_unified`, `int_sensors_unified`
- Mart:
  - `mart_inverter_performance_5min`, `mart_meter_performance_5min`, `mart_sensor_measurements_5min`, `mart_string_performance_5min`
- Seeds:
  - `seed_metric_mapper` (akan diperbarui untuk klasifikasi inverter vs string)

Referensi ukuran: lihat `docs/DATABASE_SIZING_DETAIL.md` untuk angka terbaru.

---

## Rencana Rebuild (singkat)

1) Seeds
- Perbarui `seed_metric_mapper` untuk menandai kategori metrik: inverter vs string; sertakan nama/unit/kelompok jika perlu.
- Jalankan: `dbt seed`.

2) Intermediate
- Pastikan `int_inverters_unified_5min` tetap incremental dan stabil.

3) Marts (full refresh terarah)
- `mart_inverter_performance_5min`: terapkan filter kategori = inverter via mapper; full‑refresh jika perlu menertibkan historis.
- `mart_string_performance_5min`: turunkan dari `int_inverters_unified_5min`, filter kategori = string via mapper; full‑refresh.

4) Validasi
- Cek tidak ada metrik string di mart inverter dan tidak ada metrik inverter di mart string.
- Sampling beberapa `metric_id` kritikal untuk verifikasi nama/unit.

