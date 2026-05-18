# Analisis Kondisi Data: Staging hingga Mart (Report)

Dokumen ini mendeskripsikan **kerangka analisis** kondisi data dari raw/staging sampai mart untuk keperluan report dan monitoring. Gunakan bersama file query: `queries/analysis_data_condition_staging_to_mart.sql`.

---

## 1. Tujuan Report

- **Snapshot kondisi data** saat ini di setiap layer (raw → staging → dimensions → mart → facts).
- **Dasar untuk report** ke stakeholder: volume, coverage tanggal, jumlah asset/site, dan kualitas data.
- **Baseline untuk monitoring** setelah ETL/dbt run atau setelah reingestion.

---

## 2. Arsitektur Alur Data (Ringkas)

```
Raw (PostgreSQL: raw.*)
  ├── fusionsolar_historical_data, fusionsolar_devices, fusionsolar_plants
  └── isolarcloud_historical_data, isolarcloud_devices, isolarcloud_power_stations
        ↓
Staging (MMSR.staging)
  ├── stg_fusionsolar__perf_unpivoted, stg_fusionsolar__devices, stg_fusionsolar__sites
  └── stg_isolarcloud__perf_unpivoted, stg_isolarcloud__devices, stg_isolarcloud__sites
        ↓
Dimensions (MMSR.dimensions)
  ├── dim_assets
  └── dim_date_generated
        ↓
Mart 5 menit (MMSR.mart) — langsung dari staging + seed_metric_mapper + dimensions
  ├── mart_inverter_performance_5min
  ├── mart_sensor_measurements_5min
  └── mart_meter_performance_5min
        ↓
Mart daily & lainnya (MMSR.mart)
  ├── mart_site_performance_daily  (dari mart_meter, mart_sensor_daily, fact_site, seeds)
  ├── mart_sensor_daily            (dari mart_sensor_measurements_5min)
  ├── mart_inverter_yield_daily    (dari mart_inverter_performance_5min)
  ├── mart_simulation_targets_daily, mart_site_performance_monthly, mart_weekly_log, dll.
        ↓
Facts (MMSR.mart) — calculated metrics
  ├── fact_inverter_calculations_5min
  ├── fact_sensor_calculations_5min
  └── fact_site_calculations_5min
```

**Catatan:** Saat ini tidak ada layer intermediate (int_*) di codebase; mart 5 menit dibangun langsung dari staging + device tables + seed_metric_mapper.

---

## 3. Cara Menjalankan Analisis

1. **Jalankan script SQL**  
   File: `queries/analysis_data_condition_staging_to_mart.sql`  
   Jalankan per blok (1–14) di client PostgreSQL (DBeaver, psql, dsb.) terhadap database **MMSR**.  
   Blok 13–14: data size (storage) per tabel dan per schema.  
   Sesuaikan schema jika berbeda (mis. `staging`/`mart`/`dimensions` di bawah database lain).

2. **Simpan hasil**  
   Export hasil tiap blok ke CSV/Excel atau salin ke bagian "Hasil Analisis" di bawah (atau ke dokumen terpisah).

3. **Isi report**  
   Isi tabel ringkasan di Section 4 dan 5 dengan angka dari hasil query; tambah catatan/insight di Section 6.

---

## 4. Hasil Analisis (Terisi dari query)

*Diisi dari hasil jalankan `queries/analysis_data_condition_staging_to_mart.sql` via MCP Postgres. Tanggal snapshot: 2026-01-27.*

### 4.1 Raw Layer

| Object                     | total_rows | min_timestamp | max_timestamp | unique_devices |
|---------------------------|------------|---------------|---------------|----------------|
| FusionSolar historical    | 13,191,064 | 2024-04-30 23:00 | 2026-01-29 23:55 | 201 |
| iSolarCloud historical    | 17,110,847 | 2024-10-10 16:15 | 2026-01-29 23:55 | 293 |
| FusionSolar devices        | 227        | -             | -             | -              |
| iSolarCloud devices        | 315        | -             | -             | -              |

### 4.2 Staging Layer

| Object                           | total_rows | min_timestamp | max_timestamp | unique_devices | unique_metrics |
|---------------------------------|------------|---------------|---------------|----------------|----------------|
| stg_fusionsolar__perf_unpivoted | 508,777,855 | 2024-04-30 23:00 | 2026-01-29 23:55 | 201 | 101 |
| stg_isolarcloud__perf_unpivoted | 348,955,279 | 2024-10-10 16:15 | 2026-01-29 23:55 | 293 | 71 |

### 4.3 Staging per Device Type

| Object (contoh)     | total_rows | unique_devices | unique_metrics | min_ts | max_ts |
|--------------------|------------|----------------|----------------|--------|--------|
| FusionSolar_Inverter | 421,497,906 | 103 | 88 | 2024-05-01 05:10 | 2026-01-29 18:40 |
| FusionSolar_Sensor   | 19,932,551 | 60 | 6 | 2024-04-30 23:00 | 2026-01-29 23:55 |
| FusionSolar_Meter    | 67,347,398 | 38 | 20 | 2024-04-30 23:00 | 2026-01-29 23:55 |
| iSolarCloud_Inverter | 306,553,312 | 117 | 52 | 2024-10-10 16:15 | 2026-01-29 23:55 |
| iSolarCloud_Sensor   | 14,056,986 | 66 | 7 | 2024-10-13 12:10 | 2026-01-29 23:55 |
| iSolarCloud_Meter    | 28,030,331 | 100 | 12 | 2024-10-10 16:15 | 2026-01-29 23:55 |

### 4.4 Dimensions

| Object           | total_rows | unique_assets (untuk dim_assets) |
|------------------|------------|----------------------------------|
| dim_assets       | 556        | 556                              |
| dim_date_generated | 2,930    | -                                |

### 4.5 Mart 5 Menit

| Object                         | total_rows | min_timestamp | max_timestamp | unique_assets | unique_metrics |
|--------------------------------|------------|---------------|---------------|---------------|----------------|
| mart_inverter_performance_5min  | 52,339,748 | 2024-05-01 05:10 | 2026-01-29 23:55 | 220 | 9 |
| mart_sensor_measurements_5min   | 30,397,075 | 2024-04-30 23:00 | 2026-01-29 23:55 | 126 | 11 |
| mart_meter_performance_5min    | 48,436,481 | 2024-04-30 23:00 | 2026-01-29 23:55 | 135 | 12 |

### 4.6 Mart 5 Menit per System

| Mart name                      | system     | total_rows | min_timestamp | max_timestamp | unique_assets | unique_metrics |
|--------------------------------|------------|------------|---------------|---------------|---------------|----------------|
| mart_inverter_performance_5min | fusionsolar | 25,132,850 | 2024-05-01 05:10 | 2026-01-29 18:40 | 103 | 5 |
| mart_inverter_performance_5min | isolarcloud | 27,206,898 | 2024-10-10 16:15 | 2026-01-29 23:55 | 117 | 4 |
| mart_sensor_measurements_5min  | fusionsolar | 16,340,089 | 2024-04-30 23:00 | 2026-01-29 23:55 | 60 | 4 |
| mart_sensor_measurements_5min  | isolarcloud | 14,056,986 | 2024-10-13 12:10 | 2026-01-29 23:55 | 66 | 7 |
| mart_meter_performance_5min   | fusionsolar | 27,790,757 | 2024-04-30 23:00 | 2026-01-29 23:55 | 38 | 6 |
| mart_meter_performance_5min   | isolarcloud | 20,645,724 | 2024-10-10 16:15 | 2026-01-29 23:55 | 97 | 6 |

### 4.7 Mart Daily

| Object                        | total_rows | min_date | max_date | unique_sites/assets |
|-------------------------------|------------|----------|----------|----------------------|
| mart_site_performance_daily   | 5,290      | 2024-04-30 | 2026-01-29 | 22 sites |
| mart_sensor_daily             | 26,247     | 2024-04-30 | 2026-01-29 | 99 assets |
| mart_inverter_yield_daily     | 55,418     | 2024-05-01 | 2026-01-29 | 203 assets |
| mart_simulation_targets_daily | 4,198      | 2025-01-01 | 2025-12-31 | 15 sites |

### 4.8 Mart Monthly / Weekly

| Object                       | total_rows | unique_sites (jika ada) |
|-----------------------------|------------|--------------------------|
| mart_site_performance_monthly | 186       | 22                       |
| mart_site_kpi_monthly       | 143        | -                        |
| mart_simulation_targets_monthly | 138    | -                        |
| mart_weekly_log             | 4          | -                        |

### 4.9 Facts (Calculated Metrics)

| Object                         | total_rows | min_timestamp | max_timestamp | unique_assets/sites |
|--------------------------------|------------|---------------|---------------|----------------------|
| fact_inverter_calculations_5min | 11,347,469 | 2024-05-01 05:10 | 2026-01-29 23:55 | 219 |
| fact_sensor_calculations_5min  | 6,385,935  | 2024-04-30 23:00 | 2026-01-29 23:55 | 89 |
| fact_site_calculations_5min     | 1,237,685 | 2024-05-01 05:10 | 2026-01-29 23:55 | 23 sites |

### 4.10 Data Size (Storage)

**Per tabel** (blok query 13): total size = tabel + index. *Catatan: Beberapa tabel besar (raw historical, staging perf_unpivoted, mart inverter/meter 5min) dapat tampil kecil di `pg_total_relation_size` jika menggunakan Hyperscale/foreign storage.*

| schema_name | table_name | total_size_pretty | total_size_bytes |
|-------------|------------|-------------------|------------------|
| mart        | mart_sensor_measurements_5min | 6750 MB | 7,078,027,264 |
| mart        | fact_inverter_calculations_5min | 2748 MB | 2,881,126,400 |
| mart        | fact_sensor_calculations_5min | 1617 MB | 1,695,711,232 |
| mart        | fact_site_calculations_5min | 245 MB | 256,901,120 |
| mart        | mart_inverter_yield_daily | 14 MB | 14,295,040 |
| mart        | mart_sensor_daily | 8872 kB | 9,084,928 |
| mart        | mart_site_performance_daily | 2480 kB | 2,539,520 |
| raw         | fusionsolar_historical_data | 24 kB | 24,576 |
| raw         | isolarcloud_historical_data | 24 kB | 24,576 |
| staging     | stg_fusionsolar__perf_unpivoted | 16 kB | 16,384 |
| staging     | stg_isolarcloud__perf_unpivoted | 16 kB | 16,384 |
| mart        | mart_inverter_performance_5min | 32 kB | 32,768 |
| mart        | mart_meter_performance_5min | 32 kB | 32,768 |
| …           | (lainnya lihat hasil query 13) | | |

**Ringkasan per schema** (blok query 14):

| schema_name | total_size_pretty | total_size_bytes | table_count |
|-------------|-------------------|------------------|-------------|
| mart        | 11 GB             | 11,939,504,128   | 13          |
| staging     | 1344 kB           | 1,376,256        | 20          |
| raw         | 992 kB            | 1,015,808        | 7           |
| dimensions  | 624 kB            | 638,976          | 3           |

---

## 5. Data Quality & Coverage

### 5.1 Kualitas (Null / Outlier)

| table_name           | null_metric_count | outlier_likely_count | total_rows |
|----------------------|-------------------|----------------------|------------|
| mart_inverter_5min   | 0                 | 0                    | 52,339,748 |
| mart_sensor_5min     | 0                 | 0                    | 30,397,075 |
| mart_meter_5min      | 0                 | 1,573,123            | 48,436,481 |

*Meter memiliki 1.57M baris dengan nilai ekstrem (outlier_likely); dapat ditinjau apakah threshold (-1e6 / 1e9) perlu disesuaikan atau ada meter tertentu yang perlu koreksi.*

### 5.2 Coverage Tanggal Terakhir (Mart 5 Menit)

| table_name                      | system     | last_timestamp | last_date_key |
|---------------------------------|------------|----------------|---------------|
| mart_inverter_performance_5min  | fusionsolar | 2026-01-29 18:40 | 2026-01-29 |
| mart_inverter_performance_5min  | isolarcloud | 2026-01-29 23:55 | 2026-01-29 |
| mart_sensor_measurements_5min   | fusionsolar | 2026-01-29 23:55 | 2026-01-29 |
| mart_sensor_measurements_5min   | isolarcloud | 2026-01-29 23:55 | 2026-01-29 |
| mart_meter_performance_5min    | fusionsolar | 2026-01-29 23:55 | 2026-01-29 |
| mart_meter_performance_5min    | isolarcloud | 2026-01-29 23:55 | 2026-01-29 |

*Coverage sampai 2026-01-29; semua mart 5 min ter-update.*

### 5.3 Site Performance Daily per Site (Ringkasan)

*22 site total. Contoh (6 FusionSolar + 6 iSolarCloud):*

| site_id | site_name | system     | first_date | last_date | days_with_data | total_energy_mwh |
|---------|-----------|------------|------------|-----------|----------------|------------------|
| FS_SITE_NE=50488260 | PT. MMKI 1.75 MWp - Painting Building | fusionsolar | 2024-04-30 | 2026-01-29 | 627 | 3,540.85 |
| FS_SITE_NE=51758766 | PT. MMKI 5.7 MWp - Phase 2 | fusionsolar | 2024-06-26 | 2026-01-29 | 583 | 11,115.78 |
| FS_SITE_NE=58630782 | PT. MMKI 4.292 MWP - Phase 3 | fusionsolar | 2025-07-16 | 2026-01-29 | 198 | 2,946.83 |
| ISO_SITE_1458125 | Garuda Metalindo 1 | isolarcloud | 2024-11-07 | 2026-01-29 | 448 | 1,097.03 |
| ISO_SITE_1479456 | Shoetown Ligung Indonesia | isolarcloud | 2025-01-09 | 2026-01-29 | 385 | 3,799.35 |
| ISO_SITE_1680199 | PLTS Rooftop Sumatera Prima Fibreboard | isolarcloud | 2025-10-01 | 2026-01-29 | 120 | 921.98 |
| … | *(16 site lainnya — jalankan query blok 8 untuk daftar lengkap)* | | | | | |

---

## 6. Interpretasi & Rekomendasi

- **Konsistensi staging vs mart:** Staging ~857M baris (FusionSolar 509M + iSolarCloud 349M); mart 5 min ~131M baris (inverter 52M + sensor 30M + meter 48M). Perbedaan wajar karena mart hanya memuat metric dengan `used = 'yes'` di seed_metric_mapper dan filter device type.
- **Coverage tanggal:** Semua mart 5 min terakhir sampai **2026-01-29**; data terbaru masuk.
- **Gaps:** mart_site_performance_daily mencakup 22 site; beberapa site baru (Samator Bali, Samator Gas Malang/Sidoarjo/Solo, Indo Gas Semarang) punya `days_with_data` sedikit dan total_energy_mwh 0 — perlu dicek konfigurasi meter/sensor/revenue meter.
- **Data quality:** Null metric = 0 di ketiga mart 5 min. Outlier_likely di mart_meter_5min = 1.57M baris; disarankan tinjau threshold atau meter tertentu.
- **Data size:** Mart schema dominan (~11 GB, 13 tabel); terbesar mart_sensor_measurements_5min (6.75 GB), fact_inverter (2.75 GB), fact_sensor (1.6 GB). Raw/staging tampil kecil di `pg_total_relation_size` — kemungkinan pakai Hyperscale/foreign storage; ukuran aktual bisa dilihat di monitoring storage.
- **Langkah tindak lanjut:** (1) Review outlier meter (1.57M baris); (2) pastikan site baru ter-mapping di seed_meter_config/seed_sensor_site_mapping; (3) refresh report ini berkala setelah dbt run/reingestion.

---

## 7. Referensi

- **Query analisis:** `queries/analysis_data_condition_staging_to_mart.sql`
- **Panduan query staging (termasuk per device type):** [QUERYING_STAGING_DATA_GUIDE.md](QUERYING_STAGING_DATA_GUIDE.md)
- **Arsitektur & keputusan:** [architecture/README.md](architecture/README.md), [architecture/ARCHITECTURE_DECISIONS.md](architecture/ARCHITECTURE_DECISIONS.md)
- **Data flow lama (archive):** [architecture/archive/DATA_FLOW_SUMMARY.md](architecture/archive/DATA_FLOW_SUMMARY.md)

---

*Terakhir diupdate: 2026-01-27. Hasil dari query dijalankan via MCP Postgres. Report dapat di-refresh berkala (setelah dbt run atau reingestion) dengan menjalankan ulang `queries/analysis_data_condition_staging_to_mart.sql` (blok 1–14) dan mengisi ulang Section 4–6.*
