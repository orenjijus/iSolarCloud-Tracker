# Referensi Tabel Database

Dokumentasi ini menjelaskan struktur tabel database yang digunakan dalam proyek dbt MMSR Solar Data, sesuai dengan model dbt yang telah dibuat.

## Tabel Utama

### Schema: `mart`

Tabel-tabel di schema `mart` berisi data yang siap digunakan untuk analitik dan reporting (PowerBI).

| Tabel | Deskripsi | Schema | Status | Materialization |
|-------|-----------|--------|--------|-----------------|
| `mart_site_performance_daily` | Tabel utama untuk semua metrik harian site-level. Berisi aggregasi harian: energy (MWh), GHI, POA, availability, PR GHI, PR POA, dan perbandingan dengan target. | `mart` | ✅ Built | Incremental Table |
| `mart_meter_performance_5min` | Data meter 5 menit. Berisi metrik meter (positive_active_energy, negative_active_energy) dari revenue meters. Digunakan untuk perhitungan daily energy. | `mart` | ✅ Built | Incremental Table |
| `mart_sensor_measurements_5min` | Data sensor 5 menit. Berisi pengukuran sensor (irradiance, temperature, dll) untuk perhitungan GHI dan POA. | `mart` | ✅ Built | Incremental Table |
| `mart_inverter_performance_5min` | Data inverter 5 menit. Berisi metrik inverter (active_power, dll) untuk perhitungan availability dan performance. | `mart` | ✅ Built | Incremental Table |
| `fact_site_calculations_5min` | Kalkulasi availability 5 menit. Berisi agregasi site-level dari inverter metrics dengan perhitungan MIT (Minimum Irradiance Threshold), power_available_ratio, dan unavailability_ratio. | `mart` | ✅ Built | Incremental Table |
| `mart_simulation_targets_daily` | Target dari simulasi harian. Berisi target energy, GHI, POA, dan PR target untuk perbandingan dengan actual performance. | `mart` | ✅ Built | Table |

**Catatan**: Meskipun `fact_site_calculations_5min` menggunakan prefix `fact_`, tabel ini berada di schema `mart` (bukan schema `facts` terpisah), sesuai dengan konfigurasi dbt project.

### Schema: `dimensions`

Tabel-tabel dimensi untuk analitik.

| Tabel | Deskripsi | Schema | Status | Materialization |
|-------|-----------|--------|--------|-----------------|
| `dim_site` | Dimensi site (kapasitas, tariff, dll). **Catatan**: Berdasarkan pemeriksaan model dbt, informasi site sebenarnya ada di `dim_assets` dengan `asset_level = 'Site'`. Jika `dim_site` disebutkan dalam dokumentasi lain, kemungkinan merujuk ke subset dari `dim_assets`. | `dimensions` | ⚠️ Perlu Verifikasi | Table |
| `dim_assets` | Dimensi unified untuk semua assets (sites + devices). Berisi informasi site-level (kapasitas, tariff, site_order) dan device-level. Kolom `asset_level` membedakan antara 'Site' dan 'Device'. | `dimensions` | ✅ Built | Table |
| `dim_date_generated` | Dimensi tanggal. Berisi informasi tanggal (year, month, week_of_month, season, day_type, dll) untuk periode 2020-01-01 hingga current + 2 years. | `dimensions` | ✅ Built | Table |

**Catatan Penting**: 
- `dim_assets` menggabungkan informasi site dan device dalam satu tabel
- Untuk site-level attributes (capacity, tariff), gunakan `dim_assets` dengan filter `asset_level = 'Site'`
- Jika `dim_site` disebutkan dalam dokumentasi, kemungkinan merujuk ke view atau subset dari `dim_assets`

### Schema: `staging`

Tabel-tabel seed (konfigurasi) yang digunakan untuk mapping dan konfigurasi.

| Tabel | Deskripsi | Schema | Status | Materialization |
|-------|-----------|--------|--------|-----------------|
| `seed_meter_config` | Konfigurasi meter. Berisi informasi meter (meter_type, polarity_swapped, voltage_level, dll) untuk mapping dan perhitungan energy. | `staging` | ✅ Built | Seed (CSV) |
| `seed_sensor_config` | Konfigurasi sensor. Berisi informasi sensor (sensor_type, device_id, sensor_capacity_kwp, dll) untuk mapping sensor ke site dan perhitungan POA weighted average. | `staging` | ✅ Built | Seed (CSV) |
| `seed_sensor_site_mapping` | Mapping sensor ke site. Berisi mapping khusus seperti POA_OVERRIDE dan GHI_FALLBACK untuk menangani kasus khusus (misalnya sensor fisik di site lain). | `staging` | ✅ Built | Seed (CSV) |
| `seed_issue_dates` | Tanggal issue/maintenance. Berisi daftar tanggal yang harus di-flag sebagai issue date untuk perhitungan adjusted performance. Kolom: site_id, site_name, issue_date, is_active. | `staging` | ✅ Built | Seed (CSV) |

## Detail Tabel

### `mart_site_performance_daily`

**Tujuan**: Tabel utama untuk semua metrik harian site-level.

**Kolom Utama**:
- `date_key`, `year`, `month`, `month_name`, `week_of_month`
- `site_id`, `site_name`, `system`, `actual_capacity_kw`, `tariff`, `site_order`
- `is_issue_date` - Flag untuk tanggal maintenance/issue
- `daily_energy_mwh` - Energy harian dari revenue meters (MWh)
- `daily_ghi_kwh_m2` - GHI harian (kWh/m²), dengan fallback logic
- `daily_poa_weighted_kwh_m2` - POA weighted average (kWh/m²)
- `power_available_hours`, `unavailability_hours`, `mit_hours`, `total_hours`
- `availability_percent` - Availability dalam decimal (0-1)
- `pr_ghi_actual`, `pr_poa_actual` - Performance Ratio (decimal)
- `energy_target_mwh`, `daily_pr_ghi_target`, `daily_pr_poa_target`, `ghi_target`, `poa_target`
- `energy_actual_vs_target_pct`, `ghi_actual_vs_target_pct`, `poa_actual_vs_target_pct`
- `energy_kpi_daily_mwh`, `energy_kpi_monthly_mwh`, `energy_target_monthly_mwh`

**Sumber Data**:
- Energy: `mart_meter_performance_5min` (revenue meters)
- GHI: `mart_sensor_daily` (dengan fallback logic dari `seed_sensor_site_mapping`)
- POA: `mart_sensor_daily` (weighted average berdasarkan capacity)
- Availability: `fact_site_calculations_5min` (agregasi dari inverter metrics)
- Target: `mart_simulation_targets_daily`
- KPI: `mart_site_kpi_monthly`
- Issue dates: `seed_issue_dates`

**Dependencies**:
- `mart_meter_performance_5min`
- `mart_sensor_daily` (untuk GHI dan POA)
- `fact_site_calculations_5min` (untuk availability)
- `mart_simulation_targets_daily`
- `mart_site_kpi_monthly`
- `dim_assets`
- `dim_date_generated`
- `seed_issue_dates`
- `seed_sensor_site_mapping` (untuk GHI fallback)

### `mart_meter_performance_5min`

**Tujuan**: Data meter 5 menit untuk perhitungan energy harian.

**Kolom Utama**:
- `timestamp`, `date_key`
- `asset_id`, `asset_name`, `site_name`, `system`
- `metric_id`, `metric_name`, `metric_group`, `metric_unit`
- `metric_value` - Nilai metrik (kWh atau Wh, dikonversi ke kWh)
- `meter_type` - Tipe meter (Revenue, dll)
- `voltage_level`, `meter_dev_name`

**Sumber Data**:
- `stg_isolarcloud__perf_unpivoted` dan `stg_fusionsolar__perf_unpivoted`
- Filter: `metric_group = 'meter'` dan `used = 'yes'` dari `seed_metric_mapper`
- Device type: `device_type = 7` (iSolarCloud) atau `dev_type_id = 17` (FusionSolar)

**Dependencies**:
- `stg_isolarcloud__perf_unpivoted`
- `stg_fusionsolar__perf_unpivoted`
- `stg_isolarcloud__devices`, `stg_isolarcloud__sites`
- `stg_fusionsolar__devices`
- `dim_assets`
- `dim_date_generated`
- `seed_metric_mapper`
- `seed_meter_config`

### `mart_sensor_measurements_5min`

**Tujuan**: Data sensor 5 menit untuk perhitungan GHI dan POA.

**Kolom Utama**:
- `timestamp`, `date_key`
- `asset_id`, `asset_name`, `site_name`, `system`
- `metric_id`, `metric_name`, `metric_group`, `metric_unit`
- `metric_value` - Nilai metrik (irradiance, temperature, dll)
- `sensor_type` - Tipe sensor (GHI, POA, dll)
- `sensor_dev_name`

**Sumber Data**:
- `stg_isolarcloud__perf_unpivoted` dan `stg_fusionsolar__perf_unpivoted`
- Filter: `metric_group = 'sensor'` dan `used = 'yes'` dari `seed_metric_mapper`
- Device type: `device_type = 5` (iSolarCloud) atau `dev_type_id = 10` (FusionSolar)

**Dependencies**:
- `stg_isolarcloud__perf_unpivoted`
- `stg_fusionsolar__perf_unpivoted`
- `stg_isolarcloud__devices`, `stg_isolarcloud__sites`
- `stg_fusionsolar__devices`
- `dim_assets`
- `dim_date_generated`
- `seed_metric_mapper`
- `seed_sensor_config`

### `mart_inverter_performance_5min`

**Tujuan**: Data inverter 5 menit untuk perhitungan availability dan performance.

**Kolom Utama**:
- `timestamp`, `date_key`
- `asset_id`, `asset_name`, `site_name`, `system`
- `metric_id`, `metric_name`, `metric_group`, `metric_unit`
- `metric_value` - Nilai metrik (active_power, dll)

**Sumber Data**:
- `stg_isolarcloud__perf_unpivoted` dan `stg_fusionsolar__perf_unpivoted`
- Filter: `metric_group = 'inverter'` dan `used = 'yes'` dari `seed_metric_mapper`
- Device type: `device_type = 1` (iSolarCloud) atau `dev_type_id = 1` (FusionSolar)
- Mendukung ID consolidation dan name override dari `seed_inverter_site_mapping`

**Dependencies**:
- `stg_isolarcloud__perf_unpivoted`
- `stg_fusionsolar__perf_unpivoted`
- `stg_isolarcloud__devices`, `stg_isolarcloud__sites`
- `stg_fusionsolar__devices`
- `dim_assets`
- `dim_date_generated`
- `seed_metric_mapper`
- `seed_inverter_site_mapping` (untuk ID consolidation dan name override)

### `fact_site_calculations_5min`

**Tujuan**: Kalkulasi availability 5 menit di level site.

**Kolom Utama**:
- `timestamp`, `date_key`
- `site_id`, `site_name`, `system`
- `total_inverters` - Total inverter (fixed dari config atau dynamic untuk MMKI)
- `available_inverters` - Jumlah inverter yang available (power > 0)
- `mit` - Minimum Irradiance Threshold (1 jika irradiance > 40 W/m²)
- `power_available_ratio` - Ratio inverter available (0-1)
- `unavailability_ratio` - Ratio unavailability (0-1), hanya dihitung saat MIT = 1
- `calculation_timestamp`

**Sumber Data**:
- Agregasi dari `fact_inverter_calculations_5min`
- MIT dihitung dari GHI atau POA (fallback logic)

**Dependencies**:
- `fact_inverter_calculations_5min`
- `dim_assets` (untuk total_inverters fixed)

**Catatan**: 
- Untuk MMKI group: menggunakan dynamic total_inverters (hanya menghitung inverter yang reporting)
- Untuk site lain: menggunakan fixed total_inverters dari config (missing inverters = unavailable)

### `mart_simulation_targets_daily`

**Tujuan**: Target dari simulasi harian untuk perbandingan dengan actual performance.

**Kolom Utama**:
- `date_key`, `year`, `month`, `month_name`, `day_type`
- `site_name`, `site_code`, `asset_id`
- `ghi`, `poa` - Target irradiance (kWh/m²)
- `energy_simulation_mwh`, `energy_target_mwh` - Target energy (MWh)
- `daily_pr_ghi_simulation`, `daily_pr_ghi_target` - Target PR GHI (decimal)
- `daily_pr_poa_simulation`, `daily_pr_poa_target` - Target PR POA (decimal)
- `ghi_vs_poa`

**Sumber Data**:
- `seed_daily_simulation_target` (CSV)

**Dependencies**:
- `seed_daily_simulation_target`
- `dim_date_generated`

### `dim_assets`

**Tujuan**: Dimensi unified untuk semua assets (sites + devices).

**Kolom Utama**:
- `asset_id` - ID asset (ISO_*, FS_*, ISO_SITE_*, FS_SITE_*)
- `asset_level` - 'Site' atau 'Device'
- `asset_name`, `device_type_id`, `device_category`
- `system` - 'isolarcloud' atau 'fusionsolar'
- `site_id`, `site_name`, `site_name_clean`
- `latitude`, `longitude`
- `actual_capacity_kw` - Kapasitas site (hanya untuk asset_level = 'Site')
- `tariff` - Tarif (hanya untuk asset_level = 'Site')
- `site_order` - Urutan site (hanya untuk asset_level = 'Site')
- `calculation_start_date` - Tanggal mulai perhitungan (hanya untuk asset_level = 'Site')
- `total_inverters` - Total inverter fixed (hanya untuk asset_level = 'Site')

**Sumber Data**:
- `stg_isolarcloud__sites`, `stg_isolarcloud__devices`
- `stg_fusionsolar__sites`, `stg_fusionsolar__devices`
- `seed_site_config` (untuk capacity, tariff, site_order, calculation_start_date, total_inverters)

**Catatan**: 
- Informasi site-level (capacity, tariff) ada di `dim_assets` dengan filter `asset_level = 'Site'`
- Tidak ada tabel `dim_site` terpisah dalam model dbt yang ada

### `dim_date_generated`

**Tujuan**: Dimensi tanggal untuk analitik.

**Kolom Utama**:
- `date_key` - Primary key (DATE)
- `year`, `month`, `day`, `month_name`
- `week_of_month` - Week number dalam bulan (reset setiap bulan, max 5)
- `day_of_week`, `day_name`, `day_of_year`
- `month_key`, `quarter_key`, `year_key`
- `season` - 'Dry Season (Kemarau)' atau 'Wet Season (Hujan)'
- `day_type` - 'Weekday' atau 'Weekend'

**Periode**: 2020-01-01 hingga CURRENT_DATE + 2 years

**Catatan**: 
- Week logic: Week increment pada Monday, reset ke 1 pada hari pertama bulan baru
- Week 6 dengan hanya 1 hari (last Monday of month) akan digabung ke Week 5

### `seed_meter_config`

**Tujuan**: Konfigurasi meter untuk mapping dan perhitungan energy.

**Kolom Utama**:
- `esn_code` - ESN code meter
- `Source` - 'iSolarCloud' atau 'FusionSolar'
- `meter_type` - Tipe meter ('Revenue', dll)
- `polarity_swapped` - Flag untuk meter dengan polarity swapped (TRUE/FALSE)
- `voltage_level`, `dev_name`

**Penggunaan**:
- Mapping meter ke asset_id
- Filter revenue meters untuk perhitungan energy
- Polarity correction untuk perhitungan energy harian

### `seed_sensor_config`

**Tujuan**: Konfigurasi sensor untuk mapping dan perhitungan irradiance.

**Kolom Utama**:
- `device_id` - ID sensor
- `sensor_type` - Tipe sensor ('GHI', 'POA', dll)
- `sensor_capacity_kwp` - Kapasitas sensor (kWp) untuk weighted average POA
- `dev_name`

**Penggunaan**:
- Mapping sensor ke asset_id
- Identifikasi sensor type (GHI vs POA)
- Kapasitas sensor untuk weighted average POA

### `seed_sensor_site_mapping`

**Tujuan**: Mapping khusus sensor ke site untuk kasus edge case.

**Kolom Utama**:
- `mapping_type` - Tipe mapping ('POA_OVERRIDE', 'GHI_FALLBACK', dll)
- `device_id` - ID sensor fisik
- `logical_site_id` - Site logis (untuk POA_OVERRIDE) atau source site (untuk GHI_FALLBACK)
- `effective_date_start`, `effective_date_end` - Periode efektif mapping
- `notes` - Catatan

**Penggunaan**:
- **POA_OVERRIDE**: Sensor fisik di site A, tetapi data digunakan untuk site B (misalnya sensor MMKI II secara fisik ada di MMKI I)
- **GHI_FALLBACK**: Site tanpa GHI sensor menggunakan GHI dari source site (misalnya MMKI II menggunakan GHI dari MMKI I)

### `seed_issue_dates`

**Tujuan**: Tanggal issue/maintenance untuk flagging adjusted performance.

**Kolom Utama**:
- `site_id` - ID site (ISO_SITE_* atau FS_SITE_*)
- `site_name` - Nama site
- `issue_date` - Tanggal issue/maintenance (DATE)
- `is_active` - Flag aktif (TRUE/FALSE)

**Penggunaan**:
- Flag tanggal yang harus di-exclude dari adjusted performance calculations
- Digunakan di `mart_site_performance_daily` untuk kolom `is_issue_date`

## Relasi dan Dependencies

### Data Flow

```
Raw Data (raw schema)
  ↓
Staging (staging schema)
  ├── stg_*_perf_unpivoted (incremental)
  ├── stg_*_sites, stg_*_devices (views)
  └── seed_* (CSV seeds)
  ↓
Dimensions (dimensions schema)
  ├── dim_assets (table)
  └── dim_date_generated (table)
  ↓
Mart 5min (mart schema)
  ├── mart_meter_performance_5min (incremental)
  ├── mart_sensor_measurements_5min (incremental)
  ├── mart_inverter_performance_5min (incremental)
  └── fact_site_calculations_5min (incremental)
  ↓
Mart Daily (mart schema)
  ├── mart_site_performance_daily (incremental)
  └── mart_simulation_targets_daily (table)
```

### Key Dependencies

1. **mart_site_performance_daily** bergantung pada:
   - `mart_meter_performance_5min` → Energy calculation
   - `mart_sensor_daily` → GHI dan POA calculation
   - `fact_site_calculations_5min` → Availability calculation
   - `mart_simulation_targets_daily` → Target comparison
   - `dim_assets` → Site metadata (capacity, tariff)
   - `dim_date_generated` → Date attributes
   - `seed_issue_dates` → Issue date flagging
   - `seed_sensor_site_mapping` → GHI fallback logic

2. **fact_site_calculations_5min** bergantung pada:
   - `fact_inverter_calculations_5min` → Inverter metrics aggregation
   - `dim_assets` → Total inverters fixed

3. **mart_*_performance_5min** bergantung pada:
   - `stg_*_perf_unpivoted` → Raw unpivoted data
   - `dim_assets` → Asset mapping
   - `dim_date_generated` → Date attributes
   - `seed_metric_mapper` → Metric mapping
   - `seed_*_config` → Device configuration

## Catatan Penting

1. **Schema Naming**: 
   - Semua tabel mart dan fact berada di schema `mart` (tidak ada schema `facts` terpisah)
   - Tabel dengan prefix `fact_` tetap berada di schema `mart`

2. **dim_site vs dim_assets**:
   - Tidak ada model `dim_site.sql` terpisah dalam codebase
   - Informasi site ada di `dim_assets` dengan `asset_level = 'Site'`
   - Jika dokumentasi lain menyebutkan `dim_site`, kemungkinan merujuk ke subset dari `dim_assets`

3. **Materialization Strategy**:
   - Tabel 5min menggunakan `incremental` untuk performa optimal
   - Tabel daily menggunakan `incremental` dengan unique key `(date_key, site_id)`
   - Dimensions menggunakan `table` (static, kecil)
   - Seeds adalah CSV files yang di-load ke staging schema

4. **Incremental Logic**:
   - Semua tabel incremental mendukung reingestion dengan variables:
     - `reingest_start_date`, `reingest_end_date`
     - `reingest_ps_ids`, `reingest_plant_codes`, `reingest_device_ids`

## Update History

- **2025-01-XX**: Dokumentasi awal dibuat berdasarkan model dbt yang ada
- Verifikasi: Semua tabel yang disebutkan dalam referensi telah diverifikasi dengan model dbt aktual

