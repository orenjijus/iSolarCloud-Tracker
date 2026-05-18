# Dependency Graph Model - Penjelasan Lengkap

## 🎯 Apa itu Dependency Graph?

**Dependency Graph** adalah struktur yang menunjukkan bagaimana models saling bergantung satu sama lain. dbt menggunakan dependency graph ini untuk menentukan **urutan eksekusi** models saat `dbt run`.

### Cara dbt Menentukan Dependency

dbt menganalisis setiap model dan mencari fungsi `{{ ref('model_name') }}` untuk mengetahui dependencies:

```sql
-- Contoh: mart_site_performance_daily.sql
FROM {{ ref('mart_meter_performance_5min') }} m
FROM {{ ref('fact_site_calculations_5min') }} fsc
FROM {{ ref('dim_assets') }} da
```

Ini berarti `mart_site_performance_daily` **bergantung pada**:
- `mart_meter_performance_5min`
- `fact_site_calculations_5min`
- `dim_assets`

## 📊 Dependency Graph Lengkap Project Ini

### Visual Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    RAW DATA (Sources)                            │
│  - raw.fusionsolar_historical_data                              │
│  - raw.isolarcloud_historical_data                              │
│  - raw.fusionsolar_plants, raw.fusionsolar_devices              │
│  - raw.isolarcloud_power_stations, raw.isolarcloud_devices      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    SEEDS (Pre-loaded, Manual) ⚠️                │
│                                                                   │
│  ⚠️ PENTING: Seeds TIDAK otomatis di-run saat dbt run!          │
│  Seeds harus di-load manual: dbt seed (atau dbt seed --select)  │
│                                                                   │
│  Seeds yang digunakan oleh models:                              │
│  - seed_metric_mapper                                            │
│  - seed_sensor_config                                            │
│  - seed_meter_config                                             │
│  - seed_site_config                                             │
│  - seed_daily_simulation_target                                 │
│  - seed_sensor_site_mapping                                     │
│  - seed_inverter_site_mapping                                   │
│  - seed_issue_dates                                              │
│  - seed_energy_adjustment_daily                                 │
│  - seed_ghi_adjustment_daily                                    │
│                                                                   │
│  Seeds diasumsikan sudah di-load sebelum dbt run                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                              (Seeds sudah ada di database)
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LAYER 2: STAGING (Views)                      │
│  - stg_fusionsolar__sites (VIEW)                                │
│  - stg_fusionsolar__devices (VIEW)                              │
│  - stg_isolarcloud__sites (VIEW)                                │
│  - stg_isolarcloud__devices (VIEW)                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LAYER 3: STAGING (Incremental Tables)         │
│  - stg_fusionsolar__perf_unpivoted (INCREMENTAL)                │
│    └─ depends on: stg_fusionsolar__devices, stg_fusionsolar__sites │
│  - stg_isolarcloud__perf_unpivoted (INCREMENTAL)                │
│    └─ depends on: stg_isolarcloud__devices, stg_isolarcloud__sites │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LAYER 4: DIMENSIONS (Tables)                  │
│  - dim_date_generated (TABLE)                                   │
│    └─ no dependencies (static date generation)                  │
│  - dim_assets (TABLE)                                           │
│    └─ depends on:                                               │
│       • stg_isolarcloud__sites, stg_isolarcloud__devices        │
│       • stg_fusionsolar__sites, stg_fusionsolar__devices        │
│       • seed_site_config                                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LAYER 5: MART 5MIN (Incremental Tables)       │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ mart_inverter_performance_5min (INCREMENTAL)           │   │
│  │   └─ depends on:                                        │   │
│  │      • stg_fusionsolar__perf_unpivoted                  │   │
│  │      • stg_isolarcloud__perf_unpivoted                  │   │
│  │      • stg_fusionsolar__devices, stg_isolarcloud__devices│   │
│  │      • stg_fusionsolar__sites, stg_isolarcloud__sites   │   │
│  │      • seed_metric_mapper                                │   │
│  │      • dim_assets                                        │   │
│  │      • dim_date_generated                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ mart_meter_performance_5min (INCREMENTAL)               │   │
│  │   └─ depends on:                                        │   │
│  │      • stg_fusionsolar__perf_unpivoted                  │   │
│  │      • stg_isolarcloud__perf_unpivoted                  │   │
│  │      • stg_fusionsolar__devices, stg_isolarcloud__devices│   │
│  │      • stg_fusionsolar__sites, stg_isolarcloud__sites   │   │
│  │      • seed_metric_mapper                                │   │
│  │      • seed_meter_config                                 │   │
│  │      • dim_assets                                        │   │
│  │      • dim_date_generated                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ mart_sensor_measurements_5min (INCREMENTAL)             │   │
│  │   └─ depends on:                                        │   │
│  │      • stg_fusionsolar__perf_unpivoted                  │   │
│  │      • stg_isolarcloud__perf_unpivoted                  │   │
│  │      • stg_fusionsolar__devices, stg_isolarcloud__devices│   │
│  │      • seed_metric_mapper                                │   │
│  │      • dim_assets                                        │   │
│  │      • dim_date_generated                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LAYER 6: FACTS 5MIN (Incremental Tables)     │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ fact_sensor_calculations_5min (INCREMENTAL)             │   │
│  │   └─ depends on:                                        │   │
│  │      • mart_sensor_measurements_5min                     │   │
│  │      • seed_sensor_config                                │   │
│  │      • seed_sensor_site_mapping                           │   │
│  │      • dim_assets                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ fact_inverter_calculations_5min (INCREMENTAL)           │   │
│  │   └─ depends on:                                        │   │
│  │      • mart_inverter_performance_5min                    │   │
│  │      • fact_sensor_calculations_5min (MIT calculation)   │   │
│  │      • dim_assets                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ fact_site_calculations_5min (INCREMENTAL)                │   │
│  │   └─ depends on:                                        │   │
│  │      • fact_inverter_calculations_5min                    │   │
│  │      • dim_assets                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LAYER 7: MART DAILY (Incremental Tables)     │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ mart_sensor_daily (INCREMENTAL)                        │   │
│  │   └─ depends on:                                        │   │
│  │      • mart_sensor_measurements_5min                    │   │
│  │      • seed_sensor_config                                │   │
│  │      • seed_sensor_site_mapping                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ mart_inverter_yield_daily (INCREMENTAL)                 │   │
│  │   └─ depends on:                                        │   │
│  │      • mart_inverter_performance_5min                    │   │
│  │      • dim_assets                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ mart_site_performance_daily (INCREMENTAL) ⚠️ PROBLEM     │   │
│  │   └─ depends on:                                        │   │
│  │      • mart_meter_performance_5min (Energy)             │   │
│  │      • mart_sensor_daily (GHI/POA)                      │   │
│  │      • fact_site_calculations_5min (Availability)        │   │
│  │      • dim_assets                                        │   │
│  │      • dim_date_generated                                 │   │
│  │      • seed_energy_adjustment_daily                      │   │
│  │      • seed_ghi_adjustment_daily                         │   │
│  │      • seed_issue_dates                                  │   │
│  │      • mart_simulation_targets_daily                     │   │
│  │      • mart_site_kpi_monthly                             │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    LAYER 8: MART MONTHLY (Views)                 │
│  - mart_site_performance_monthly (VIEW)                         │
│    └─ depends on: mart_site_performance_daily                   │
│  - mart_simulation_targets_monthly (VIEW)                       │
│    └─ depends on: mart_simulation_targets_daily                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🔄 Execution Order saat `dbt run`

### Bagaimana dbt Menentukan Urutan?

dbt menggunakan **topological sort** pada dependency graph:

1. **Models tanpa dependencies** di-run pertama (dim_date_generated, mart_site_kpi_monthly)
   - **Seeds TIDAK otomatis di-run** - harus di-load manual dengan `dbt seed`
2. **Models yang dependencies-nya sudah selesai** di-run berikutnya
3. **Models yang depend pada models lain** menunggu dependencies selesai

### Contoh Execution Order (ACTUAL dari Log Terminal)

**⚠️ PENTING:** Seeds **TIDAK otomatis di-run** saat `dbt run`. Seeds harus di-load manual dengan `dbt seed` sebelum menjalankan models.

Saat `dbt run`, urutan eksekusi **actual** berdasarkan log terminal:

```
0. Seeds (TIDAK otomatis - harus di-run manual terlebih dahulu)
   └─ dbt seed (atau dbt seed --select <seed_name>)
   └─ Seeds diasumsikan sudah ada di database sebelum dbt run

1. Dimensions & Independent Tables (parallel)
   ├─ dim_date_generated (no dependencies) ← Line 66
   └─ mart_site_kpi_monthly (table, no dependencies) ← Line 68

2. Staging Views (parallel) ← Lines 70-73
   ├─ stg_fusionsolar__devices
   ├─ stg_fusionsolar__sites
   ├─ stg_isolarcloud__devices
   └─ stg_isolarcloud__sites

3. Staging Incremental (parallel, tapi tunggu views selesai) ← Lines 75, 79
   ├─ stg_fusionsolar__perf_unpivoted ← Line 75
   └─ stg_isolarcloud__perf_unpivoted ← Line 79

4. Dimensions yang depend pada staging ← Line 77
   └─ dim_assets (depends on staging sites/devices)

5. Mart Tables (independent) ← Line 80
   └─ mart_simulation_targets_daily (table, depends on seeds)

6. Mart 5min (parallel, tapi tunggu staging selesai) ← Lines 82-87
   ├─ mart_inverter_performance_5min ← Line 82
   ├─ mart_meter_performance_5min ← Line 84
   └─ mart_sensor_measurements_5min ← Line 86

7. Mart Monthly Table (independent) ← Line 88
   └─ mart_simulation_targets_monthly (depends on mart_simulation_targets_daily)

8. Mart Daily (dari 5min) ← Line 90
   └─ mart_inverter_yield_daily (depends on mart_inverter_performance_5min)

9. Facts 5min (sequential, karena dependencies) ← Lines 92-99
   ├─ fact_sensor_calculations_5min (first) ← Line 92
   ├─ fact_inverter_calculations_5min (depends on fact_sensor) ← Line 96
   └─ fact_site_calculations_5min (depends on fact_inverter) ← Line 98

10. Mart Daily (dari facts) ← Line 94
    └─ mart_sensor_daily (depends on mart_sensor_measurements_5min)

11. Mart Site Performance Daily (depends on facts) ← Line 100
    └─ mart_site_performance_daily (depends on fact_site_calculations_5min)

12. Mart Monthly Views (tunggu daily selesai) ← Line 102
    └─ mart_site_performance_monthly (view, depends on mart_site_performance_daily)
```

**Catatan dari Log Actual:**
- Seeds **TIDAK** muncul di log (karena tidak di-run)
- `dim_date_generated` di-run pertama (line 66)
- `mart_site_kpi_monthly` di-run kedua (line 68) - ini table, bukan seed
- Staging views di-run parallel (lines 70-73)
- Staging incremental di-run setelah views (lines 75, 79)
- `dim_assets` di-run setelah staging (line 77)
- Models di-run berdasarkan dependency, bukan urutan file

### ⚠️ Catatan Penting

**Models yang tidak depend satu sama lain bisa di-run parallel atau dalam urutan berbeda!**

Contoh:
- `mart_inverter_performance_5min` dan `mart_meter_performance_5min` tidak depend satu sama lain
- dbt bisa run `mart_inverter_performance_5min` dulu, atau `mart_meter_performance_5min` dulu
- **Urutan tidak guaranteed!**

## 🐛 Masalah dengan Incremental Filter

### Kenapa Data FusionSolar Tidak Masuk?

Masalah terjadi di `mart_site_performance_daily` karena:

1. **Filter Incremental:**
   ```sql
   AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
   ```

2. **Execution Order:**
   - `mart_site_performance_daily` di-run setelah `fact_site_calculations_5min`
   - Saat `mart_site_performance_daily` di-run, filter menggunakan `MAX(date_key)` dari tabel yang sudah ada
   - Jika sudah ada data dengan `date_key > 2025-12-17`, filter akan skip 2025-12-17

3. **Kenapa iSolarCloud Masuk tapi FusionSolar Tidak?**
   
   **Kemungkinan 1: Data Existing**
   - Sudah ada data iSolarCloud dengan `date_key = 2025-12-18` (atau lebih baru)
   - Saat `mart_site_performance_daily` di-run:
     - `MAX(date_key)` = 2025-12-18
     - Filter: `date_key > 2025-12-18`
     - Data 2025-12-17 untuk **kedua sistem** di-skip
   - Tapi kenapa iSolarCloud masuk? Mungkin ada data iSolarCloud yang di-run lebih dulu dengan date_key yang lebih kecil

   **Kemungkinan 2: Execution Order**
   - iSolarCloud data di-run lebih dulu dalam dependency chain
   - Saat itu `MAX(date_key)` masih < 2025-12-17, jadi data masuk
   - FusionSolar di-run setelahnya, tapi `MAX(date_key)` sudah >= 2025-12-17, jadi di-skip

   **Kemungkinan 3: Data di Intermediate Layer**
   - Data FusionSolar mungkin tidak sampai ke `fact_site_calculations_5min` karena filter incremental di layer intermediate
   - Jika `fact_site_calculations_5min` tidak punya data FusionSolar untuk 2025-12-17, maka `mart_site_performance_daily` juga tidak akan punya

## 🔍 Cara Cek Dependency Graph

### 1. Menggunakan dbt list

```bash
# Lihat semua models dengan dependencies
dbt list

# Lihat models yang depend pada model tertentu
dbt list --select stg_fusionsolar__perf_unpivoted+

# Lihat models yang model tertentu depend pada
dbt list --select +mart_site_performance_daily
```

### 2. Menggunakan dbt docs

```bash
# Generate documentation dengan dependency graph
dbt docs generate
dbt docs serve
```

Ini akan membuka browser dengan visualisasi dependency graph interaktif.

### 3. Manual Check

Cari `{{ ref('model_name') }}` di setiap model file:

```bash
# Cari semua dependencies
grep -r "ref\(" dbt/models/
```

## 📝 Dependencies Detail per Model

### mart_site_performance_daily

**Dependencies:**
- `mart_meter_performance_5min` (untuk energy calculation)
- `mart_sensor_daily` (untuk GHI/POA)
- `fact_site_calculations_5min` (untuk availability)
- `dim_assets` (untuk site metadata)
- `dim_date_generated` (untuk date attributes)
- `seed_energy_adjustment_daily` (untuk energy adjustment)
- `seed_ghi_adjustment_daily` (untuk GHI adjustment)
- `seed_issue_dates` (untuk issue date flag)
- `mart_simulation_targets_daily` (untuk target comparison)
- `mart_site_kpi_monthly` (untuk KPI calculation)

**Dependents:**
- `mart_site_performance_monthly` (view yang depend pada daily)

### fact_site_calculations_5min

**Dependencies:**
- `fact_inverter_calculations_5min` (untuk aggregate inverter metrics)
- `dim_assets` (untuk total_inverters)

**Dependents:**
- `mart_site_performance_daily` (untuk availability calculation)

### fact_inverter_calculations_5min

**Dependencies:**
- `mart_inverter_performance_5min` (untuk active power)
- `fact_sensor_calculations_5min` (untuk MIT)
- `dim_assets` (untuk site metadata)

**Dependents:**
- `fact_site_calculations_5min` (untuk site aggregation)

## 🎯 Kesimpulan

1. **Dependency Graph** menentukan urutan eksekusi models
2. **Models yang tidak depend satu sama lain** bisa di-run dalam urutan berbeda
3. **Incremental Filter** menggunakan `MAX(date_key)` global, bukan per-system
4. **Execution Order** bisa mempengaruhi hasil karena filter incremental di-evaluate saat model di-run
5. **Solusi:** Gunakan `--vars` untuk force re-process tanggal tertentu

## 📚 Related Documentation

- [INCREMENTAL_FILTER_ISSUE_EXPLANATION.md](./INCREMENTAL_FILTER_ISSUE_EXPLANATION.md)
- [TROUBLESHOOTING_FUSIONSOLAR_DATA.md](./TROUBLESHOOTING_FUSIONSOLAR_DATA.md)
- [REINGESTION_WORKFLOW.md](./REINGESTION_WORKFLOW.md)

