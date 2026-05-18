# Final Schema Structure - Current Setup (Verified from Database)

## ✅ Schema Organization

### `raw` Schema
**Purpose**: Raw source data from APIs (loaded by Python harvesters)

**Tables (6):**
- `fusionsolar_plants` - Plant/site metadata (**9 rows**)
- `fusionsolar_devices` - Device metadata (**227 rows**)
- `fusionsolar_historical_data` - Time-series JSONB data (**9,880,365 rows**, ~8.4 GB)
- `isolarcloud_power_stations` - Power station/site metadata (**10 rows**)
- `isolarcloud_devices` - Device metadata (**276 rows**)
- `isolarcloud_historical_data` - Time-series JSONB data (**11,666,283 rows**, ~4.3 GB)

**Total Raw Data**: **~21.5M historical records** (~12.7 GB)

---

### `staging` Schema
**Purpose**: Transformed models (staging + intermediate + seeds)

#### Staging Models (6 models)

**Views (4):**
- `stg_fusionsolar__sites` - Cleaned FusionSolar site metadata
- `stg_fusionsolar__devices` - Cleaned FusionSolar device metadata
- `stg_isolarcloud__sites` - Cleaned iSolarCloud site metadata
- `stg_isolarcloud__devices` - Cleaned iSolarCloud device metadata

**Incremental Tables (2):**
- `stg_fusionsolar__perf_unpivoted` - **379,490,591 rows** (~43 GB)
  - Unpivoted FusionSolar performance data (JSONB → long format)
  - Unique key: `(timestamp, dev_id, metric_id)`
  
- `stg_isolarcloud__perf_unpivoted` - **236,260,553 rows** (~30 GB)
  - Unpivoted iSolarCloud performance data (JSONB → long format)
  - Unique key: `(timestamp, device_ps_key, metric_id)`

**Total Staging Rows**: **~615.8M rows** (~73 GB)

#### Intermediate Models (3 models)

**Tables (3):**
- `int_inverters_unified_5min` - Unified inverter data at 5-minute resolution
  - Combines data from both FusionSolar and iSolarCloud
  - Row count: **519,646,125 rows** (~59 GB)
  
- `int_meters_unified` - Unified meter data
  - Row count: **70,208,625 rows** (~8.8 GB)
  
- `int_sensors_unified` - Unified sensor data
  - Row count: **25,619,343 rows** (~3.3 GB)

**Total Intermediate Rows**: **~615.5M rows** (~71.1 GB)

#### Seed Tables (5 models)

**Configuration Data:**
- `seed_metric_mapper` - **340 rows** (~64 KB)
  - Maps platform-specific metric IDs to unified metric names
  - Defines which metrics are used (`used='yes'`)
  
- `seed_sensor_config` - **117 rows** (~48 KB)
  - Sensor configuration and metadata
  - Maps sensors to sites and sensor types
  - Includes `sensor_capacity` column (kWp) for POA weighted average calculation
  
- `seed_meter_config` - **128 rows** (~48 KB)
  - Meter configuration and metadata
  - Maps meters to sites and meter types
  - Includes `meter_type` column (e.g., 'Revenue') for filtering
  
- `seed_daily_simulation_target` - **3,286 rows** (~560 KB)
  - Daily simulation targets for performance comparison
  
- `seed_site_config` - **15 rows** (~2 KB)
  - Site metadata including actual capacity (kW) and tariff
  - Used for site-level capacity and tariff information

**Total Seed Rows**: **~3,886 rows** (~722 KB)

#### ⚠️ Old Duplicate Tables (To Be Cleaned Up)

These tables exist in `staging` schema but should be removed (duplicates from before schema fix):
- `staging.dim_assets` - **522 rows** (duplicate - correct one is in `dimensions` schema)
- `staging.dim_date_generated` - **2,870 rows** (duplicate - correct one is in `dimensions` schema)
- `staging.mart_inverter_performance_5min` - **526,476,242 rows** (~65 GB) (should be in `mart` schema)
- `staging.mart_simulation_targets_daily` - **3,286 rows** (duplicate - correct one is in `mart` schema)

---

### `dimensions` Schema
**Purpose**: Dimension tables for analytics

**Tables (3):**
- `dim_assets` - **522 rows** (~136 KB)
  - Unified asset dimension (sites + devices from both systems)
  - Columns: asset_id, asset_level, asset_name, device_type_id, device_category, system, site_id, site_name, latitude, longitude
  - Asset IDs: `ISO_*` (iSolarCloud), `FS_*` (FusionSolar), `ISO_SITE_*`, `FS_SITE_*`
  
- `dim_date_generated` - **2,870 rows** (~392 KB)
  - Date dimension table (2020-01-01 to current + 2 years)
  - Columns: date_key, year, month, month_name, quarter_key, day_of_week, season, day_type, etc.
  
- `dim_site` - **19 rows** (~8 KB) ⚠️ **Note**: Berdasarkan pemeriksaan model dbt, tidak ada file `dim_site.sql` terpisah. Informasi site (kapasitas, tariff) sebenarnya ada di `dim_assets` dengan `asset_level = 'Site'`. Jika `dim_site` disebutkan, kemungkinan merujuk ke subset dari `dim_assets` atau view yang perlu dibuat.
  - Site dimension with actual capacity and tariff
  - Columns: site_id, site_code, site_name, site_name_clean, system, latitude, longitude, actual_capacity_kw, tariff, site_order
  - Source: Combines `stg_isolarcloud__sites`, `stg_fusionsolar__sites`, and `seed_site_config`
  - Used for site-level capacity and tariff in performance calculations
  - **Alternatif**: Gunakan `dim_assets` dengan filter `asset_level = 'Site'` untuk mendapatkan informasi site

**Total Dimension Rows**: **~3,411 rows** (~536 KB)

---

### `mart` Schema
**Purpose**: Business-ready fact tables for PowerBI and analytics

**Tables (3 models - Currently Built):**

- `mart_simulation_targets_daily` - **3,286 rows** (~992 KB)
  - Daily simulation targets for performance comparison
  - Source: seed_daily_simulation_target
  - Includes: energy_target_mwh, daily_pr_ghi_target, daily_pr_poa_target, ghi, poa

- `mart_inverter_performance_5min` - **37,418,383 rows** (~4.5 GB)
  - 5-minute inverter performance metrics (incremental table)
  - Filters: `metric_group = 'inverter'` and `used = 'yes'` from seed_metric_mapper
  - Excludes meter metrics (only inverter metrics)
  - Joined with `dim_assets` and `dim_date_generated`
  - Source: `stg_fusionsolar__perf_unpivoted` and `stg_isolarcloud__perf_unpivoted`

- `mart_site_performance_daily` - **4,255 rows** (~1.2 MB)
  - Daily site-level performance aggregations
  - **Energy**: Daily yield from revenue meters (kWh), calculated as MAX - MIN excluding 0/NULL
  - **GHI**: MAX of daily_irradiance from GHI sensors (kWh/m²), with MJ/m² conversion
  - **POA**: Weighted average by capacity from POA sensors (kWh/m²)
  - **Availability**: MIT-based calculation (Minimum Irradiance Threshold > 40 W/m²)
    - Uses GHI if available, otherwise POA for MIT
    - Calculates inverter availability ratio per timestamp
    - Unavailability = 1 - power_availability_ratio (when MIT = 1)
    - Daily percentage = power_available_hours / (power_available_hours + unavailability_hours) * 100
  - **Performance Ratios**: PR GHI and PR POA (daily_energy_kwh / irradiance / capacity_kw)
  - **Target Comparisons**: energy_actual_vs_target_pct, ghi_actual_vs_target_pct
  - Sources: `mart_meter_performance_5min`, `mart_sensor_measurements_5min`, `mart_inverter_performance_5min`
  - Joined with: `dim_assets` (untuk site metadata), `dim_date_generated`, `mart_simulation_targets_daily`
  - **Note**: Site metadata (capacity, tariff) diambil dari `dim_assets` dengan `asset_level = 'Site'`

**Other Mart Models (Not Yet Built):**
The following mart models are defined in dbt but have not been built yet:
- `mart_meter_performance_5min` - Incremental table (ready to build)
- `mart_sensor_measurements_5min` - Incremental table (ready to build)
- `mart_inverter_performance_daily` - Table (ready to build)

**Disabled Models (2):**
- `mart_string_performance_5min` - Disabled (`enabled=false`)
- `mart_string_performance_daily` - Disabled (`enabled=false`)

**Total Mart Models**: 3 built in `mart` schema, 3 ready to build, 2 disabled

---

## Configuration

### `profiles.yml`
```yaml
schema: staging  # Default schema for models without explicit schema
```

### `dbt_project.yml`
- **Staging models**: Views (default), perf_unpivoted override with incremental tables
- **Intermediate models**: Tables, use default schema (staging)
- **Dimensions**: Tables with `schema='dimensions'` in model config
- **Marts**: Tables with `schema='mart'` in model config

### Custom Schema Macro
**File**: `macros/generate_schema_name.sql`
- Overrides dbt's default schema prefixing behavior
- When `schema='mart'` or `schema='dimensions'` is specified in model config, uses exact schema name (no prefix)
- Models without explicit schema use default from `profiles.yml` (staging)

---

## Data Flow

```
raw schema (source data)
  ├── fusionsolar_* (3 tables, ~9.9M rows)
  └── isolarcloud_* (3 tables, ~11.7M rows)
  Total: ~21.5M rows, ~12.7 GB
  ↓
staging schema
  ├── Staging Models (6)
  │   ├── Sites/Devices Views (4 views)
  │   └── Performance Unpivoted Tables (2 tables - 615.8M rows, ~73 GB)
  │
  ├── Intermediate Models (3)
  │   ├── int_inverters_unified_5min (519.6M rows, ~59 GB)
  │   ├── int_meters_unified (70.2M rows, ~8.8 GB)
  │   └── int_sensors_unified (25.6M rows, ~3.3 GB)
  │
  └── Seed Tables (5)
      ├── seed_metric_mapper (340 rows)
      ├── seed_sensor_config (117 rows - includes sensor_capacity)
      ├── seed_meter_config (128 rows)
      ├── seed_daily_simulation_target (3,286 rows)
      └── seed_site_config (15 rows)
  ↓
dimensions schema
  ├── dim_assets (522 rows) - Includes site-level info (asset_level = 'Site')
  ├── dim_date_generated (2,870 rows)
  └── dim_site (19 rows) ⚠️ Note: Tidak ada model terpisah, info site ada di dim_assets
  ↓
mart schema
  ├── mart_simulation_targets_daily (3,286 rows)
  ├── mart_inverter_performance_5min (37.4M rows, ~4.5 GB)
  └── mart_site_performance_daily (4,255 rows, ~1.2 MB)
  (Other marts ready to build)
```

---

## Storage Summary

### By Schema (Verified from Database)

| Schema | Tables | Views | Total Rows | Data Size | Purpose |
|--------|--------|-------|------------|-----------|---------|
| **raw** | 6 | 0 | ~21.5M | ~12.7 GB | Source data (JSONB) |
| **staging** | 14* | 4 | ~1,231.3M* | ~144.8 GB* | Transformed data |
| **dimensions** | 3 | 0 | ~3,411 | ~536 KB | Dimension tables |
| **mart** | 3 | 0 | ~37.4M | ~4.5 GB | Business-ready facts |

*Note: Staging includes 4 duplicate tables that should be cleaned up (see below)

### By Layer (Actual Database State)

| Layer | Models | Total Rows | Data Size | Notes |
|-------|--------|------------|-----------|-------|
| **Raw** | 6 | ~21.5M | ~12.7 GB | Source JSONB data |
| **Staging** | 6 | ~615.8M | ~73 GB | Unpivoted raw data |
| **Intermediate** | 3 | ~615.5M | ~71.1 GB | Unified cross-source data |
| **Dimensions** | 3 | ~3,411 | ~536 KB | Asset, date, and site dimensions |
| **Marts** | 3 | ~37.4M | ~4.5 GB | Simulation targets + inverter + site performance |
| **Seeds** | 5 | ~3,886 | ~722 KB | Configuration data (metric, sensor, meter, site, targets) |

---

## ⚠️ Cleanup Required

### Old Duplicate Tables in Staging Schema

The following tables exist in `staging` schema but are duplicates from before the schema configuration was fixed. They should be dropped:

```sql
-- Remove old duplicates from staging schema
DROP TABLE IF EXISTS staging.dim_assets;
DROP TABLE IF EXISTS staging.dim_date_generated;
DROP TABLE IF EXISTS staging.mart_inverter_performance_5min;
DROP TABLE IF EXISTS staging.mart_simulation_targets_daily;
```

**Note**: The correct versions exist in:
- `dimensions.dim_assets` and `dimensions.dim_date_generated`
- `mart.mart_simulation_targets_daily`
- `mart.mart_inverter_performance_5min` (now built in correct schema)

---

## Benefits of This Setup

1. **Clear Schema Separation**: Each layer in its own schema (raw → staging → dimensions → mart)
2. **No Prefixing Issues**: Custom macro ensures exact schema names when specified
3. **Easy Queries**: Cross-schema joins work seamlessly
4. **Scalable**: Incremental models for large tables
5. **Maintainable**: Configuration data in seeds, dimensions separate from facts

---

## Next Steps

✅ **Completed:**
- Seeds rebuilt and verified in `staging` schema (5 seeds including `seed_site_config`)
- Dimensions rebuilt in `dimensions` schema:
  - `dim_assets` (522 rows)
  - `dim_date_generated` (2,870 rows)
  - `dim_site` (19 rows) - Site metadata with actual capacity and tariff
- Schema configuration verified (no prefixing)
- Three mart models built:
  - `mart_simulation_targets_daily` (3,286 rows)
  - `mart_inverter_performance_5min` (37.4M rows) - Fixed to exclude meter metrics, only inverter metrics
  - `mart_site_performance_daily` (4,255 rows) - Daily site aggregations with MIT-based availability

🧹 **Cleanup Needed:**
- Remove duplicate tables from `staging` schema (see above)

🚧 **Ready to Build:**
- `mart_meter_performance_5min` - Meter performance metrics (dependencies ready)
- `mart_sensor_measurements_5min` - Sensor measurements (dependencies ready)
- `mart_inverter_performance_daily` - Inverter-level daily aggregations (dependencies ready)
- All dependencies (staging, intermediate, dimensions, seeds) are ready

---

## Model Status Summary

| Category | Total | Built | Not Built | Disabled | Schema |
|----------|-------|-------|-----------|----------|--------|
| Staging | 6 | 6 | 0 | 0 | staging |
| Intermediate | 3 | 3 | 0 | 0 | staging |
| Dimensions | 3 | 3 | 0 | 0 | dimensions |
| Marts | 8 | 3 | 3 | 2 | mart |
| Seeds | 5 | 5 | 0 | 0 | staging |
| **Total** | **25** | **20** | **3** | **2** | - |

**Last Verified**: Updated on 2025-01-30
**Recent Updates**: 
- `mart_inverter_performance_5min` built in `mart` schema (37.4M rows)
  - Fixed filtering to exclude meter metrics (only `metric_group = 'inverter'`)
  - Fixed join to `dim_assets` (correct asset_id format: `ISO_*` and `FS_*`)
- `dim_site` dimension created (19 rows) with actual capacity and tariff
- `seed_site_config` added (15 rows) for site metadata
- `seed_sensor_config` updated with `sensor_capacity` column for POA weighted average
- `mart_site_performance_daily` built (4,255 rows) with:
  - Energy calculation from revenue meters (kWh, MAX-MIN excluding 0/NULL)
  - GHI: MAX daily_irradiance with MJ/m² conversion
  - POA: Weighted average by sensor capacity
  - Availability: MIT-based calculation (irradiance > 40 W/m² threshold)
  - PR calculations: GHI and POA based (using capacity_kw)
  - Target comparisons: energy_actual_vs_target_pct, ghi_actual_vs_target_pct
  - No MTD/YTD (calculated in PowerBI)
