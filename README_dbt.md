# MMSR Solar Data - dbt Project

## Overview
This dbt project transforms raw solar energy data from iSolarCloud and FusionSolar systems into a unified data warehouse structure optimized for PowerBI reporting.

## Architecture

```
Raw Tables (Python Loaded)
    ↓
Staging Models (Unpivot)
    ↓
Dimensions
    ↓
Intermediate (Unified)
    ↓
Marts (Final Fact Tables)
    ↓
PowerBI
```

## Project Structure

```
.
├── models/
│   ├── staging/           # Raw data unpivoted and cleaned
│   ├── dimensions/         # Dimension tables (assets, dates)
│   ├── intermediate/      # Unified data from both systems
│   └── marts/            # Final fact tables for BI
├── seeds/                # CSV configuration files
├── tests/                # Data quality tests
└── macros/              # Reusable SQL macros
```

## Running the Project

### 1. Initialize Profiles
Copy `profiles.yml` to `~/.dbt/profiles.yml` and update database credentials.

### 2. Install dbt
```bash
pip install dbt-postgres
```

### 3. Seed Configuration Data
```bash
dbt seed
```

### 4. Run Models
```bash
# Run all models
dbt run

# Run specific model
dbt run --select stg_isolarcloud__perf_unpivoted

# Run with specific tag
dbt run --select tag:marts
```

### 5. Test Data Quality
```bash
dbt test
```

## Key Models

### Staging Layer
- `stg_isolarcloud__perf_unpivoted`: Unpivot iSolarCloud JSONB data
- `stg_fusionsolar__perf_unpivoted`: Unpivot FusionSolar JSONB data
- `stg_isolarcloud__sites`: iSolarCloud site metadata
- `stg_isolarcloud__devices`: iSolarCloud device metadata
- `stg_fusionsolar__sites`: FusionSolar site metadata
- `stg_fusionsolar__devices`: FusionSolar device metadata

### Dimensions
- `dim_assets`: Unified asset dimension (sites + devices from both systems)
- `dim_date_generated`: Date dimension table

### Intermediate
- `int_inverters_unified_5min`: Unified inverter data at 5-minute resolution
- `int_strings_unified_5min`: Unified string-level measurements
- `int_meters_unified`: Unified meter data
- `int_sensors_unified`: Unified sensor/weather data

### Marts (Final Fact Tables)
- `mart_inverter_performance_5min`: 5-minute inverter performance
- `mart_string_performance_5min`: 5-minute string performance
- `mart_meter_performance_5min`: 5-minute meter data
- `mart_sensor_measurements_5min`: 5-minute sensor data
- `mart_site_performance_daily`: Daily aggregated site performance
- `mart_inverter_performance_daily`: Daily aggregated inverter performance
- `mart_string_performance_daily`: Daily aggregated string performance

## PowerBI Connection

The mart tables are optimized for PowerBI with:
- Star schema design (fact + dimension tables)
- Proper indexing on foreign keys
- Descriptive column names
- Date dimensions for time-based analysis
- Unified asset structure across both systems

## Benefits Over Current Approach

1. **Single Source of Truth**: All views transformed from raw JSONB into consistent structure
2. **Maintainability**: Configuration-driven with seed files (CSVs)
3. **Performance**: Materialized tables for fast queries
4. **Scalability**: dbt handles dependencies and incremental updates
5. **Testing**: Built-in data quality tests
6. **Documentation**: Automated documentation with `dbt docs generate`

## Migration Path

1. Existing Python harvesters continue to load raw data
2. dbt transforms raw data into marts
3. PowerBI connects to mart tables instead of 100+ views
4. Gradual migration - can run both in parallel initially

