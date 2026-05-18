# dbt Migration Guide: From 100+ Views to Unified Data Warehouse

## Problem Statement

**Before**: You had to maintain 100+ separate views, one per site (e.g., `garuda_metalindo_1_inverter_data`, `charoen_pokphand_bandung_inverter_data`, etc.)

**After**: Single unified data warehouse with 7 mart tables that serve all sites and all requirements.

## Transformation Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     RAW DATA LAYER                            │
│  ┌─────────────────────┐      ┌─────────────────────┐       │
│  │ isolarcloud_*       │      │ fusionsolar_*       │       │
│  │ - historical_data   │      │ - historical_data    │       │
│  │ - devices           │      │ - devices            │       │
│  │ - power_stations    │      │ - plants             │       │
│  └─────────────────────┘      └─────────────────────┘       │
└─────────────────────────────────────────────────────────────┘
                      ↓ Python Loaders
                      (Continue as-is)
┌─────────────────────────────────────────────────────────────┐
│                     STAGING LAYER (dbt)                      │
│  ┌──────────────────────┐     ┌──────────────────────┐     │
│  │ Unpivot iSolarCloud │     │ Unpivot FusionSolar  │     │
│  │ JSONB → Long Format  │     │ JSONB → Long Format  │     │
│  └──────────────────────┘     └──────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                      DIMENSIONS (dbt)                        │
│  ┌────────────────────────┐   ┌──────────────────────────┐ │
│  │ dim_assets             │   │ dim_date_generated       │ │
│  │ - All sites/devices    │   │ - Date attributes        │ │
│  │ - From both systems    │   │ - Time intelligence      │ │
│  └────────────────────────┘   └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                   INTERMEDIATE LAYER (dbt)                   │
│  ┌──────────────────────┐  ┌──────────────────────────┐   │
│  │ int_inverters_       │  │ int_strings_unified_5min │   │
│  │ unified_5min         │  │                          │   │
│  └──────────────────────┘  └──────────────────────────┘   │
│  ┌──────────────────────┐  ┌──────────────────────────┐   │
│  │ int_meters_unified   │  │ int_sensors_unified      │   │
│  └──────────────────────┘  └──────────────────────────┘   │
│        (Combine both systems into unified metrics)          │
└─────────────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                    MARTS LAYER (dbt)                         │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ 5-Minute Fact Tables:                                  │ │
│  │ • mart_inverter_performance_5min                       │ │
│  │ • mart_string_performance_5min                        │ │
│  │ • mart_meter_performance_5min                       │ │
│  │ • mart_sensor_measurements_5min                       │ │
│  └───────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Daily Fact Tables:                                      │ │
│  │ • mart_site_performance_daily                          │ │
│  │ • mart_inverter_performance_daily                      │ │
│  │ • mart_string_performance_daily                        │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                      POWERBI                                 │
│  • Connect to 7 marts tables (not 100+ views)             │
│  • Star schema ready                                       │
│  • Time intelligence enabled                               │
└─────────────────────────────────────────────────────────────┘
```

## Installation & Setup

### Prerequisites
- PostgreSQL with TimescaleDB (already exists)
- Python 3.8+
- Raw data already being collected (existing harvesters)

### Step 1: Install dbt
```bash
pip install dbt-postgres
```

### Step 2: Configure Database Connection
Edit `profiles.yml` or create `~/.dbt/profiles.yml`:

```yaml
mmsr_solar:
  outputs:
    dev:
      type: postgres
      host: localhost  # Update as needed
      user: postgres
      password: "{{ env_var('DB_PASSWORD') }}"  # Use environment variable
      port: 5432
      dbname: solar_data
      schema: public
      threads: 4
      
  target: dev
```

### Step 3: Load Configuration Data
```bash
dbt seed
```
This loads the `seed_metric_mapper.csv` which maps measurement points to human-readable names.

### Step 4: Build the Data Warehouse
```bash
# Run all transformations
dbt run

# Or run in stages:
dbt run --select staging       # First: Stage raw data
dbt run --select dimensions     # Second: Build dimensions
dbt run --select intermediate   # Third: Unify systems
dbt run --select marts         # Fourth: Create final tables
```

### Step 5: Generate Documentation
```bash
dbt docs generate
dbt docs serve
```
Open http://localhost:8080 to view interactive documentation.

### Step 6: Test Data Quality
```bash
dbt test
```

## Key Features

### 1. Unified Data Model
- **Before**: Separate views for each site × device type
- **After**: Single view per device type across all sites

Example:
```sql
-- Before: You needed
garuda_metalindo_1_inverter_data
garuda_metalindo_2_inverter_data
charoen_pokphand_bandung_inverter_data
... (100+ more)

-- After: One unified table
SELECT * FROM mart_inverter_performance_5min
WHERE site_name = 'Garuda Metalindo 1'
```

### 2. Configuration-Driven
- Add new sites without writing SQL
- Metrics mapped via CSV (`seed_metric_mapper.csv`)
- Metadata enriched automatically

### 3. Incremental Loading
dbt can build incrementally to save time:

```yaml
# models/marts/mart_inverter_performance_5min.sql
{{ config(
    materialized='incremental',
    unique_key='timestamp_5min || asset_id || metric_id',
    incremental_strategy='delete+insert'
) }}
```

### 4. Data Quality Tests
Add tests to `models/schema.yml`:

```yaml
models:
  - name: mart_inverter_performance_5min
    columns:
      - name: metric_value
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000
```

## Migration Path

### Phase 1: Parallel Operation (Week 1-2)
1. Keep existing views running
2. Set up dbt on schedule (daily/hourly)
3. Both systems operational

### Phase 2: Testing (Week 3-4)
1. Compare dbt marts vs existing views
2. Validate data accuracy
3. Performance testing

### Phase 3: Cutover (Week 5)
1. Point PowerBI to marts tables
2. Monitor for issues
3. Deprecate old view creation scripts

### Phase 4: Cleanup (Week 6+)
1. Remove per-site view SQL files
2. Simplify Python harvesters
3. Document new process

## Expected Benefits

| Metric | Before | After | Improvement |
|--------|-------|-------|-------------|
| Views/Tables | 100+ | 7 marts | 93% reduction |
| SQL Files | 20+ per site | 1 per device type | 95% reduction |
| Maintenance | High | Low | 80% faster |
| Query Performance | Slow (views) | Fast (tables) | 3-5x faster |
| Onboarding | Complex | Simple | Easier for new team members |
| Testing | None | Automated | 100% coverage |
| Documentation | None | Auto-generated | Complete |

## Troubleshooting

### Issue: "Could not find profile 'mmsr_solar'"
**Solution**: Create `~/.dbt/profiles.yml` with database credentials

### Issue: "relation does not exist"
**Solution**: Run `dbt run --select staging+` to build dependencies

### Issue: "No data in marts"
**Solution**: Check source tables exist and have data

### Issue: "Performance slow"
**Solution**: Add indexes, use incremental materialization, partition large tables

## Next Steps

1. **Review the generated files** in `models/` directory
2. **Customize metrics** in `seed_metric_mapper.csv`
3. **Add tests** for data quality
4. **Schedule dbt runs** (via Airflow, cron, or dbt Cloud)
5. **Connect PowerBI** to marts schema

## Support

- dbt documentation: https://docs.getdbt.com
- Project README: `README_dbt.md`
- This guide: `docs/DBT_MIGRATION_GUIDE.md`

