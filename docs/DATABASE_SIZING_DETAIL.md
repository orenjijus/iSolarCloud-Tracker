# Database Table Sizing Details

*Last Updated: Generated from PostgreSQL Statistics*

## Executive Summary

This document provides detailed sizing information for all tables in the MMSR Solar Data database, including row counts, data sizes, index sizes, and average row lengths.

### Overall Statistics

| Metric | Value |
|--------|-------|
| **Total Tables Analyzed** | 26 |
| **Total Data Size** | ~147 GB |
| **Total Index Size** | ~24 GB |
| **Total Storage** | ~171 GB |
| **Total Rows** | 1,345,776,142 |

### Largest Tables by Total Size

| Table Name | Total Size | Data Size | Index Size | Row Count | Avg Row Length |
|------------|------------|-----------|------------|-----------|----------------|
| `mart_inverter_performance_5min` | 52 GB | 43 GB | 8.9 GB | 433,277,857 | 107.50 bytes |
| `mart_string_performance_5min` | 46 GB | 39 GB | 6.8 GB | 332,823,401 | 126.48 bytes |
| `stg_fusionsolar__perf_unpivoted` | 38 GB | 38 GB | 0 bytes | 328,988,295 | 122.63 bytes |
| `stg_isolarcloud__perf_unpivoted` | 22 GB | 22 GB | 0 bytes | 177,900,166 | 132.87 bytes |
| `mart_meter_performance_5min` | 10 GB | 9.7 GB | 993 MB | 57,407,544 | 176.91 bytes |

---

## Detailed Table Breakdown

### Schema: `public`

All tables are currently stored in the `public` schema.

#### Fact Tables (Marts) - High-Volume Data

##### `mart_inverter_performance_5min`
- **Purpose**: 5-minute aggregated inverter performance data
- **Row Count**: 433,277,857
- **Data Size**: 43 GB (46,575,648,768 bytes)
- **Index Size**: 8.9 GB (9,306,603,520 bytes)
- **Total Size**: 52 GB (55,895,162,880 bytes)
- **Average Row Length**: 107.50 bytes
- **Storage Efficiency**: Data:Index ratio ≈ 5:1
- **Notes**: Largest table in database, heavily indexed for query performance

##### `mart_string_performance_5min`
- **Purpose**: 5-minute aggregated string-level performance data
- **Row Count**: 332,823,401
- **Data Size**: 39 GB (42,096,132,096 bytes)
- **Index Size**: 6.8 GB (7,161,208,832 bytes)
- **Total Size**: 46 GB (49,269,006,336 bytes)
- **Average Row Length**: 126.48 bytes
- **Storage Efficiency**: Data:Index ratio ≈ 5.9:1

##### `mart_meter_performance_5min`
- **Purpose**: 5-minute aggregated meter performance data
- **Row Count**: 57,407,544
- **Data Size**: 9.7 GB (10,155,982,848 bytes)
- **Index Size**: 993 MB (1,041,235,968 bytes)
- **Total Size**: 10 GB (11,200,053,248 bytes)
- **Average Row Length**: 176.91 bytes
- **Storage Efficiency**: Data:Index ratio ≈ 9.8:1
- **Notes**: Highest average row length among marts, likely contains more metric columns

##### `mart_sensor_measurements_5min`
- **Purpose**: 5-minute aggregated sensor measurement data
- **Row Count**: 21,745,812
- **Data Size**: 3.6 GB (3,813,146,624 bytes)
- **Index Size**: 559 MB (586,129,408 bytes)
- **Total Size**: 4.2 GB (4,400,365,568 bytes)
- **Average Row Length**: 175.35 bytes
- **Storage Efficiency**: Data:Index ratio ≈ 6.5:1

##### `mart_string_performance_daily`
- **Purpose**: Daily aggregated string performance data
- **Row Count**: 602,967
- **Data Size**: 98 MB (102,760,448 bytes)
- **Index Size**: 8.6 MB (8,847,360 bytes)
- **Total Size**: 107 MB (111,673,344 bytes)
- **Average Row Length**: 170.42 bytes

##### `mart_inverter_performance_daily`
- **Purpose**: Daily aggregated inverter performance data
- **Row Count**: 604,479
- **Data Size**: 94 MB (98,566,144 bytes)
- **Index Size**: 8.8 MB (8,994,816 bytes)
- **Total Size**: 103 MB (107,618,304 bytes)
- **Average Row Length**: 163.06 bytes

##### `mart_site_performance_daily`
- **Purpose**: Daily aggregated site-level performance data
- **Row Count**: 0 (empty table)
- **Data Size**: 0 bytes
- **Index Size**: 24 kB (24,576 bytes)
- **Total Size**: 32 kB (32,768 bytes)
- **Status**: ⚠️ **Empty table - may need data population**

##### `mart_simulation_targets_daily`
- **Purpose**: Daily simulation targets for comparison
- **Row Count**: 3,286
- **Data Size**: 744 kB (761,856 bytes)
- **Index Size**: 208 kB (212,992 bytes)
- **Total Size**: 992 kB (1,015,808 bytes)
- **Average Row Length**: 231.85 bytes

---

#### Staging Tables (Unpivoted Raw Data)

##### `stg_fusionsolar__perf_unpivoted`
- **Purpose**: Unpivoted FusionSolar performance data (staging layer)
- **Row Count**: 328,988,295
- **Data Size**: 38 GB (40,343,961,600 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 38 GB (40,355,143,680 bytes)
- **Average Row Length**: 122.63 bytes
- **Materialization**: View (as per dbt config)
- **Notes**: No indexes as it's a view, sourced from raw data

##### `stg_isolarcloud__perf_unpivoted`
- **Purpose**: Unpivoted iSolarCloud performance data (staging layer)
- **Row Count**: 177,900,166
- **Data Size**: 22 GB (23,637,000,192 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 22 GB (23,643,570,176 bytes)
- **Average Row Length**: 132.87 bytes
- **Materialization**: View (as per dbt config)

---

#### Intermediate Tables

##### `int_meters_unified`
- **Purpose**: Unified meter data from all sources (intermediate layer)
- **Row Count**: 55,173,815
- **Data Size**: 7.0 GB (7,317,487,616 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 7.0 GB (7,319,543,808 bytes)
- **Average Row Length**: 132.63 bytes
- **Materialization**: Table (as per dbt config)
- **Notes**: Consider adding indexes if used in joins frequently

##### `int_sensors_unified`
- **Purpose**: Unified sensor data from all sources (intermediate layer)
- **Row Count**: 21,755,509
- **Data Size**: 2.8 GB (2,903,506,944 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 2.8 GB (2,904,342,528 bytes)
- **Average Row Length**: 133.46 bytes
- **Materialization**: Table (as per dbt config)

---

#### Dimension Tables

##### `dim_assets`
- **Purpose**: Unified asset dimension table (sites + devices from both systems)
- **Row Count**: 431
- **Data Size**: 128 kB (131,072 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 136 kB (139,264 bytes)
- **Average Row Length**: 304.11 bytes
- **Materialization**: Table (as per dbt config)
- **Notes**: Small dimension table, may benefit from indexes for join performance

##### `dim_date_generated`
- **Purpose**: Date dimension table for time-based analysis
- **Row Count**: 2,857
- **Data Size**: 512 kB (524,288 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 552 kB (565,248 bytes)
- **Average Row Length**: 183.51 bytes
- **Materialization**: Table (as per dbt config)

---

#### Seed Tables (Configuration Data)

##### `seed_daily_simulation_target`
- **Purpose**: Daily simulation target values from seed file
- **Row Count**: 3,286
- **Data Size**: 520 kB (532,480 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 560 kB (573,440 bytes)
- **Average Row Length**: 162.05 bytes

##### `seed_metric_mapper`
- **Purpose**: Metric mapping configuration (source to unified metric names)
- **Row Count**: 208
- **Data Size**: 24 kB (24,576 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 56 kB (57,344 bytes)
- **Average Row Length**: 118.15 bytes

##### `seed_sensor_config`
- **Purpose**: Sensor configuration data
- **Row Count**: 117
- **Data Size**: 16 kB (16,384 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 48 kB (49,152 bytes)
- **Average Row Length**: 140.03 bytes

##### `seed_meter_config`
- **Purpose**: Meter configuration data
- **Row Count**: 128
- **Data Size**: 16 kB (16,384 bytes)
- **Index Size**: 0 bytes (no indexes)
- **Total Size**: 48 kB (49,152 bytes)
- **Average Row Length**: 128.00 bytes

---

#### Source System Tables (Raw Data)

##### `isolarcloud_devices`
- **Purpose**: iSolarCloud device metadata
- **Row Count**: 232
- **Data Size**: 40 kB (40,960 bytes)
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 88 kB (90,112 bytes)
- **Average Row Length**: 176.55 bytes

##### `fusionsolar_devices`
- **Purpose**: FusionSolar device metadata
- **Row Count**: 182
- **Data Size**: 32 kB (32,768 bytes)
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 80 kB (81,920 bytes)
- **Average Row Length**: 180.04 bytes

##### `isolarcloud_power_stations`
- **Purpose**: iSolarCloud power station (site) metadata
- **Row Count**: 9
- **Data Size**: 8 kB (8,192 bytes)
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 32 kB (32,768 bytes)
- **Average Row Length**: 910.22 bytes

##### `fusionsolar_plants`
- **Purpose**: FusionSolar plant (site) metadata
- **Row Count**: 8
- **Data Size**: 8 kB (8,192 bytes)
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 32 kB (32,768 bytes)
- **Average Row Length**: 1,024.00 bytes

##### `isolarcloud_historical_data`
- **Purpose**: Raw historical data from iSolarCloud API (JSONB storage)
- **Row Count**: 0 (empty table)
- **Data Size**: 0 bytes
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 24 kB (24,576 bytes)
- **Status**: ⚠️ **Empty table - historical data likely migrated to staging**

##### `fusionsolar_historical_data`
- **Purpose**: Raw historical data from FusionSolar API (JSONB storage)
- **Row Count**: 0 (empty table)
- **Data Size**: 0 bytes
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 24 kB (24,576 bytes)
- **Status**: ⚠️ **Empty table - historical data likely migrated to staging**

---

#### System Tables (ETL & API Management)

##### `fusionsolar_etl_status`
- **Purpose**: ETL process status tracking
- **Row Count**: 11
- **Data Size**: 8 kB (8,192 bytes)
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 32 kB (32,768 bytes)
- **Average Row Length**: 744.73 bytes

##### `fusionsolar_api_quota_config`
- **Purpose**: API quota configuration
- **Row Count**: 0 (empty table)
- **Data Size**: 0 bytes
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 16 kB (16,384 bytes)
- **Status**: ⚠️ **Empty table**

##### `fusionsolar_api_quota_usage`
- **Purpose**: API quota usage tracking
- **Row Count**: 0 (empty table)
- **Data Size**: 0 bytes
- **Index Size**: 16 kB (16,384 bytes)
- **Total Size**: 16 kB (16,384 bytes)
- **Status**: ⚠️ **Empty table**

##### `fusionsolar_api_request_queue`
- **Purpose**: API request queue management
- **Row Count**: 0 (empty table)
- **Data Size**: 0 bytes
- **Index Size**: 8 kB (8,192 bytes)
- **Total Size**: 16 kB (16,384 bytes)
- **Status**: ⚠️ **Empty table**

---

## Storage Analysis by Category

### By Table Type

| Category | Table Count | Total Size | Data Size | Index Size | Total Rows |
|----------|------------|------------|-----------|------------|------------|
| **Marts (Fact Tables)** | 8 | ~116 GB | ~97 GB | ~17 GB | 813,103,506 |
| **Staging** | 2 | ~60 GB | ~60 GB | 0 bytes | 506,888,461 |
| **Intermediate** | 2 | ~9.8 GB | ~9.8 GB | 0 bytes | 76,929,324 |
| **Dimensions** | 2 | ~688 kB | ~640 kB | 0 bytes | 3,288 |
| **Seeds** | 4 | ~712 kB | ~576 kB | 0 bytes | 3,739 |
| **Source System** | 6 | ~296 kB | ~120 kB | ~80 kB | 431 |
| **System/ETL** | 4 | ~96 kB | ~8 kB | ~64 kB | 11 |

### Storage Efficiency Metrics

- **Total Storage**: ~171 GB
- **Data Storage**: ~147 GB (86% of total)
- **Index Storage**: ~24 GB (14% of total)
- **Average Data:Index Ratio**: ~6:1

### Largest Tables Requiring Attention

1. **`mart_inverter_performance_5min`** (52 GB)
   - Consider partitioning if growth continues
   - Monitor index maintenance overhead

2. **`mart_string_performance_5min`** (46 GB)
   - Similar considerations as inverter table
   - Review data retention policies

3. **`stg_fusionsolar__perf_unpivoted`** (38 GB)
   - Currently a view, verify materialization strategy
   - Consider if materialized view would be beneficial

4. **`stg_isolarcloud__perf_unpivoted`** (22 GB)
   - Similar considerations as FusionSolar staging

---

## Recommendations

### 1. Index Strategy
- ✅ Well-indexed: Major mart tables have appropriate indexes
- ⚠️ Missing indexes: Intermediate tables (`int_meters_unified`, `int_sensors_unified`) could benefit from indexes if frequently joined
- ⚠️ Dimension tables: Consider adding indexes on `dim_assets` and `dim_date_generated` for faster joins

### 2. Table Maintenance
- ⚠️ Empty tables: Review and either populate or archive:
  - `mart_site_performance_daily`
  - `isolarcloud_historical_data`
  - `fusionsolar_historical_data`
  - `fusionsolar_api_quota_config`
  - `fusionsolar_api_quota_usage`
  - `fusionsolar_api_request_queue`

### 3. Partitioning Considerations
- Large mart tables (>40 GB) may benefit from partitioning:
  - Time-based partitioning for 5-minute tables
  - Daily tables already at manageable size

### 4. Vacuum & Analyze
- Regular VACUUM ANALYZE recommended for large tables
- Monitor bloat on heavily updated tables

### 5. Data Retention Policy
- Establish retention policies for 5-minute data
- Consider archiving old data to reduce size of fact tables

---

## SQL Query for Regeneration

To regenerate this report, run:

```sql
SELECT 
    schemaname,
    relname AS tablename,
    n_live_tup AS rowcountestimate,
    pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) AS datasize_readable,
    pg_relation_size(schemaname||'.'||relname) AS datasizebytes,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||relname)) AS indexsize_readable,
    pg_indexes_size(schemaname||'.'||relname) AS indexsizebytes,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS totalsize_readable,
    pg_total_relation_size(schemaname||'.'||relname) AS totalsizebytes,
    CASE 
        WHEN n_live_tup > 0 THEN 
            ROUND((pg_relation_size(schemaname||'.'||relname)::numeric / n_live_tup), 2)
        ELSE 0
    END AS avgrowlengthbytes
FROM
    pg_stat_user_tables
WHERE
    schemaname NOT IN ('information_schema', 'pg_catalog', '_timescaledb_cache', '_timescaledb_catalog', '_timescaledb_config', '_timescaledb_debug', '_timescaledb_internal', 'timescaledb_information', 'timescaledb_experimental', '_timescaledb_functions')
ORDER BY
    schemaname, totalsizebytes DESC NULLS LAST;
```

---

## Notes

- All sizes are approximations based on PostgreSQL statistics
- Row counts are estimates from `pg_stat_user_tables` (may require VACUUM ANALYZE for accuracy)
- Index sizes include all indexes on the table
- Empty tables may still have metadata overhead (minimum 8-16 kB)
- Storage efficiency calculations assume standard PostgreSQL row overhead

---

*Document generated automatically from PostgreSQL system catalogs*

