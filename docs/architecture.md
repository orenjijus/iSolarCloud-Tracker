# PV System Data Architecture

## 1. High-Level Architecture

### 1.1 System Overview
The architecture is designed to handle large-scale PV system data with a focus on time-series analytics and cross-platform data integration. The system leverages PostgreSQL with TimescaleDB for time-series optimization, dbt for data transformation, and Airflow for workflow orchestration.

### 1.2 Architecture Diagram
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│                 │    │                 │    │                 │
│  Data Sources   ├───►│  Ingestion     ├───►│  PostgreSQL     │
│  (FusionSolar,  │    │  Layer         │    │  + TimescaleDB  │
│   iSolarCloud,  │    │  (Airflow)     │    └────────┬────────┘
│   Google Sheets)│    │                 │             │
└─────────────────┘    └─────────────────┘             │
                                                      ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│                 │    │                 │    │                 │
│  BI & Analytics │◄───┤  dbt Models    ◄├────┤  Staging Area   │
│  Tools          │    │  (Transforms)   │    │  (Raw Data)     │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 2. Data Layer Architecture

### 2.1 Database System
- **Primary Database**: PostgreSQL 13+ with TimescaleDB extension
- **Optimization**: Hypertables for time-series data
- **Schema**: Multi-layered approach (raw, staging, marts)

### 2.2 Data Storage Layers
1. **Raw Layer**:
   - `raw_fusionsolar_*`: Raw data from FusionSolar API
   - `raw_isolarcloud_*`: Raw data from iSolarCloud API
   - `gSheet_*`: Configuration data from Google Sheets

2. **Staging Layer**:
   - Source-specific transformations
   - Data type standardization
   - Basic validation

3. **Mart Layer**:
   - Business-ready aggregates
   - Cross-source unified views
   - Performance-optimized for analytics

## 3. Data Processing Pipeline

### 3.1 Data Flow Architecture

1. **Raw Data Layer**
   - `raw_isolarcloud_performance`: Time-series data from iSolarCloud
   - `raw_fusionsolar_performance`: Time-series data from FusionSolar
   - `raw_isolarcloud_metadata`: Device and site metadata from iSolarCloud
   - `raw_fusionsolar_metadata`: Device and site metadata from FusionSolar

2. **Staging Layer (Views)**
   - `stg_isolarcloud__perf_unpivoted`: Unpivoted iSolarCloud metrics
   - `stg_fusionsolar__perf_unpivoted`: Unpivoted FusionSolar metrics
   - `stg_*_sites`: Site metadata views
   - `stg_*_devices`: Device metadata views

3. **Intermediate Layer**
   - `int_meters_unified`: Standardized meter data
   - `int_sensors_unified`: Standardized sensor data
   - `int_inverters_unified_5min`: 5-minute inverter metrics
   - `int_strings_unified_5min`: 5-minute string-level metrics

4. **Mart Layer**
   - `mart_site_performance_daily`: Daily site aggregations
   - `mart_inverter_performance_5min`: Granular inverter data
   - `mart_string_performance_5min`: Granular string data
   - `mart_meter_performance_5min`: Granular meter data
   - `mart_simulation_targets_daily`: Expected performance metrics

### 3.2 Data Quality & Monitoring
- Automated validation of data completeness
- Cross-source consistency checks
- Performance anomaly detection
- Data freshness monitoring
- Automated alerting for data issues

## 4. Integration Points

### 4.1 Data Sources
1. **FusionSolar**:
   - Site Metadata
   - Device status and configuration
   - Historical data (measurement key-value pairs)

2. **iSolarCloud**:
   - Site Metadata
   - Device status and configurations
   - Historical data (measurement key-value pairs)

3. **Google Sheets**:
   - Device metadata (meter type, sensor type, inverter-string-poa mapping)
   - Configuration overrides
   - Manual data entry

## 5. Development Setup

### 5.1 Prerequisites
- PostgreSQL 13+ with TimescaleDB
- Python 3.8+
- dbt-core and dbt-postgres
- Apache Airflow (for local development)

### 5.2 Environment Configuration

#### Database Connection
```bash
# PostgreSQL Configuration (Shared by both systems)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=MMSR
POSTGRES_USER=postgres
POSTGRES_PASSWORD= [REDACTED]

# FusionSolar API
FUSION_BASE_URL=https://sg5.fusionsolar.huawei.com
FUSIONSOLAR_USERNAME= [REDACTED]
FUSIONSOLAR_PASSWORD= [REDACTED]

# iSolarCloud API
ISOLARCLOUD_APP_KEY= [REDACTED]
ISOLARCLOUD_SECRET_KEY= [REDACTED]
ISOLARCLOUD_USERNAME= [REDACTED]
ISOLARCLOUD_PASSWORD= [REDACTED]

# dbt Configuration
DBT_PROFILES_DIR=~/.dbt
DBT_TARGET=dev
```

### 5.3 Seed File Structure
Seeds are used for configuration and mapping tables that are manually maintained:

1. **seed_metric_mapper.csv**
   - Maps raw metric names to standardized names
   - Defines units and data types
   - Specifies aggregation methods

2. **seed_string_config.csv**
   - Maps strings to inverters and POA orientation
   - Defines string parameters
   - Tracks configuration changes over time

3. **seed_sensor_config_override.csv**
   - Maps physical sensors to logical sensors
   - Defines sensor types and specifications
   - Tracks calibration data

4. **seed_meter_config_override.csv**
   - Defines meter types and configurations
   - Maps meters to electrical circuits
   - Tracks installation parameters

5. **seed_simulation_targets.csv**
   - Defines expected performance metrics
   - Sets target values for comparison
   - Tracks simulation scenarios

## 6. Performance Considerations

### 6.1 Query Optimization
- Time-based partitioning
- Indexing strategy
- Materialized views for common queries

### 6.2 Scaling
- Read replicas for analytics
- Connection pooling
- Query result caching

## 7. Monitoring & Maintenance

### 7.1 Performance Monitoring
- Query performance tracking
- System resource utilization
- Data freshness monitoring

### 7.2 Maintenance Tasks
- Vacuum and analyze
- Index maintenance
- Data retention policies
