# PV System Data Architecture Setup - Instructions

This document provides step-by-step instructions for setting up the PV system data architecture.

## Prerequisites

- PostgreSQL 13+ with TimescaleDB extension
- Python 3.8+
- dbt-core and dbt-postgres
- Apache Airflow (optional, for orchestration)

## Step 1: Database Design

### 1.1 Create Database

```sql
CREATE DATABASE pv_systems;
\c pv_systems
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### 1.2 Create Tables

#### Raw API Data Tables
1. `raw_fusionsolar_performance` - Raw performance data from FusionSolar
2. `raw_isolarcloud_performance` - Raw performance data from iSolarCloud
3. `raw_fusionsolar_metadata` - Device metadata from FusionSolar
4. `raw_isolarcloud_metadata` - Device metadata from iSolarCloud
5. `raw_fusionsolar_site_metadata` - Site metadata from FusionSolar
6. `raw_isolarcloud_site_metadata` - Site metadata from iSolarCloud

#### Google Sheets Integration
7. `gSheet_device_metadata` - Device metadata from Google Sheets
8. `gSheet_meter_config` - Meter configuration from Google Sheets
9. `gSheet_sensor_config` - Sensor configuration from Google Sheets

## Step 2: ETL Pipeline Setup

### 2.1 Set up dbt Project Structure

```bash
# Create project directory
mkdir pv_analytics
cd pv_analytics

# Initialize dbt project
dbt init pv_analytics
cd pv_analytics

# Create model directories
mkdir -p models/{staging,intermediate,mart,staging/fusionsolar,staging/isolarcloud}

# Create schema files
touch models/staging/schema.yml
models/intermediate/schema.yml
models/mart/schema.yml
```

### 2.2 Configure dbt Models

#### Staging Layer (`models/staging/`)
- Platform-specific models (FusionSolar and iSolarCloud)
- Unpivot and normalize raw data
- Naming convention: `stg_<source>_<entity>_<granularity>`
  - Example: `stg_fusionsolar_perf_5min`
  - Example: `stg_isolarcloud_devices`

#### Intermediate Layer (`models/intermediate/`)
- Cross-platform unified streams
- Combine data from different sources
- Naming convention: `int_<entity>_unified_<granularity>`
  - Example: `int_inverters_unified_5min`
  - Example: `int_sensors_unified_5min`

#### Mart Layer (`models/mart/`)
- Business logic and aggregations
- Ready for consumption by BI tools
- Naming convention: `mart_<entity>_<granularity>`
  - Example: `mart_inverter_performance_5min`
  - Example: `mart_site_performance_daily`

### 2.3 Configure dbt Profiles

Update `~/.dbt/profiles.yml`:

```yaml
pv_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: your_username
      password: your_password
      port: 5432
      dbname: pv_systems
      schema: analytics
      threads: 4
```

## Step 3: API Integration

### 3.1 Set up FastAPI Application

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Example endpoint
@app.get("/api/v1/pv-systems/{system_id}")
async def get_pv_system(system_id: str):
    # Implementation here
    return {"system_id": system_id}
```

## Step 4: Documentation

### 4.1 Generate Schema Documentation

Use `dbdocs` or similar tool to generate schema documentation:

```bash
npm install -g dbdocs
dbdocs build 'postgresql://user:password@localhost:5432/pv_systems' --project pv-systems
```

## Validation

Run the following checks to validate your setup:

1. Database connection test
2. dbt model compilation
3. API endpoint testing
4. Data validation queries

## Troubleshooting

Common issues and solutions:

1. **TimescaleDB extension not loading**: Ensure the extension is installed in PostgreSQL
2. **Connection issues**: Verify database credentials and network access
3. **Performance problems**: Check indexes and query plans

## Next Steps

1. Set up monitoring for the database
2. Implement data backup strategy
3. Set up user access controls
