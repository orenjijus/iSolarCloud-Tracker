# API to Database Run-Book

This document explains the data flow from external solar monitoring APIs to our PostgreSQL database, including the extraction, transformation, and loading (ETL) processes.

## Data Sources

The system collects data from two primary API sources:

1. **iSolarCloud API**
   - Provides access to solar power station data, device information, and time-series measurements
   - Authentication handled by API client in `isolarcloud_harvester_src`

2. **FusionSolar API**
   - Alternative data source for Huawei solar installations
   - Authentication and data retrieval handled in `fusionsolar_harvester_src`

## Data Flow

```
  ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
  │ External APIs │  ─────▶  │ ETL Process   │  ─────▶  │  PostgreSQL   │
  │ (Solar Data)  │          │ (Python)      │          │  Database     │
  └───────────────┘          └───────────────┘          └───────────────┘
                                     │                          │
                                     ▼                          ▼
                              ┌───────────────┐          ┌───────────────┐
                              │ Data          │          │ SQL Views     │
                              │ Transformation │         │ (Analytics)   │
                              └───────────────┘          └───────────────┘
```

## Pipeline Components

### 1. API Data Extraction

#### iSolarCloud Pipeline:
- **Power Stations Sync**: `sync_power_stations()` in `isolar_db_operations.py`
- **Devices Sync**: `sync_devices()` in `isolar_db_operations.py`
- **Historical Data Fetch**: `fetch_historical_data()` in `isolar_data_processing.py`

#### FusionSolar Pipeline:
- Similar structure in `fusionsolar_harvester_src` modules

### 2. Data Processing

- Data is typically fetched in 5-minute intervals
- JSON responses are parsed and transformed into structured formats
- For inverters, both summary-level and string-level data is extracted

### 3. Database Storage

- Data is stored using SQLAlchemy with Upsert operations (INSERT ... ON CONFLICT DO UPDATE)
- Time-series measurements are stored in JSON format in the `measurement_data` column
- Primary keys ensure no duplicate records

### 4. View Generation

- `site_device_views.py` creates SQL views to extract specific measurements from JSON data
- `create_full_day_views.py` generates complete time series for each day (00:00-23:55)
- Pivoted views transform data into a column-based format for easier analysis

## Command-Line Utilities

### Data Fetching

1. **Fetch Historical Data**:
   ```bash
   python isolarcloud/fetch_historical_device_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--ps-id ID] [--device-type TYPE]
   ```

2. **Fetch Inverter-Level Data**:
   ```bash
   python isolarcloud/fetch_inverter_level_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD [options]
   ```

### View Creation

1. **Create Device Views**:
   ```bash
   python isolarcloud/site_device_views.py [--sites SITE1,SITE2] [--no-inverter] [--no-inverter-summary] [--no-meter] [--no-meteo]
   ```

## Extending the Pipeline

To extend the pipeline for new data sources or device types:

1. **Add a new data source**:
   - Create a new directory structure (e.g., `new_source/new_source_harvester_src/`)
   - Implement API client, database operations, and data processing modules
   - Follow the pattern established in existing sources

2. **Add a new device type**:
   - Update the device type mapping in the data processing modules
   - Define measurement points for the new device type
   - Add view creation logic in `site_device_views.py`

3. **Add a new site**:
   - Add site configuration to `ISOLARCLOUD_SITE_MEASURING_POINTS` in `site_device_views.py`
   - Define the measurement points to extract for each device type

## Best Practices

1. **Error Handling**:
   - All API requests include proper error handling and retries
   - Database operations use transactions to ensure data integrity

2. **Data Consistency**:
   - The `fetch_inverter_level_data.py` script ensures inverter summary data doesn't overwrite string-level measurements
   - Full day views ensure no time points are skipped, providing consistent time series

3. **Performance Optimization**:
   - Batch processing for large data fetches
   - SQL views optimize query performance for analytics

## Monitoring and Maintenance

1. **Data Validation**:
   - Use `check_db.py` to verify data integrity
   - `verify_views.py` ensures SQL views are properly created

2. **Logging**:
   - All modules use Python's logging framework
   - Check logs for errors or warnings during data fetching
