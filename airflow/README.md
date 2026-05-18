# MMSR Airflow Orchestration

This directory contains the Airflow setup for orchestrating the daily MMSR data pipeline.

## Overview

The pipeline runs daily at 01:00 WIB (Asia/Jakarta timezone) and performs:
1. Data ingestion from FusionSolar and iSolarCloud APIs (parallel)
2. Data transformation using dbt (staging → intermediate → marts)
3. Data quality tests
4. Status logging for Power BI refresh

## Directory Structure

```
airflow/
├── dags/
│   └── mmsr_daily_pipeline.py    # Main DAG definition
├── plugins/
│   └── mmsr_tasks/                 # Custom task modules
│       ├── __init__.py
│       ├── fusionsolar_ingest.py  # FusionSolar ingestion wrapper
│       ├── isolarcloud_ingest.py  # iSolarCloud ingestion wrapper
│       └── monitoring.py          # Status logging utilities
├── requirements.txt               # Python dependencies
├── docker-compose.yml             # Docker setup for local development
└── README.md                      # This file
```

## Setup

### Prerequisites

1. Python 3.8+ with pip
2. PostgreSQL database (for Airflow metadata)
3. Access to MMSR database (for data ingestion)
4. dbt installed and configured
5. Environment variables set (see below)

### Environment Variables

Create a `.env` file or set the following environment variables:

```bash
# Database connection
POSTGRES_HOST=10.101.4.88
POSTGRES_PORT=5432
POSTGRES_DB=MMSR
POSTGRES_USER=juice
POSTGRES_PASSWORD=your_password

# FusionSolar API credentials
FUSIONSOLAR_USERNAME=your_username
FUSIONSOLAR_PASSWORD=your_password

# iSolarCloud API credentials
ISOLARCLOUD_APP_KEY=your_app_key
ISOLARCLOUD_SECRET_KEY=your_secret_key
ISOLARCLOUD_USERNAME=your_username
ISOLARCLOUD_PASSWORD=your_password
```

### Installation Options

#### Option 1: Docker Compose (Recommended for Local Development)

1. Install Docker and Docker Compose
2. Set environment variables in `.env` file
3. Run:
   ```bash
   cd airflow
   docker-compose up -d
   ```
4. Access Airflow UI at http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

#### Option 2: Self-Managed Airflow

1. Install Airflow:
   ```bash
   pip install -r airflow/requirements.txt
   ```

2. Initialize Airflow database:
   ```bash
   airflow db init
   ```

3. Create admin user:
   ```bash
   airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com \
     --password admin
   ```

4. Set environment variables:
   ```bash
   export AIRFLOW_HOME=/path/to/airflow
   export DATABASE_URL=postgresql://user:pass@host:port/dbname
   # ... other environment variables
   ```

5. Start Airflow:
   ```bash
   airflow webserver --port 8080 &
   airflow scheduler
   ```

## DAG Configuration

The main DAG is defined in `dags/mmsr_daily_pipeline.py`:

- **DAG ID**: `mmsr_daily_pipeline`
- **Schedule**: `0 1 * * *` (01:00 WIB daily)
- **Timezone**: Asia/Jakarta
- **Catchup**: Disabled (no backfill)
- **Max Active Runs**: 1

## Task Flow

```
prepare_date_window
    ↓
    ├──→ ingest_fusionsolar ─┐
    └──→ ingest_isolarcloud ──┤
                              ↓
                        dbt_run_staging
                              ↓
                  dbt_run_marts_measurement_5min
                              ↓
                  dbt_run_facts_calculation_5min
                              ↓
                        dbt_run_marts_daily
                              ↓
                        dbt_test_critical
                              ↓
                        log_completion_status
```

## Task Descriptions

1. **prepare_date_window**: Calculates yesterday's date in Asia/Jakarta timezone
2. **ingest_fusionsolar**: Ingests data from FusionSolar API (parallel with iSolarCloud)
3. **ingest_isolarcloud**: Ingests data from iSolarCloud API (parallel with FusionSolar)
4. **dbt_run_staging**: Runs dbt staging models (unpivoted incremental tables)
5. **dbt_run_marts_measurement_5min**: Runs dbt mart models for device measurements 5min
6. **dbt_run_facts_calculation_5min**: Runs dbt facts models for device calculations 5min
7. **dbt_run_marts_daily**: Runs dbt mart models for daily aggregations (devices + site performance)
8. **dbt_test_critical**: Runs critical data quality tests
9. **log_completion_status**: Logs pipeline completion status to file

## dbt Models Tagging

Models are tagged for selective execution following the data flow:

- `tag:daily_staging`: Staging incremental models (raw → staging)
  - `stg_fusionsolar__perf_unpivoted`
  - `stg_isolarcloud__perf_unpivoted`

- `tag:daily_marts_measurement_5min`: Mart models for device measurements 5min (staging → mart)
  - `mart_inverter_performance_5min`
  - `mart_meter_performance_5min`
  - `mart_sensor_measurements_5min`

- `tag:daily_facts_calculation_5min`: Facts models for device calculations 5min (mart measurements → facts)
  - `fact_inverter_calculations_5min`
  - `fact_sensor_calculations_5min`
  - `fact_site_calculations_5min`

- `tag:daily_marts_daily`: Mart models for daily aggregations (mart 5min + facts → mart daily)
  - `mart_sensor_daily`
  - `mart_site_performance_daily`

- `tag:critical`: Critical data quality tests

## Monitoring

### Status Logs

Pipeline completion status is logged to:
- `logs/mmsr_pipeline_status_YYYYMMDD_HHMMSS.txt`

Each log file contains:
- Timestamp
- Status (SUCCESS/FAILED)
- Execution time
- Rows inserted per platform
- DAG run metadata

### Airflow UI

Monitor pipeline execution in Airflow UI:
- View DAG runs and task status
- Check logs for each task
- View task execution times
- Set up alerts for failures

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure project root and harvester directories are in Python path
2. **Database connection errors**: Verify environment variables and database accessibility
3. **dbt command not found**: Ensure dbt is installed and in PATH
4. **XCom errors**: Check that `prepare_date_window` task completed successfully

### Testing

Test individual tasks:
```bash
# Test date window preparation
airflow tasks test mmsr_daily_pipeline prepare_date_window 2025-01-01

# Test FusionSolar ingestion
airflow tasks test mmsr_daily_pipeline ingest_fusionsolar 2025-01-01

# Test dbt staging
airflow tasks test mmsr_daily_pipeline dbt_run_staging 2025-01-01
```

## Power BI Integration

Since Power BI uses on-premises gateway:
1. Pipeline logs completion status to file
2. Manual trigger Power BI refresh after pipeline completes
3. Or setup scheduled refresh in Power BI Service (after 02:00 WIB)
4. Status file can be used for monitoring/alerting

## Maintenance

### Updating DAG

After modifying DAG files:
- Restart Airflow scheduler (if self-managed)
- Or restart Docker containers (if using Docker Compose)

### Adding New Tasks

1. Create task function in appropriate module
2. Add task to DAG with proper dependencies
3. Update task flow documentation

### Modifying Schedule

Edit `schedule` parameter in DAG definition:
```python
schedule='0 1 * * *'  # Daily at 01:00 WIB
```

## Support

For issues or questions, refer to:
- Airflow documentation: https://airflow.apache.org/docs/
- dbt documentation: https://docs.getdbt.com/
- Project documentation in `docs/` directory

