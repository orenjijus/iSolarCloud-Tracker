# MMSR Prefect Orchestration

This directory contains the Prefect setup for orchestrating the daily MMSR data pipeline.

## Overview

The pipeline runs daily at 01:00 WIB (Asia/Jakarta timezone) and performs:
1. Data ingestion from FusionSolar and iSolarCloud APIs (parallel)
2. Data transformation using dbt following the flow:
   - raw → staging (unpivoted data)
   - staging → mart (device measurements 5min)
   - mart (measurements) → facts (device calculations 5min)
   - mart (measurements) + facts (calculations) → mart (daily aggregations)
   - mart (daily) + facts (site calculations) → mart (site performance daily)
3. Data quality tests
4. Status logging for Power BI refresh

## Why Prefect?

Prefect is chosen as an alternative to Airflow because:
- ✅ Runs natively on Windows Server (no virtualization required)
- ✅ Easy to install and configure
- ✅ Modern Python-native orchestration
- ✅ Built-in scheduling and retry logic
- ✅ Web UI for monitoring
- ✅ Simple deployment model

## Directory Structure

```
prefect/
├── flows/
│   └── mmsr_daily_pipeline.py    # Main Prefect flow
├── requirements.txt               # Python dependencies
├── deploy.py                      # Deployment script
└── README.md                      # This file
```

## Setup

### Prerequisites

1. Python 3.8+ installed
2. PostgreSQL database (for Prefect metadata, optional - can use SQLite for local)
3. Access to MMSR database (for data ingestion)
4. dbt installed and configured
5. Environment variables set (see below)

### Installation

1. **Install Prefect and dependencies:**

```bash
cd prefect
pip install -r requirements.txt
```

2. **Set environment variables:**

Create a `.env` file in the project root or set environment variables:

```bash
# Database connection for MMSR
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

3. **Initialize Prefect (optional - for local development):**

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

### Running Locally

1. **Start Prefect server (optional - for local development):**

```bash
prefect server start
```

This starts:
- Prefect API server at http://127.0.0.1:4200
- Prefect UI at http://127.0.0.1:4200

2. **Run the flow manually:**

```bash
cd prefect
python flows/mmsr_daily_pipeline.py
```

3. **Or run via Prefect CLI:**

```bash
prefect deployment run mmsr-daily-pipeline/mmsr-daily-pipeline-production
```

## Production Deployment

### Option 1: Prefect Cloud (Recommended)

1. **Sign up for Prefect Cloud** (free tier available): https://app.prefect.cloud

2. **Login:**

```bash
prefect cloud login
```

3. **Create deployment:**

```bash
cd prefect
python deploy.py
```

4. **Start work queue:**

```bash
prefect work-queue create mmsr-queue
prefect worker start --pool mmsr-queue
```

### Option 2: Self-Hosted Prefect Server

1. **Start Prefect server as Windows Service:**

Create a batch file `start_prefect_server.bat`:

```batch
@echo off
cd C:\path\to\prefect
C:\path\to\python.exe -m prefect server start --host 0.0.0.0 --port 4200
```

2. **Create Windows Scheduled Task for worker:**

- Open Task Scheduler
- Create Basic Task
- Trigger: Daily at 00:55 (5 minutes before pipeline runs)
- Action: Start a program
- Program: `C:\path\to\python.exe`
- Arguments: `-m prefect worker start --pool mmsr-queue`

3. **Deploy the flow:**

```bash
cd prefect
python deploy.py
```

### Option 3: Windows Service (Most Reliable)

1. **Install NSSM (Non-Sucking Service Manager):**

Download from: https://nssm.cc/download

2. **Create service for Prefect server:**

```batch
nssm install PrefectServer "C:\path\to\python.exe" "-m prefect server start --host 0.0.0.0 --port 4200"
nssm set PrefectServer AppDirectory "C:\path\to\MMSR-API-Server-MA\prefect"
nssm set PrefectServer DisplayName "Prefect Server"
nssm set PrefectServer Description "Prefect orchestration server for MMSR pipeline"
nssm start PrefectServer
```

3. **Create service for Prefect worker:**

```batch
nssm install PrefectWorker "C:\path\to\python.exe" "-m prefect worker start --pool mmsr-queue"
nssm set PrefectWorker AppDirectory "C:\path\to\MMSR-API-Server-MA\prefect"
nssm set PrefectWorker DisplayName "Prefect Worker"
nssm set PrefectWorker Description "Prefect worker for MMSR pipeline"
nssm start PrefectWorker
```

## Monitoring

### Prefect UI

Access Prefect UI at:
- Local: http://localhost:4200
- Production: http://your-server-ip:4200

Features:
- View flow runs and status
- Check task logs
- Monitor execution times
- Set up alerts

### Status Logs

Pipeline completion status is logged to:
- `logs/mmsr_pipeline_status_YYYYMMDD_HHMMSS.txt`

Each log file contains:
- Timestamp
- Status (SUCCESS/FAILED)
- Execution time
- Rows inserted per platform
- Flow run metadata

## Flow Structure

The flow follows this structure:

```
prepare_date_window
    ↓
    ├──→ ingest_fusionsolar ─┐
    └──→ ingest_isolarcloud ─┤ (parallel)
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
7. **dbt_run_marts_daily**: Runs dbt mart models for daily aggregations
8. **dbt_test_critical**: Runs critical data quality tests
9. **log_completion_status**: Logs pipeline completion status to file

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure project root is in Python path
2. **Database connection errors**: Verify environment variables and database accessibility
3. **dbt command not found**: Ensure dbt is installed and in PATH
4. **Prefect server not starting**: Check if port 4200 is available

### Testing

Test individual tasks:

```python
from flows.mmsr_daily_pipeline import prepare_date_window, ingest_fusionsolar_task

# Test date window
date_window = prepare_date_window()

# Test ingestion
result = ingest_fusionsolar_task(date_window)
```

### Debugging

- Check Prefect UI for flow run details
- View task logs in Prefect UI
- Check status log files in `logs/` directory
- Verify environment variables are set correctly

## Comparison with Airflow

| Feature | Airflow | Prefect |
|---------|---------|---------|
| Windows Support | Limited (needs virtualization) | ✅ Native |
| Setup Complexity | Medium | ✅ Low |
| Web UI | ✅ Yes | ✅ Yes |
| Scheduling | ✅ Cron-based | ✅ Cron-based |
| Retry Logic | ✅ Yes | ✅ Yes |
| Task Dependencies | ✅ DAG-based | ✅ Flow-based |
| Monitoring | ✅ Good | ✅ Good |

## Migration from Airflow

The Prefect implementation maintains the same:
- Task structure and dependencies
- Data flow logic
- Error handling
- Logging format

Main differences:
- Uses Prefect flow decorator instead of DAG
- Uses Prefect tasks instead of Airflow operators
- Uses Prefect UI instead of Airflow UI
- Simpler deployment model

## Support

For issues or questions:
- Prefect documentation: https://docs.prefect.io/
- dbt documentation: https://docs.getdbt.com/
- Project documentation in `docs/` directory

