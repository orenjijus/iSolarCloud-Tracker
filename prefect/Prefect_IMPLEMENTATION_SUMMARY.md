# Prefect Implementation Summary

## Overview

Implementasi Prefect sebagai alternatif Airflow untuk orkestrasi MMSR daily pipeline di Windows Server. Prefect dipilih karena:
- ✅ Berjalan native di Windows (tidak perlu virtualisasi)
- ✅ Mudah diinstall dan dikonfigurasi
- ✅ Modern Python-native orchestration
- ✅ Built-in scheduling dan retry logic
- ✅ Web UI untuk monitoring

## Files Created

### Core Flow Files
- `prefect/flows/mmsr_daily_pipeline.py` - Main Prefect flow dengan semua tasks
- `prefect/flows/test_isolarcloud.py` - Test script untuk iSolarCloud ingestion
- `prefect/deploy.py` - Deployment script dengan schedule

### Configuration Files
- `prefect/requirements.txt` - Python dependencies
- `prefect/__init__.py` - Package init
- `prefect/flows/__init__.py` - Flows package init

### Documentation
- `prefect/README.md` - Comprehensive documentation
- `prefect/SETUP_WINDOWS.md` - Detailed Windows setup guide

### Windows Scripts
- `prefect/setup_windows.bat` - Automated setup script
- `prefect/start_server.bat` - Start Prefect server
- `prefect/start_worker.bat` - Start Prefect worker
- `prefect/run_flow_manual.bat` - Manual flow execution for testing

## Flow Structure

Flow mengikuti struktur yang sama dengan Airflow DAG:

```
prepare_date_window
    ↓
    ├──→ ingest_fusionsolar ─┐
    └──→ ingest_isolarcloud ─┤ (parallel dengan ConcurrentTaskRunner)
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

## Key Features

### 1. Task Definitions
- Semua tasks menggunakan `@task` decorator
- Retry logic: 2 retries dengan 15 menit delay untuk ingestion tasks
- Logging terintegrasi dengan Prefect logger
- Error handling yang proper

### 2. Flow Definition
- Menggunakan `@flow` decorator
- `ConcurrentTaskRunner` untuk parallel execution
- **Important**: Tasks dipanggil secara terpisah (bukan tuple assignment) agar Prefect bisa menjalankannya secara parallel
- Schedule: Daily at 01:00 WIB (Asia/Jakarta)

### 3. Deployment
- Deployment script dengan cron schedule
- Work queue untuk task distribution
- Tags untuk organization

## Comparison dengan Airflow

| Feature | Airflow | Prefect |
|---------|---------|---------|
| Windows Support | ❌ Needs virtualization | ✅ Native |
| Setup Complexity | Medium | ✅ Low |
| Web UI | ✅ Yes | ✅ Yes |
| Scheduling | ✅ Cron-based | ✅ Cron-based |
| Retry Logic | ✅ Yes | ✅ Yes |
| Task Dependencies | ✅ DAG-based | ✅ Flow-based |
| Monitoring | ✅ Good | ✅ Good |
| Deployment | Complex | ✅ Simple |

## Setup Options

### 1. Development/Testing
- Run Prefect server locally
- Run worker manually
- Test flow execution

### 2. Production - Prefect Server + Worker
- Start Prefect server as background process
- Start worker as background process
- Deploy flow with schedule

### 3. Production - Windows Service (Recommended)
- Install Prefect Server as Windows Service (NSSM)
- Install Prefect Worker as Windows Service (NSSM)
- Auto-start on boot
- Auto-restart on failure

### 4. Production - Task Scheduler
- Create Windows Scheduled Tasks
- Start server and worker on boot
- Less reliable than services

## Migration Notes

### From Airflow to Prefect

**Similarities:**
- Task structure dan dependencies sama
- Data flow logic sama
- Error handling sama
- Logging format sama

**Differences:**
- Uses `@flow` decorator instead of DAG
- Uses `@task` decorator instead of operators
- Uses Prefect UI instead of Airflow UI
- Simpler deployment model
- Native Windows support

**Code Changes:**
- Airflow DAG → Prefect Flow
- PythonOperator → Prefect Task
- XCom → Task return values
- Airflow context → Prefect logger

## Next Steps

1. **Install Prefect:**
   ```batch
   cd prefect
   setup_windows.bat
   ```

2. **Set Environment Variables:**
   - Database credentials
   - API credentials

3. **Test Flow:**
   ```batch
   run_flow_manual.bat
   ```

4. **Setup Production:**
   - Choose setup option (Service/Task Scheduler)
   - Deploy flow
   - Monitor first runs

## Testing

### Manual Testing - Full Pipeline
```batch
cd prefect
python flows\mmsr_daily_pipeline.py
```

### Testing Individual Components

#### Test iSolarCloud Only
```batch
cd prefect
python flows\test_isolarcloud.py
```

#### Test Individual Tasks
```python
from prefect.flows.mmsr_daily_pipeline import prepare_date_window, ingest_fusionsolar_task

date_window = prepare_date_window()
result = ingest_fusionsolar_task(date_window)
```

### Testing from Command Line
```python
# Test iSolarCloud task directly
from isolarcloud.tasks import run_isolarcloud_ingest
from datetime import datetime, timedelta

result = run_isolarcloud_ingest(
    (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d'),
    (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
)
```

## Monitoring

### Prefect UI
- Access: http://localhost:4200
- View flow runs
- Check task status
- View logs
- Monitor execution times

### Status Logs
- Location: `logs/mmsr_pipeline_status_*.txt`
- Contains: Status, execution time, rows inserted, metrics

## Recent Fixes and Improvements

### 1. Parallel Execution Fix
**Issue**: FusionSolar dan iSolarCloud tidak berjalan parallel, malah serial
**Fix**: Tasks dipanggil secara terpisah (bukan tuple assignment) agar Prefect bisa menjalankannya secara parallel dengan ConcurrentTaskRunner

**Before (Serial):**
```python
fusionsolar_result, isolarcloud_result = ingest_fusionsolar_task(date_window), ingest_isolarcloud_task(date_window)
```

**After (Parallel):**
```python
fusionsolar_result = ingest_fusionsolar_task(date_window)
isolarcloud_result = ingest_isolarcloud_task(date_window)
```

### 2. Database Engine Initialization (iSolarCloud)
**Issue**: "Database engine not initialized" error di iSolarCloud
**Fix**: Implementasi `ensure_db_initialized()` function untuk re-import engine setelah `init_database()` dipanggil
- File: `isolarcloud/isolarcloud_harvester_src/isolar_data_processing.py`
- Semua fungsi yang menggunakan engine sekarang memanggil `ensure_db_initialized()` terlebih dahulu

### 3. FusionSolar Rate Limiter Bypass
**Issue**: API rate limit errors meskipun sudah ada rate limiter
**Fix**: Rate limiter di-patch untuk bypass (sama seperti `fusionsolar_data_harvester_no_limit.py`)
- File: `fusionsolar/fusionsolar_harvester_src/fusionsolar_data_processing.py`
- Rate limiter methods (`can_make_api_call`, `wait_for_api_call`) di-patch untuk selalu return `True`
- Payload parameter diubah dari `"devIds"` ke `"sns"` untuk kompatibilitas API yang lebih baik
- Response handling diperbaiki untuk handle berbagai format response
- Database insert dipindah ke luar loop untuk efisiensi

### 4. Enhanced Logging
- API request payload logging
- Response summary logging
- Sample data point structure logging
- Detailed error messages untuk debugging

## Troubleshooting

### Common Issues
1. **Import errors** → Check Python path, pastikan project root dan subdirectories ada di `sys.path`
2. **Database connection** → Verify environment variables, pastikan `ensure_db_initialized()` dipanggil
3. **dbt not found** → Install dbt-postgres
4. **Port conflict** → Change port or stop existing service
5. **Tasks tidak parallel** → Pastikan tasks dipanggil secara terpisah, bukan dalam tuple assignment
6. **API rate limit** → Check FusionSolar rate limiter bypass sudah aktif
7. **Database engine None** → Pastikan `init_database()` dipanggil sebelum menggunakan engine

### Debug Steps
1. Test flow manually
2. Test individual components (gunakan `test_isolarcloud.py` untuk iSolarCloud)
3. Check Prefect UI for errors
4. Verify environment variables
5. Check logs in `logs/` directory
6. Verify database initialization dengan `ensure_db_initialized()`
7. Check API response format dengan enhanced logging

## Benefits

1. **Windows Native** - Tidak perlu virtualisasi
2. **Simple Setup** - Mudah diinstall dan configure
3. **Modern** - Python-native, clean API
4. **Reliable** - Built-in retry dan error handling
5. **Monitorable** - Web UI untuk monitoring
6. **Flexible** - Multiple deployment options

## Implementation Notes

### Parallel Execution
Untuk menjalankan tasks secara parallel di Prefect dengan `ConcurrentTaskRunner`, penting untuk:
1. Memanggil tasks secara terpisah (bukan dalam tuple assignment)
2. Tasks tidak boleh dependen satu sama lain
3. `ConcurrentTaskRunner` harus di-set di flow decorator

### Database Initialization Pattern
Untuk menghindari "Database engine not initialized" errors:
1. Gunakan `ensure_db_initialized()` function yang re-import engine setelah init
2. Jangan import engine di module level jika engine di-initialize di runtime
3. Pastikan `init_database()` dipanggil sebelum menggunakan engine

### API Rate Limiting
FusionSolar API memiliki rate limits yang ketat:
1. Rate limiter di-bypass untuk testing/production dengan patch methods
2. Payload menggunakan `"sns"` parameter (lebih reliable dari `"devIds"`)
3. Response handling fleksibel untuk berbagai format response
4. Database insert dilakukan setelah loop selesai untuk efisiensi

## Support

- Prefect documentation: https://docs.prefect.io/
- Setup guide: `SETUP_WINDOWS.md`
- General README: `README.md`
- Test scripts: `prefect/flows/test_isolarcloud.py`

