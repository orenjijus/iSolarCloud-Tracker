# Parallel Processing Implementation for iSolarCloud

## Overview

Parallel processing telah diimplementasikan untuk mempercepat iSolarCloud ingestion dengan memproses 8 batch (3-hour intervals) secara bersamaan.

## Changes Made

### 1. Configuration (`isolarcloud_harvester_src/isolar_config.py`)

Added new configuration parameters:
```python
PARALLEL_PROCESSING_ENABLED = True  # Enable parallel processing for 3-hour intervals
PARALLEL_MAX_WORKERS = 8  # Number of parallel workers (1 day = 8 intervals of 3 hours)
```

### 2. Data Processing (`isolarcloud_harvester_src/isolar_data_processing.py`)

#### Modified Functions:

1. **`fetch_historical_data_for_batch`**
   - Added `parallel` and `max_workers` parameters
   - Implemented parallel processing using `ThreadPoolExecutor`
   - Falls back to sequential processing if `parallel=False` or only 1 interval

2. **`_fetch_historical_data_for_batch_with_custom_mapping`**
   - Added `parallel` and `max_workers` parameters
   - Implemented parallel processing for custom mapping scenarios (e.g., inverter summary data)

#### Key Features:

- **Thread-safe**: SQLAlchemy Session is thread-local, so database operations are safe
- **Error handling**: If one interval fails, other intervals continue processing
- **Backward compatible**: Can be disabled by setting `PARALLEL_PROCESSING_ENABLED = False`
- **Configurable**: Number of workers can be adjusted via `PARALLEL_MAX_WORKERS`

## Expected Performance Improvement

**Before:**
- iSolarCloud ingestion: ~980 detik (~16.3 menit) untuk 1 hari
- Sequential processing: 8 intervals × ~2 menit per interval = ~16 menit

**After:**
- iSolarCloud ingestion: ~120-180 detik (~2-3 menit) untuk 1 hari
- Parallel processing: 8 intervals processed concurrently = ~2-3 menit total

**Overall Pipeline Impact:**
- Before: ~20-25 menit total
- After: **~6-10 menit total** (2-3x faster)

## Usage

### Default (Parallel Enabled)

No changes needed - parallel processing is enabled by default:
```python
from isolarcloud_harvester_src.isolar_data_processing import fetch_historical_data

# This will use parallel processing automatically
fetch_historical_data('2025-12-02', '2025-12-02')
```

### Disable Parallel Processing

If you need to disable parallel processing:
```python
# In isolar_config.py
PARALLEL_PROCESSING_ENABLED = False
```

### Custom Workers

Adjust number of workers:
```python
# In isolar_config.py
PARALLEL_MAX_WORKERS = 4  # Use 4 workers instead of 8
```

### Programmatic Control

You can also control parallel processing per call:
```python
from isolarcloud_harvester_src.isolar_data_processing import fetch_historical_data_for_batch

# Force sequential
fetch_historical_data_for_batch(devices, start, end, 5, parallel=False)

# Use custom worker count
fetch_historical_data_for_batch(devices, start, end, 5, max_workers=4)
```

## Monitoring

### Logs

Parallel processing logs will show:
```
Processing 8 intervals in parallel with 8 workers
Completed 3-hour interval: 2025-12-02 00:00:00 to 2025-12-02 02:59:59 - 1234 points
Completed 3-hour interval: 2025-12-02 03:00:00 to 2025-12-02 05:59:59 - 1234 points
...
```

### Performance Metrics

Monitor execution time in logs:
- Before: `fetch_historical_data completed successfully in 980.72 seconds`
- After: `fetch_historical_data completed successfully in 120-180 seconds`

## Testing

### Test Parallel Processing

1. Run ingestion for 1 day:
   ```python
   from isolarcloud.tasks import run_isolarcloud_ingest
   result = run_isolarcloud_ingest('2025-12-02', '2025-12-02')
   print(f"Execution time: {result['execution_time']:.2f} seconds")
   ```

2. Expected: ~120-180 seconds (vs ~980 seconds before)

### Test Sequential (Fallback)

1. Disable parallel in config:
   ```python
   PARALLEL_PROCESSING_ENABLED = False
   ```

2. Run same test - should take ~980 seconds

## Troubleshooting

### Issue: High Memory Usage

**Solution**: Reduce `PARALLEL_MAX_WORKERS` to 4 or 6

### Issue: API Rate Limits

**Solution**: 
- Reduce `PARALLEL_MAX_WORKERS` to 4
- Or increase `REQUEST_DELAY_SECONDS` in config

### Issue: Database Connection Errors

**Solution**: 
- Ensure database connection pool is large enough
- SQLAlchemy Session is thread-local, so should be safe
- If issues persist, reduce `PARALLEL_MAX_WORKERS`

## Notes

- Parallel processing is most effective for full-day ingestion
- For smaller time windows (< 3 hours), sequential may be faster due to overhead
- Thread-safe: SQLAlchemy handles thread-local sessions automatically
- API calls are still rate-limited via `REQUEST_DELAY_SECONDS`

