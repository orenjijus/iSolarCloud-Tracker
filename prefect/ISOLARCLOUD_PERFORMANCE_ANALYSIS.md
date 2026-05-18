# iSolarCloud Performance Analysis & Optimization

## Masalah: iSolarCloud Ingestion Butuh Waktu Lama

### Root Cause Analysis

**FusionSolar:**
- `MAX_DAYS_PER_REQUEST = 3` hari
- Untuk 1 hari data: **1 batch** (atau kurang dari 1 batch)
- Waktu: ~98 detik (~1.6 menit)

**iSolarCloud:**
- Memproses dalam **3-hour intervals** (hardcoded di `fetch_historical_data_for_batch`)
- Untuk 1 hari data (24 jam): **8 batches** (24 jam / 3 jam = 8)
- Waktu: ~980 detik (~16.3 menit)

**Perbandingan:**
- iSolarCloud butuh **8x lebih banyak API calls** untuk 1 hari data
- Setiap 3-hour interval = 1 API call
- Total overhead: 8x network calls, 8x processing overhead

## Solusi Optimasi

### Option 1: Parallel Processing (Recommended) ⭐

**Implementasi:**
- Jalankan 8 batch (3-hour intervals) secara **parallel** menggunakan threading/multiprocessing
- Mengurangi waktu dari ~16 menit menjadi ~2-3 menit (dengan 8 workers)

**Keuntungan:**
- ✅ Paling efektif - bisa reduce waktu hingga 8x
- ✅ Tidak perlu ubah API calls
- ✅ Tetap respect API limits

**Implementasi:**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def fetch_historical_data_for_batch_parallel(devices_batch, day_dt_start, day_dt_end, minute_interval, max_workers=8):
    """Process 3-hour intervals in parallel."""
    intervals = []
    current = day_dt_start
    while current < day_dt_end:
        interval_end = min(current + timedelta(hours=3), day_dt_end)
        intervals.append((current, interval_end))
        current += timedelta(hours=3)
    
    total_points = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_and_store_minute_data, devices_batch, start, end, minute_interval): (start, end)
            for start, end in intervals
        }
        
        for future in as_completed(futures):
            start, end = futures[future]
            try:
                points = future.result()
                total_points += points
            except Exception as e:
                logging.error(f"Error processing interval {start} to {end}: {e}")
    
    return total_points
```

### Option 2: Increase Batch Window (Jika API Support)

**Implementasi:**
- Cek apakah iSolarCloud API bisa handle lebih dari 3 jam per request
- Jika bisa, increase ke 6 jam atau 12 jam
- Reduce dari 8 batches menjadi 4 atau 2 batches

**Keuntungan:**
- ✅ Kurang API calls
- ✅ Kurang overhead

**Risiko:**
- ⚠️ Perlu test apakah API support
- ⚠️ Mungkin ada rate limit issues

### Option 3: Optimize Device Batching

**Implementasi:**
- Batch lebih banyak devices per API call
- Reduce jumlah total API calls dengan mengoptimalkan `MAX_PS_KEYS_PER_REQUEST` dan `MAX_POINTS_PER_REQUEST`

**Keuntungan:**
- ✅ Kurang API calls per interval
- ✅ Lebih efisien

**Catatan:**
- Sudah ada batching, tapi bisa dioptimalkan lebih lanjut

## Rekomendasi Implementasi

### Priority 1: Parallel Processing (Quick Win)

**File:** `isolarcloud/isolarcloud_harvester_src/isolar_data_processing.py`

**Changes:**
1. Modify `fetch_historical_data_for_batch` untuk support parallel processing
2. Add parameter `parallel=True, max_workers=8`
3. Use ThreadPoolExecutor untuk parallelize 3-hour intervals

**Estimated Impact:**
- Waktu: ~16 menit → **~2-3 menit** (5-8x faster)
- Risk: Low (tetap respect API limits, hanya parallelize)

### Priority 2: Configuration Tuning

**File:** `isolarcloud/isolarcloud_harvester_src/isolar_config.py`

**Changes:**
1. Test apakah bisa increase `MAX_PS_KEYS_PER_REQUEST` dari 50 ke 100
2. Test apakah bisa increase `MAX_POINTS_PER_REQUEST` dari 50 ke 100
3. Add config untuk `HOURS_PER_INTERVAL` (default 3, bisa test 6 atau 12)

**Estimated Impact:**
- Waktu: ~16 menit → **~12-14 menit** (moderate improvement)
- Risk: Medium (perlu test API limits)

## Implementation Plan

### Phase 1: Quick Win - Parallel Processing
1. ✅ Add parallel processing to `fetch_historical_data_for_batch`
2. ✅ Test dengan 1 hari data
3. ✅ Monitor API rate limits
4. ✅ Deploy jika successful

### Phase 2: Configuration Optimization
1. ✅ Test increased batch sizes
2. ✅ Test increased interval windows (jika API support)
3. ✅ Fine-tune based on results

## Expected Results

**Before:**
- iSolarCloud: ~16 menit untuk 1 hari
- Total pipeline: ~20-25 menit

**After (Parallel Processing):**
- iSolarCloud: ~2-3 menit untuk 1 hari
- Total pipeline: **~6-10 menit** (2-3x faster overall)

## Notes

- Parallel processing adalah solusi terbaik karena:
  - Tidak perlu ubah API calls
  - Tetap respect API limits
  - Bisa reduce waktu secara signifikan
  - Risk rendah

- Perlu monitor:
  - API rate limits (jangan exceed)
  - Database connection pool (pastikan cukup)
  - Memory usage (multiple threads)

