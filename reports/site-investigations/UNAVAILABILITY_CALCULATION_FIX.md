# Fix: Unavailability Calculation Issue

**Tanggal**: 2025-01-XX  
**Masalah**: Unavailability hours di database jauh lebih besar dari Excel  
**Contoh**: Tanggal 10
- Excel: unavailability = 0.010416667 hours, power available = 11.42708333 hours
- DB: unavailability_hours = 6.23 hours (sangat jauh berbeda)

---

## Root Cause

**Masalah**: FULL OUTER JOIN antara `inverter_availability_5min` dan `mit_calculation` menyebabkan interval yang hanya punya MIT data (tanpa inverter data) dihitung sebagai **full unavailability**.

### Penjelasan Detail

**Sebelum Fix (FULL OUTER JOIN)**:
```sql
FROM inverter_availability_5min ia
FULL OUTER JOIN mit_calculation m 
    ON ia.date_key = m.date_key 
    AND ia.site_name = m.site_name 
    AND ia.timestamp = m.timestamp
```

**Masalah yang terjadi**:
1. Jika ada timestamp dengan **MIT=1** (irradiance > 40 W/m²) tapi **TIDAK ada inverter data**:
   - `power_availability_ratio = 0` (dari COALESCE)
   - `unavailability_ratio = 1 - 0 = 1` (karena MIT = 1)
   - **Hasil**: Interval ini dihitung sebagai **FULL unavailability** (1.0), padahal seharusnya tidak bisa ditentukan karena tidak ada data inverter

2. Jika ada timestamp dengan **inverter data** tapi **TIDAK ada MIT data**:
   - `mit = 0` (dari COALESCE)
   - `unavailability_ratio = 0` (karena MIT = 0)
   - **Hasil**: Interval ini tidak dihitung sebagai unavailability, padahal mungkin seharusnya dihitung jika irradiance > 40

**Dampak**:
- Banyak interval dengan MIT=1 tapi tidak ada inverter data dihitung sebagai full unavailability
- Unavailability hours menjadi **sangat besar** (misalnya 6.23 hours vs 0.01 hours di Excel)
- Perhitungan tidak sesuai dengan logika Excel yang hanya menghitung interval dengan **BOTH** data

---

## Solution

**Fix**: Ubah FULL OUTER JOIN menjadi **INNER JOIN** untuk hanya menghitung interval yang memiliki **BOTH** inverter data dan MIT data.

### Setelah Fix (INNER JOIN)

```sql
FROM inverter_availability_5min ia
INNER JOIN mit_calculation m 
    ON ia.date_key = m.date_key 
    AND ia.site_name = m.site_name 
    AND ia.timestamp = m.timestamp
```

**Logika Baru**:
- Hanya menghitung availability/unavailability ketika **BOTH** inverter data dan MIT data ada
- Jika MIT=1 tapi tidak ada inverter data → **EXCLUDE** dari perhitungan (tidak bisa ditentukan)
- Jika inverter data ada tapi tidak ada MIT data → **EXCLUDE** dari perhitungan (tidak bisa ditentukan)
- Jika **BOTH** ada → Hitung normal: `unavailability_ratio = 1 - power_availability_ratio` (jika MIT=1)

**Hasil**:
- Unavailability hours hanya dihitung untuk interval dengan data lengkap
- Perhitungan sesuai dengan logika Excel
- Tidak ada "false positive" unavailability dari interval tanpa data inverter

---

## Expected Impact

### Before Fix
- Unavailability hours: **6.23 hours** (untuk tanggal 10)
- Banyak interval dengan MIT=1 tapi tidak ada inverter data dihitung sebagai full unavailability

### After Fix
- Unavailability hours: **~0.01 hours** (mendekati Excel: 0.010416667)
- Hanya interval dengan data lengkap yang dihitung
- Perhitungan lebih akurat dan sesuai dengan Excel

---

## Verification Steps

1. **Run diagnostic query** (`dbt/analyses/debug_unavailability_date_10.sql`):
   - Check breakdown by data source (BOTH, MIT_ONLY, INVERTER_ONLY)
   - Verify bahwa setelah fix, hanya interval dengan BOTH yang dihitung

2. **Re-run model** untuk tanggal yang bermasalah:
   ```bash
   dbt run --select mart_site_performance_daily --vars '{"reingest_start_date": "2025-11-10", "reingest_end_date": "2025-11-10"}'
   ```

3. **Compare dengan Excel**:
   - Unavailability hours seharusnya mendekati nilai Excel
   - Power available hours seharusnya juga lebih akurat

---

## Code Changes

**File**: `dbt/models/marts/mart_site_performance_daily.sql`

**Change**: Line 430-434
- **Before**: `FULL OUTER JOIN` dengan COALESCE untuk handle missing data
- **After**: `INNER JOIN` untuk hanya menghitung interval dengan data lengkap

**Additional Comments**: Added explanation comments tentang mengapa INNER JOIN digunakan

---

## Notes

1. **Data Quality**: Jika setelah fix masih ada perbedaan, kemungkinan:
   - Missing inverter data untuk beberapa interval (data quality issue)
   - Missing MIT data untuk beberapa interval (sensor issue)
   - Excel mungkin menggunakan logic berbeda untuk handle missing data

2. **Total Hours**: Setelah fix, `total_hours` mungkin lebih kecil karena hanya menghitung interval dengan data lengkap. Ini adalah expected behavior.

3. **Availability Percentage**: Setelah fix, availability percentage akan lebih akurat karena hanya berdasarkan interval dengan data lengkap.

---

**Status**: ✅ **FIXED**  
**Next Steps**: 
1. Re-run model untuk tanggal yang bermasalah
2. Verify hasil dengan Excel
3. Check apakah ada site lain yang mengalami masalah serupa

