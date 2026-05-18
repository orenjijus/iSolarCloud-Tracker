# Fix: MMKI 3 Availability Missing di October-November

**Tanggal**: 2025-01-XX  
**Masalah**: Availability untuk MMKI 3 (PT. MMKI 4.292 MWP - Phase 3) tidak ada di bulan October dan November  
**Root Cause**: GHI fallback logic tidak diterapkan di `irradiance_for_mit` calculation

---

## Root Cause

**Masalah**: MMKI 3 tidak memiliki GHI sensor sendiri, jadi menggunakan GHI fallback dari MMKI I. Namun, logika GHI fallback **hanya diterapkan di `daily_ghi`** (untuk daily aggregation), tapi **TIDAK diterapkan di `irradiance_for_mit`** (untuk 5-minute MIT calculation).

**Dampak**:
1. `irradiance_for_mit` hanya mengambil data dari site yang memiliki sensor sendiri
2. MMKI 3 tidak memiliki GHI sensor, jadi tidak ada data di `irradiance_for_mit`
3. `mit_calculation` tidak menghasilkan data untuk MMKI 3
4. INNER JOIN antara `inverter_availability_5min` dan `mit_calculation` tidak menghasilkan match
5. **Hasil**: Tidak ada availability data untuk MMKI 3

---

## Solution

**Fix**: Tambahkan GHI fallback logic ke `irradiance_for_mit` calculation, sama seperti yang sudah ada di `daily_ghi`.

### Perubahan yang Dilakukan

**File**: `dbt/models/marts/mart_site_performance_daily.sql`

**Before** (Line 337-369):
```sql
irradiance_for_mit AS (
    SELECT 
        s.date_key,
        s.site_name,
        s.timestamp,
        COALESCE(...) as irradiance_w_m2
    FROM mart_sensor_measurements_5min s
    ...
    GROUP BY s.date_key, s.site_name, s.timestamp
)
```

**After** (Line 338-408):
```sql
-- Step 1: Get raw irradiance data per site
irradiance_for_mit_raw AS (
    SELECT 
        s.date_key,
        s.site_name,
        s.timestamp,
        COALESCE(...) as irradiance_w_m2
    FROM mart_sensor_measurements_5min s
    ...
    GROUP BY s.date_key, s.site_name, s.timestamp
),

-- Apply GHI fallback logic: sites without GHI sensors use GHI from source site
irradiance_for_mit AS (
    -- Sites with their own GHI/POA sensors
    SELECT 
        date_key,
        site_name,
        timestamp,
        irradiance_w_m2
    FROM irradiance_for_mit_raw
    
    UNION
    
    -- Sites that need GHI fallback (don't have own GHI, use source site GHI)
    SELECT 
        ghi_source.date_key,
        ssm.device_id as site_name,  -- Target site (e.g., MMKI II, MMKI III)
        ghi_source.timestamp,
        ghi_source.irradiance_w_m2
    FROM irradiance_for_mit_raw ghi_source
    INNER JOIN seed_sensor_site_mapping ssm
        ON ghi_source.site_name = ssm.logical_site_id  -- Source site (e.g., MMKI I)
        AND ssm.mapping_type = 'GHI_FALLBACK'
        ...
    WHERE NOT EXISTS (
        -- Only add fallback if target site doesn't have its own irradiance data
        SELECT 1 
        FROM irradiance_for_mit_raw ghi_own
        WHERE ghi_own.site_name = ssm.device_id
            AND ghi_own.date_key = ghi_source.date_key
            AND ghi_own.timestamp = ghi_source.timestamp
    )
)
```

**Logika Fallback**:
1. **Step 1**: Ambil raw irradiance data dari semua site yang memiliki sensor sendiri
2. **Step 2**: Apply GHI fallback:
   - Sites dengan sensor sendiri → gunakan data sendiri
   - Sites tanpa GHI sensor (MMKI II, MMKI III) → gunakan GHI dari MMKI I
   - Fallback hanya diterapkan jika target site tidak memiliki data sendiri untuk timestamp tersebut

---

## Expected Impact

### Before Fix
- MMKI 3: **Tidak ada availability data** di October-November
- `irradiance_for_mit` tidak menghasilkan data untuk MMKI 3
- `mit_calculation` tidak menghasilkan data untuk MMKI 3
- INNER JOIN tidak menghasilkan match → tidak ada availability

### After Fix
- MMKI 3: **Availability data tersedia** untuk semua tanggal
- `irradiance_for_mit` menghasilkan data untuk MMKI 3 (dari GHI MMKI I)
- `mit_calculation` menghasilkan data untuk MMKI 3
- INNER JOIN menghasilkan match → availability terhitung dengan benar

---

## Sites Affected

Sites yang menggunakan GHI fallback (dari `seed_sensor_site_mapping.csv`):
1. **PT. MMKI 5.7 MWp - Phase 2** (MMKI II) → GHI dari MMKI I
2. **PT. MMKI 4.292 MWP - Phase 3** (MMKI III) → GHI dari MMKI I

**Note**: Fix ini akan mempengaruhi kedua site tersebut, bukan hanya MMKI 3.

---

## Verification Steps

1. **Run diagnostic query** (`dbt/analyses/debug_mmki3_availability_oct_nov.sql`):
   - Check apakah ada data inverter untuk MMKI 3 di October-November
   - Check apakah ada data MIT (setelah fallback) untuk MMKI 3
   - Check apakah ada match antara inverter data dan MIT data

2. **Re-run model** untuk October-November:
   ```bash
   dbt run --select mart_site_performance_daily --vars '{"reingest_start_date": "2025-10-01", "reingest_end_date": "2025-11-30"}'
   ```

3. **Verify availability data**:
   ```sql
   SELECT 
       date_key,
       site_name,
       power_available_hours,
       unavailability_hours,
       availability_percent
   FROM "MMSR"."mart"."mart_site_performance_daily"
   WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
       AND date_key >= '2025-10-01'::date
       AND date_key <= '2025-11-30'::date
   ORDER BY date_key;
   ```

4. **Compare dengan Excel** (jika tersedia):
   - Availability hours seharusnya mendekati nilai Excel
   - Availability percent seharusnya reasonable (0-100%)

---

## Related Issues Fixed

1. ✅ **MMKI 3 Availability Missing**: Fixed dengan menambahkan GHI fallback ke `irradiance_for_mit`
2. ✅ **MMKI 2 Availability**: Juga akan terpengaruh positif (menggunakan GHI fallback yang sama)

---

## Notes

1. **Consistency**: Sekarang `irradiance_for_mit` menggunakan logika fallback yang sama dengan `daily_ghi`, sehingga konsisten.

2. **Performance**: UNION operation mungkin sedikit lebih lambat, tapi impact minimal karena hanya menambahkan data untuk 2 sites (MMKI II dan MMKI III).

3. **Data Quality**: Fallback hanya diterapkan jika target site tidak memiliki data sendiri, sehingga tidak akan override data yang valid.

---

**Status**: ✅ **FIXED**  
**Next Steps**: 
1. Re-run model untuk October-November
2. Verify availability data untuk MMKI 3
3. Check apakah MMKI 2 juga terpengaruh positif

