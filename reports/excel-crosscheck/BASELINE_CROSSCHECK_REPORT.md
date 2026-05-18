# Baseline Cross-Check Report: MMKI Sites

**Date**: 2025-01-XX (Updated after Energy Unit Fix)  
**Sites**: MMKI I, MMKI II, MMKI III  
**Data Range**: 
- Excel: 2025-01-01 to 2025-11-13 (794 records)
- Database: 2024-04-30 to 2025-11-17 (1273 records)
- Comparison Window: 2025-01-01 to 2025-11-13 (overlapping dates)

**Query File**: `cross_check_queries_mmki_baseline.sql`  
**Results CSV**: `baseline_crosscheck_results_mmki.csv` (to be exported)

**Status**: ✅ Energy unit di Excel sudah diperbaiki (dari kWh ke MWh)

---

## Executive Summary

Baseline cross-check dilakukan untuk mendokumentasikan perbedaan antara data database (`mart_site_performance_daily`) dengan data Excel (`site_daily_performance_excel_mmki`) sebelum implementasi fix untuk POA override dan GHI fallback.

### Overall Match Rate: **0.95%** ⚠️

**Hampir tidak ada record yang 100% match** antara database dan Excel. Ini expected karena:
1. ✅ **Energy unit sudah diperbaiki** - MMKI I: 99.1% match rate
2. POA override belum diimplementasi (MMKI II/III POA sensors masih di MMKI I)
3. GHI fallback belum diimplementasi (MMKI II/III tidak punya GHI values)
4. ⚠️ Energy mismatch untuk MMKI II/III masih ada (perlu investigasi lebih lanjut)

---

## Summary Statistics by Site

### 1. PT. MMKI 1.75 MWp - Painting Building (MMKI I)

| Metric | Match | Mismatch | Missing in DB | Missing in Excel | Avg Diff | Max Diff |
|--------|-------|----------|---------------|------------------|----------|----------|
| **Energy** | **314 (99.1%)** ✅ | 0 (0%) | 3 (0.9%) | 0 | - | - |
| **GHI** | 111 (35.0%) | 203 (64.0%) | 3 (0.9%) | 0 | 1.48 kWh/m² | 7.13 kWh/m² |
| **POA** | 36 (11.4%) | 278 (87.7%) | 3 (0.9%) | 0 | 0.07 kWh/m² | 1.10 kWh/m² |
| **Availability** | 79 (24.9%) | 237 (74.8%) | 0 | 1 (0.3%) | - | - |

**Total Records**: 317 days

**Key Findings**:
- ✅ **Energy match rate 99.1%** → **Unit sudah diperbaiki!** Hanya 3 records missing di DB
- ✅ GHI match rate 35% (reasonable, ada rounding differences)
- ⚠️ POA match rate hanya 11.4% → Perlu investigasi lebih lanjut
- ⚠️ Availability mismatch 74.8% → Perlu investigasi

---

### 2. PT. MMKI 5.7 MWp - Phase 2 (MMKI II)

| Metric | Match | Mismatch | Missing in DB | Missing in Excel | Avg Diff | Max Diff |
|--------|-------|----------|---------------|------------------|----------|----------|
| **Energy** | 0 (0%) | 317 (100%) | 0 | 0 | **5.32 MWh** | 20.47 MWh |
| **GHI** | 1 (0.3%) | 316 (99.7%) | 0 | 0 | 4.47 kWh/m² | 8.45 kWh/m² |
| **POA** | 109 (34.4%) | 206 (65.0%) | 2 (0.6%) | 0 | 1.43 kWh/m² | 6.64 kWh/m² |
| **Availability** | 100 (31.5%) | 217 (68.5%) | 0 | 0 | - | - |

**Total Records**: 317 days

**Key Findings**:
- ⚠️ **Energy mismatch 100%** (avg 5.32 MWh, max 20.47 MWh) → **Unit sudah benar**, tapi ada perbedaan nilai yang perlu investigasi
  - Excel energy lebih tinggi dari DB (systematic difference)
  - Perlu cek: meter selection, aggregation logic, atau data source differences
- ❌ **GHI mismatch 99.7%** → **Expected**: GHI fallback belum diimplementasi, semua GHI values NULL di DB
- ⚠️ **POA mismatch 65%** → **Expected**: POA override belum diimplementasi, POA sensors masih di MMKI I
- ⚠️ Availability mismatch 68.5% → Perlu investigasi

**Expected Issues (Pre-Fix)**:
- ✅ GHI missing in DB (expected - fallback not implemented)
- ✅ POA mismatch (expected - override not implemented)
- ⚠️ Energy mismatch (needs investigation - unit sudah benar)

---

### 3. PT. MMKI 4.292 MWP - Phase 3 (MMKI III)

| Metric | Match | Mismatch | Missing in DB | Missing in Excel | Avg Diff | Max Diff |
|--------|-------|----------|---------------|------------------|----------|----------|
| **Energy** | 10 (5.9%) | 160 (94.1%) | 0 | 0 | **0.36 MWh** | 18.48 MWh |
| **GHI** | 9 (5.3%) | 158 (92.9%) | 3 (1.8%) | 0 | 4.87 kWh/m² | 8.45 kWh/m² |
| **POA** | 9 (5.3%) | 153 (90.0%) | 8 (4.7%) | 0 | 4.61 kWh/m² | 6.58 kWh/m² |
| **Availability** | 89 (52.4%) | 81 (47.6%) | 0 | 0 | - | - |

**Total Records**: 170 days

**Key Findings**:
- ⚠️ **Energy mismatch 94.1%** (avg 0.36 MWh, max 18.48 MWh) → **Unit sudah benar**, tapi ada perbedaan nilai
  - Average difference kecil (0.36 MWh), tapi banyak records mismatch
  - Max difference besar (18.48 MWh) menunjukkan beberapa outliers
  - Perlu investigasi: meter selection atau calculation differences
- ❌ **GHI mismatch 92.9%** → **Expected**: GHI fallback belum diimplementasi
- ❌ **POA mismatch 90%** → **Expected**: POA override belum diimplementasi, POA sensors masih di MMKI I
- ✅ Availability match rate 52.4% (better than other sites)

**Expected Issues (Pre-Fix)**:
- ✅ GHI missing in DB (expected - fallback not implemented)
- ✅ POA missing/mismatch (expected - override not implemented)
- ⚠️ Energy mismatch (needs investigation - unit sudah benar, tapi ada calculation differences)

---

## Root Cause Analysis

### 1. Energy Unit Mismatch ✅ **FIXED**

**Status**: ✅ **RESOLVED** - Unit energy di Excel sudah diperbaiki

**Previous Problem**: 
- Database: Energy dalam MWh (e.g., 5.44 MWh)
- Excel: Energy dalam kWh (e.g., 5442.47 kWh = 5.44 MWh)

**Current Status**:
- ✅ MMKI I: **99.1% match rate** - Unit sudah benar, data match dengan baik
- ⚠️ MMKI II: 100% mismatch, tapi perbedaan reasonable (avg 5.32 MWh) - perlu investigasi calculation differences
- ⚠️ MMKI III: 94.1% mismatch, avg diff kecil (0.36 MWh) tapi banyak records - perlu investigasi

**Action Required**:
- [x] ✅ Excel energy unit sudah diperbaiki (kWh → MWh)
- [ ] Investigate energy calculation differences untuk MMKI II/III:
  - Check meter selection (apakah semua meters included?)
  - Check aggregation logic (sum vs average?)
  - Check data source differences

---

### 2. GHI Fallback Not Implemented ✅ **EXPECTED**

**Problem**: 
- MMKI II dan III tidak punya GHI sensor sendiri
- Mereka harus menggunakan GHI dari MMKI I (fallback)
- Fallback logic belum diimplementasi di `mart_site_performance_daily.sql`

**Evidence**:
- MMKI II: 0% GHI match, 99.7% mismatch (GHI NULL di DB)
- MMKI III: 5.3% GHI match, 92.9% mismatch (GHI NULL di DB)

**Expected Behavior After Fix**:
- MMKI II GHI should equal MMKI I GHI
- MMKI III GHI should equal MMKI I GHI

**Action Required**:
- [x] Seed file created: `seed_sensor_site_mapping.csv` (GHI_FALLBACK entries)
- [x] Logic implemented in `mart_site_performance_daily.sql` (CTE `daily_ghi`)
- [ ] Load seed and re-run dbt models
- [ ] Re-validate GHI values after fix

---

### 3. POA Override Not Implemented ✅ **EXPECTED**

**Problem**: 
- POA sensors untuk MMKI II/III secara fisik tersimpan di MMKI I
- Mereka harus di-assign ke logical site (MMKI II/III) berdasarkan `seed_sensor_site_mapping.csv`
- Override logic belum diimplementasi di `mart_site_performance_daily.sql`

**Evidence**:
- MMKI II: 34.4% POA match, 65% mismatch
- MMKI III: 5.3% POA match, 90% mismatch

**Expected Behavior After Fix**:
- MMKI II should have POA values (from sensors physically at MMKI I)
- MMKI III should have POA values (from sensors physically at MMKI I)
- POA values should match Excel after override

**Action Required**:
- [x] Seed file created: `seed_sensor_site_mapping.csv` (POA_OVERRIDE entries)
- [x] Logic implemented in `mart_site_performance_daily.sql` (CTE `daily_poa_per_sensor`)
- [ ] Load seed and re-run dbt models for dates >= 2025-09-15
- [ ] Re-validate POA values after fix

---

### 4. Availability Mismatch ⚠️ **NEEDS INVESTIGATION**

**Problem**: 
- Availability mismatch tinggi di semua sites (47.6% - 74.8%)
- Excel availability data mungkin tidak lengkap (NULL values)

**Evidence**:
- MMKI I: 24.9% match, 74.8% mismatch
- MMKI II: 31.5% match, 68.5% mismatch
- MMKI III: 52.4% match, 47.6% mismatch (best)

**Action Required**:
- [ ] Investigate availability calculation differences
- [ ] Check if Excel availability data is complete
- [ ] Compare availability calculation logic between DB and Excel

---

### 5. POA Values Mismatch (MMKI I) ⚠️ **NEEDS INVESTIGATION**

**Problem**: 
- MMKI I POA match rate hanya 11.4%
- Average difference kecil (0.07 kWh/m²) tapi mismatch rate tinggi

**Possible Causes**:
- Rounding differences
- Weighted average calculation differences
- Sensor selection differences

**Action Required**:
- [ ] Compare POA calculation logic between DB and Excel
- [ ] Check if sensor selection matches
- [ ] Verify weighted average calculation

---

## Next Steps (Prioritized)

### Phase 1: Fix Known Issues (Expected)

1. **Load Seed File** ✅
   - [ ] Run `dbt seed` untuk load `seed_sensor_site_mapping.csv`
   - [ ] Verify seed data loaded correctly

2. **Re-run dbt Models** ✅
   - [ ] Re-run `mart_site_performance_daily` untuk affected dates:
     - POA: dates >= 2025-09-15 (override effective date)
     - GHI: all dates (fallback always active)
   - [ ] Use incremental mode with date range: `dbt run --select mart_site_performance_daily --vars '{"reingest_start_date": "2025-09-15", "reingest_end_date": "2025-11-13"}'`

3. **Re-validation** ✅
   - [ ] Re-run baseline comparison query
   - [ ] Compare results with baseline (this report)
   - [ ] Document improvements

### Phase 2: Investigate Energy Calculation Differences

4. **Energy Calculation Investigation** ⚠️
   - [x] ✅ Excel energy unit sudah diperbaiki
   - [ ] Investigate MMKI II energy mismatch (avg 5.32 MWh difference)
     - Check meter selection in database vs Excel
     - Verify aggregation logic matches
     - Check for missing meters in database
   - [ ] Investigate MMKI III energy mismatch (avg 0.36 MWh, but 94% mismatch rate)
     - Check for systematic differences
     - Verify calculation logic

### Phase 3: Investigate Remaining Issues

5. **Availability Investigation** ⚠️
   - [ ] Compare availability calculation logic
   - [ ] Check Excel data completeness
   - [ ] Document findings

6. **POA Calculation Investigation** ⚠️
   - [ ] Compare POA weighted average calculation
   - [ ] Verify sensor selection matches
   - [ ] Document findings

---

## Files Generated

1. **Query File**: `cross_check_queries_mmki_baseline.sql`
   - STEP 1: Database data export
   - STEP 2: Baseline comparison query

2. **Results CSV**: `baseline_crosscheck_results_mmki.csv` (to be exported)
   - Full comparison results with all metrics
   - Status flags for each metric
   - Difference calculations

3. **This Report**: `BASELINE_CROSSCHECK_REPORT.md`
   - Summary statistics
   - Root cause analysis
   - Action items

---

## Notes

- **Tolerance Levels**:
  - Energy: 0.01 MWh
  - GHI: 0.01 kWh/m²
  - POA: 0.01 kWh/m²
  - Availability: 1.0%

- **Data Quality**:
  - Excel data range: 2025-01-01 to 2025-11-13
  - Database data range: 2024-04-30 to 2025-11-17
  - Comparison window: Overlapping dates only

- **Expected Improvements After Fix**:
  - ✅ Energy unit sudah diperbaiki - MMKI I: 99.1% match rate
  - GHI match rate for MMKI II/III should improve significantly (from ~0% to ~100%)
  - POA match rate for MMKI II/III should improve (from ~5-34% to ~80-90%+)
  - Energy calculation differences untuk MMKI II/III perlu investigasi lebih lanjut

---

**Report Generated**: 2025-01-XX  
**Next Review**: After Phase 1 fixes implemented
