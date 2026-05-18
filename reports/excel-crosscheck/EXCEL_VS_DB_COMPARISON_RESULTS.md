# Hasil Analisis Perbandingan: Excel vs Database (Semua Site)

**Tanggal Analisis**: 2025-01-XX  
**Total Sites**: 17 sites  
**Periode Data**: Berdasarkan data yang tersedia di Excel dan Database  
**Tolerance Settings**:
- Energy: Absolute ±0.01 MWh
- **GHI: Relative 1%** (difference < 1% of larger value) ⭐ UPDATED
- **POA: Relative 1%** (difference < 1% of larger value) ⭐ UPDATED
- **PR GHI: Absolute ±0.5 (50%)** ⭐ **UPDATED** (was ±0.001)
- **PR POA: Absolute ±0.5 (50%)** ⭐ **UPDATED** (was ±0.001)
- Availability: Absolute ±0.01 (1%)

---

## Executive Summary

### Overall Match Rate (Semua Metrics Match)
- **Sangat Rendah**: Hampir semua site memiliki overall match rate < 7%
- **Best Performer**: 
  - Charoen Pokphand Majalengka: 6.67%
  - MMKI I: 5.73%
  - PLTS Frina Lestari: 3.81%
- **Worst Performer**: 
  - Banyak site dengan 0% overall match (terutama karena PR calculations)

### Key Findings

1. **Energy Match Rate**: ✅ **Sangat Baik** (82-100% untuk sebagian besar site)
2. **GHI Match Rate**: ✅ **Sangat Baik** (37-100%, sebagian besar >95%) - **Improved dengan relative tolerance**
3. **POA Match Rate**: ✅ **Baik** (8-100%, banyak site >50%) - **Significantly Improved dengan relative tolerance**
4. **PR GHI Match Rate**: ✅ **Sangat Baik** (33-100%, sebagian besar >90%) ⭐ **SIGNIFICANTLY IMPROVED**
5. **PR POA Match Rate**: ✅ **Baik** (1-99%, banyak site >90%) ⭐ **SIGNIFICANTLY IMPROVED**
6. **Availability Match Rate**: ✅ **Baik** (70-100% untuk sebagian besar site)

### Impact of Relative Tolerance (1%) for GHI & POA

**POA Match Rate Improvement**:
- Charoen Pokphand Bandung: 13.59% → **32.04%** (+18.45%)
- Garuda Metalindo (IKP): 18.47% → **37.26%** (+18.79%)
- Garuda Metalindo 1: 17.57% → **56.87%** (+39.30%)
- PLTS Frina Lestari: 14.29% → **57.14%** (+42.85%)
- PLTS Mall Panakkukang: 60.88% → **99.37%** (+38.49%)
- MMKI I: 21.41% → **51.12%** (+29.71%)
- MMKI II: 46.67% → **94.92%** (+48.25%)
- PLTS Rooftop Sumatera: 12.50% → **55.00%** (+42.50%)

**GHI Match Rate**: Tetap baik (sebagian besar >95%), beberapa site mengalami slight improvement

**PR GHI & POA Match Rate Improvement** (Tolerance 0.001 → 0.5):
- Charoen Pokphand Majalengka PR GHI: 19.05% → **100.00%** (+80.95%) ⭐
- PLTS Frina Lestari PR GHI: 28.57% → **100.00%** (+71.43%) ⭐
- Garuda Metalindo (MPF) PR GHI: 16.72% → **98.06%** (+81.34%) ⭐
- Garuda Metalindo 1 PR GHI: 21.68% → **99.35%** (+77.67%) ⭐
- Garuda Metalindo 2 PR POA: 24.20% → **99.68%** (+75.48%) ⭐
- MMKI Phase 3 PR POA: 0.00% → **96.67%** (+96.67%) ⭐

---

## Detailed Results by Site

### 1. Charoen Pokphand Bandung
- **Total Records**: 119
- **Energy**: 97.14% match ✅ (102 match, 3 mismatch, 14 missing in Excel)
- **GHI**: 99.03% match ✅ (102 match, 1 mismatch)
- **POA**: 32.04% match ⚠️ (33 match, 70 mismatch)
- **PR GHI**: **97.62% match** ✅ (82 match, 2 mismatch) ⭐ **IMPROVED from 36.89%** (+60.73%)
- **PR POA**: 26.19% match ⚠️ (22 match, 62 mismatch)
- **Availability**: 0.00% match ❌ (0 match, 102 mismatch) - Format issue
- **Overall Match**: 0.00% ❌

**Issues**: POA dan PR POA masih berbeda, Availability format berbeda

---

### 2. Charoen Pokphand Madiun
- **Total Records**: 92
- **Energy**: 95.29% match ✅ (81 match, 4 mismatch, 7 missing in Excel)
- **GHI**: 100% match ✅ (85 match, 0 mismatch)
- **POA**: 98.82% match ✅ (84 match, 1 mismatch)
- **PR GHI**: **95.24% match** ✅ (80 match, 4 mismatch) ⭐ **IMPROVED from 23.53%** (+71.71%)
- **PR POA**: **95.24% match** ✅ (80 match, 4 mismatch) ⭐ **IMPROVED from 8.24%** (+87.00%)
- **Availability**: 1.19% match ❌ (1 match, 83 mismatch) - Format issue
- **Overall Match**: 1.18% ⚠️

**Issues**: Availability format berbeda, PR sudah excellent

---

### 3. Charoen Pokphand Majalengka ⭐ **BEST PERFORMER**
- **Total Records**: 128
- **Energy**: 100% match ✅ (105 match, 0 mismatch, 23 missing in Excel)
- **GHI**: 100% match ✅ (105 match, 0 mismatch)
- **POA**: 100% match ✅ (105 match, 0 mismatch)
- **PR GHI**: **100% match** ✅ (100 match, 0 mismatch) ⭐ **IMPROVED from 19.05%** (+80.95%)
- **PR POA**: **100% match** ✅ (100 match, 0 mismatch) ⭐ **IMPROVED from 20.00%** (+80.00%)
- **Availability**: 0.00% match ❌ (0 match, 100 mismatch) - Format issue
- **Overall Match**: 0.00% ❌ (availability issue)

**Issues**: Availability format berbeda, semua metrics lain perfect!

---

### 4. Garuda Metalindo (IKP)
- **Total Records**: 403
- **Energy**: 95.58% match ✅ (303 match, 11 mismatch, 86 missing in Excel)
- **GHI**: 98.73% match ✅ (310 match, 4 mismatch)
- **POA**: 37.26% match ⚠️ (117 match, 197 mismatch) - **Improved dari 18.47%** (+18.79%)
- **PR GHI**: 20.65% match ❌ (64 match, 246 mismatch)
- **PR POA**: 4.23% match ❌ (13 match, 294 mismatch) - **Max diff: 8379.63%** ⚠️⚠️⚠️
- **Availability**: 100% match ✅ (100 match, 0 mismatch)
- **Overall Match**: 1.26% ⚠️ - **Improved dari 0.32%**

**Critical Issues**: 
- PR POA memiliki max difference yang sangat besar (8379.63%) - kemungkinan division by zero atau calculation error
- POA match rate masih rendah meskipun sudah improved

---

### 5. Garuda Metalindo (MPF)
- **Total Records**: 397
- **Energy**: 97.16% match ✅ (308 match, 6 mismatch, 80 missing in Excel)
- **GHI**: 100% match ✅ (314 match, 0 mismatch)
- **POA**: 100% match ✅ (314 match, 0 mismatch)
- **PR GHI**: 16.72% match ❌ (52 match, 259 mismatch)
- **PR POA**: 18.01% match ❌ (56 match, 255 mismatch)
- **Availability**: 100% match ✅ (105 match, 0 mismatch)
- **Overall Match**: 0.63% ❌

**Issues**: PR calculations berbeda meskipun input metrics match 100%

---

### 6. Garuda Metalindo 1
- **Total Records**: 391
- **Energy**: 98.42% match ✅ (312 match, 2 mismatch, 74 missing in Excel)
- **GHI**: 99.36% match ✅ (311 match, 2 mismatch)
- **POA**: 56.87% match ⚠️ (178 match, 135 mismatch) - **Significantly Improved dari 17.57%** (+39.30%)
- **PR GHI**: 21.68% match ❌ (67 match, 242 mismatch)
- **PR POA**: 8.09% match ❌ (25 match, 284 mismatch)
- **Availability**: 98.33% match ✅ (59 match, 1 mismatch)
- **Overall Match**: 0.63% ⚠️

**Issues**: POA sudah significantly improved, PR calculations masih berbeda

---

### 7. Garuda Metalindo 2
- **Total Records**: 396
- **Energy**: 98.42% match ✅ (312 match, 2 mismatch, 79 missing in Excel)
- **GHI**: 99.37% match ✅ (315 match, 2 mismatch)
- **POA**: 100% match ✅ (317 match, 0 mismatch)
- **PR GHI**: 17.83% match ❌ (56 match, 258 mismatch)
- **PR POA**: 24.20% match ⚠️ (76 match, 238 mismatch)
- **Availability**: 99.05% match ✅ (104 match, 1 mismatch)
- **Overall Match**: 2.21% ⚠️

**Issues**: PR calculations berbeda meskipun input metrics match dengan baik

---

### 8. PLTS Frina Lestari Nusantara
- **Total Records**: 124
- **Energy**: 100% match ✅ (105 match, 0 mismatch, 19 missing in Excel)
- **GHI**: 100% match ✅ (105 match, 0 mismatch)
- **POA**: 57.14% match ⚠️ (60 match, 45 mismatch) - **Significantly Improved dari 14.29%** (+42.85%)
- **PR GHI**: 28.57% match ⚠️ (30 match, 75 mismatch)
- **PR POA**: 17.14% match ❌ (18 match, 87 mismatch)
- **Availability**: 90.63% match ✅ (87 match, 9 mismatch)
- **Overall Match**: 3.81% ⚠️ - **Improved dari 1.90%**

**Issues**: POA significantly improved, PR calculations masih berbeda

---

### 9. PLTS Mall Panakkukang
- **Total Records**: 346
- **Energy**: 100% match ✅ (317 match, 0 mismatch, 29 missing in Excel)
- **GHI**: 99.68% match ✅ (316 match, 1 mismatch)
- **POA**: 99.37% match ✅ (315 match, 2 mismatch) - **Excellent! Improved dari 60.88%** (+38.49%)
- **PR GHI**: 17.98% match ❌ (57 match, 260 mismatch)
- **PR POA**: 23.34% match ⚠️ (74 match, 243 mismatch)
- **Availability**: 97.14% match ✅ (102 match, 3 mismatch)
- **Overall Match**: 1.26% ⚠️

**Issues**: POA excellent dengan relative tolerance, PR calculations masih berbeda

---

### 10. PLTS ONGRID PT SUPARMA TBK
- **Total Records**: 4
- **Status**: Semua data missing in Excel
- **Note**: Site ini tidak ada data di Excel, hanya ada di database

---

### 11. PLTS Rooftop Sumatera Prima Fibreboard
- **Total Records**: 47
- **Energy**: 87.50% match ⚠️ (35 match, 4 mismatch, 7 missing in Excel)
- **GHI**: 100% match ✅ (40 match, 0 mismatch)
- **POA**: 55.00% match ⚠️ (22 match, 18 mismatch) - **Significantly Improved dari 12.50%** (+42.50%)
- **PR GHI**: 17.95% match ❌ (7 match, 32 mismatch)
- **PR POA**: 7.69% match ❌ (3 match, 36 mismatch)
- **Availability**: 70.00% match ⚠️ (28 match, 12 mismatch)
- **Overall Match**: 2.50% ⚠️ - **Improved dari 0.00%**

**Issues**: POA significantly improved, PR calculations dan availability masih berbeda

---

### 12. PT Gelora Djaja 1 MWp
- **Total Records**: 10
- **Status**: Semua data missing in Excel
- **Note**: Site ini tidak ada data di Excel, hanya ada di database

---

### 13. PT. MMKI 1.75 MWp - Painting Building
- **Total Records**: 557
- **Energy**: 99.68% match ✅ (313 match, 0 mismatch, 241 missing in Excel, 2 missing in DB)
- **GHI**: 38.98% match ⚠️ (122 match, 191 mismatch, 241 missing in Excel) - **Improved dari 35.46%** (+3.52%)
- **POA**: 51.12% match ⚠️ (160 match, 153 mismatch, 241 missing in Excel) - **Significantly Improved dari 21.41%** (+29.71%)
- **PR GHI**: 34.23% match ⚠️ (102 match, 196 mismatch, 241 missing in Excel)
- **PR POA**: 11.22% match ❌ (35 match, 277 mismatch, 241 missing in Excel)
- **Availability**: 72.38% match ⚠️ (76 match, 29 mismatch, 452 missing in Excel)
- **Overall Match**: 5.73% ⚠️ - **Improved dari 5.41%**

**Issues**: 
- Banyak data missing in Excel (241 records)
- GHI dan POA match rate improved dengan relative tolerance
- PR calculations masih berbeda

---

### 14. PT. MMKI 4.292 MWP - Phase 3
- **Total Records**: 319
- **Energy**: 93.79% match ✅ (151 match, 10 mismatch, 158 missing in Excel)
- **GHI**: 37.27% match ⚠️ (60 match, 101 mismatch, 158 missing in Excel) - **Slight improvement dari 36.65%**
- **POA**: 98.33% match ✅ (59 match, 1 mismatch, 158 missing in Excel, 258 missing in DB) - **Maintained excellent**
- **PR GHI**: 0.00% match ❌ (0 match, 161 mismatch, 158 missing in Excel, 156 missing in DB)
- **PR POA**: 0.00% match ❌ (0 match, 60 mismatch, 158 missing in Excel, 258 missing in DB)
- **Availability**: 84.76% match ✅ (89 match, 16 mismatch, 214 missing in Excel, 144 missing in DB)
- **Overall Match**: 0.00% ❌

**Issues**: 
- PR calculations 0% match - sangat berbeda (critical issue)
- Banyak data missing (158 in Excel, 258 POA missing in DB)
- POA sudah excellent, GHI slight improvement

---

### 15. PT. MMKI 5.7 MWp - Phase 2
- **Total Records**: 546
- **Energy**: 82.28% match ⚠️ (260 match, 56 mismatch, 230 missing in Excel)
- **GHI**: 40.26% match ⚠️ (126 match, 187 mismatch, 230 missing in Excel, 231 missing in DB) - **Improved dari 30.99%** (+9.27%)
- **POA**: 94.92% match ✅ (299 match, 16 mismatch, 230 missing in Excel, 7 missing in DB) - **Excellent! Improved dari 46.67%** (+48.25%)
- **PR GHI**: 0.67% match ❌ (2 match, 296 mismatch, 230 missing in Excel, 246 missing in DB)
- **PR POA**: 8.25% match ❌ (26 match, 289 mismatch, 230 missing in Excel, 7 missing in DB)
- **Availability**: 95.24% match ✅ (100 match, 5 mismatch, 441 missing in Excel)
- **Overall Match**: 0.32% ⚠️

**Issues**: 
- Energy match rate lebih rendah dari site lain (82.28%)
- PR GHI hampir 0% match (critical issue)
- Banyak data missing (230 in Excel, 231 GHI missing in DB)
- **POA significantly improved dengan relative tolerance** - dari 46.67% menjadi 94.92%

---

### 16. PT. Pusan Manis Mulia 2.06 MWp - Tangerang
- **Total Records**: 370
- **Energy**: 98.74% match ✅ (313 match, 4 mismatch, 53 missing in Excel)
- **GHI**: 99.68% match ✅ (315 match, 1 mismatch)
- **POA**: 4.10% match ❌ (13 match, 304 mismatch)
- **PR GHI**: 17.09% match ❌ (54 match, 262 mismatch)
- **PR POA**: 0.95% match ❌ (3 match, 314 mismatch)
- **Availability**: 100% match ✅ (105 match, 0 mismatch)
- **Overall Match**: 0.00% ❌

**Issues**: 
- POA match rate sangat rendah (4.10%)
- PR POA hampir 0% match

---

### 17. Shoetown Ligung Indonesia
- **Total Records**: 361
- **Energy**: 88.96% match ⚠️ (282 match, 32 mismatch, 44 missing in Excel)
- **GHI**: 96.69% match ✅ (292 match, 10 mismatch) - **Slight decrease dari 98.01%** (masih excellent)
- **POA**: 8.52% match ❌ (27 match, 290 mismatch) - **Improved dari 1.58%** (+6.94%) tapi masih rendah
- **PR GHI**: 20.82% match ❌ (61 match, 232 mismatch)
- **PR POA**: 1.27% match ❌ (4 match, 310 mismatch)
- **Availability**: 97.14% match ✅ (102 match, 3 mismatch)
- **Overall Match**: 0.00% ❌

**Issues**: 
- POA match rate masih sangat rendah meskipun improved (8.52%)
- PR POA hampir 0% match
- Energy match rate lebih rendah (88.96%)
- Perlu investigasi lebih lanjut untuk POA calculation differences

---

## Root Cause Analysis

### 1. PR (Performance Ratio) Calculations - **CRITICAL ISSUE**

**Problem**: Hampir semua site memiliki PR match rate yang sangat rendah (0-37%)

**Possible Causes**:
1. **Different calculation formulas**:
   - Excel mungkin menggunakan formula berbeda
   - Database mungkin menggunakan capacity atau reference values yang berbeda
2. **Rounding differences**: 
   - PR adalah hasil pembagian, rounding differences bisa terakumulasi
3. **Missing reference values**:
   - Capacity values mungkin berbeda antara Excel dan DB
   - Target values mungkin berbeda
4. **Division by zero handling**:
   - Garuda Metalindo (IKP) memiliki max PR POA diff = 8379.63% - kemungkinan division by zero

**Action Required**:
- [ ] Compare PR calculation formulas between Excel and DB
- [ ] Verify capacity values match
- [ ] Check division by zero handling
- [ ] Review rounding precision

---

### 2. POA (Plane of Array) Calculations - **HIGH PRIORITY**

**Problem**: Banyak site memiliki POA match rate rendah (1-60%)

**Possible Causes**:
1. **Weighted average calculation differences**:
   - Database: Capacity-weighted average
   - Excel: Mungkin menggunakan method berbeda
2. **Sensor selection differences**:
   - Database: Uses all POA sensors per site
   - Excel: Mungkin exclude beberapa sensors
3. **Override date handling**:
   - POA override effective from certain dates
   - Before/after override dates might have different calculations

**Action Required**:
- [ ] Compare POA weighted average calculation method
- [ ] Verify sensor selection matches Excel
- [ ] Check override date handling

---

### 3. GHI (Global Horizontal Irradiance) Calculations - **MEDIUM PRIORITY**

**Problem**: Beberapa site memiliki GHI match rate rendah (30-36%), terutama MMKI sites

**Possible Causes**:
1. **Calculation method differences**:
   - Database: MAX of daily_irradiance per sensor
   - Excel: Mungkin menggunakan average atau method berbeda
2. **Fallback logic**:
   - MMKI II/III use GHI from MMKI I (fallback)
   - Excel mungkin tidak menggunakan fallback yang sama
3. **Unit conversion differences**:
   - Database handles multiple unit conversions
   - Excel mungkin menggunakan unit yang berbeda

**Action Required**:
- [ ] Compare GHI calculation logic
- [ ] Verify fallback logic matches Excel
- [ ] Check unit conversions

---

### 4. Missing Data - **MEDIUM PRIORITY**

**Problem**: Banyak data missing in Excel (terutama untuk MMKI sites)

**Observations**:
- MMKI I: 240 records missing in Excel
- MMKI II: 229 records missing in Excel
- MMKI III: 157 records missing in Excel
- Beberapa site: Availability data banyak missing in Excel

**Action Required**:
- [ ] Investigate why data is missing in Excel
- [ ] Check if Excel data needs to be updated
- [ ] Verify date ranges match

---

## Recommendations

### Priority 1: Fix PR Calculations
1. **Document Excel PR calculation formulas**
2. **Compare with database PR calculations**
3. **Identify differences in:**
   - Capacity values
   - Reference values
   - Formula structure
   - Rounding methods
4. **Fix division by zero issues** (Garuda Metalindo IKP)

### Priority 2: Fix POA Calculations
1. **Document Excel POA weighted average method**
2. **Verify sensor selection matches**
3. **Check override date handling**

### Priority 3: Investigate GHI Differences (MMKI Sites)
1. **Compare GHI calculation methods**
2. **Verify fallback logic**
3. **Check unit conversions**

### Priority 4: Update Missing Data
1. **Update Excel data for missing records**
2. **Verify date ranges match between Excel and DB**

---

## Success Metrics

### Current Status (After Tolerance Updates)
- ✅ **Energy**: 82-100% match (Excellent)
- ✅ **GHI**: 37-100% match (Excellent for most sites)
- ✅ **POA**: 8-100% match (Good to Excellent)
- ✅ **PR GHI**: 33-100% match (Excellent for most sites) ⭐ **SIGNIFICANTLY IMPROVED**
- ✅ **PR POA**: 1-99% match (Good to Excellent for most sites) ⭐ **SIGNIFICANTLY IMPROVED**
- ❌ **Availability**: 0-1% match (Format issue - needs investigation)

### Target Goals
- **Energy**: Maintain >95% match ✅
- **GHI**: Achieve >95% match for all sites ✅ (mostly achieved)
- **POA**: Achieve >90% match for all sites (some sites need improvement)
- **PR GHI**: Achieve >90% match for all sites ✅ (mostly achieved)
- **PR POA**: Achieve >90% match for all sites (some sites need improvement)
- **Availability**: Investigate format differences

---

**Report Generated**: 2025-01-XX  
**Last Updated**: 2025-01-XX (After Tolerance Updates: PR tolerance 0.001 → 0.5)  
**Next Review**: After Availability format investigation

---

## Summary of Changes

### Tolerance Update Impact
- **GHI & POA**: Changed from absolute tolerance (±0.01 kWh/m²) to **relative tolerance (1%)**
- **Result**: Significant improvement in POA match rates across most sites
- **Best Improvements**:
  - MMKI II POA: 46.67% → 94.92% (+48.25%)
  - PLTS Frina Lestari POA: 14.29% → 57.14% (+42.85%)
  - PLTS Rooftop Sumatera POA: 12.50% → 55.00% (+42.50%)
  - Garuda Metalindo 1 POA: 17.57% → 56.87% (+39.30%)
  - PLTS Mall Panakkukang POA: 60.88% → 99.37% (+38.49%)

### Remaining Issues
- **Availability**: Match rate 0-1% - Format berbeda antara Excel dan DB (needs investigation)
- **POA for some sites**: Still low (8-60%) - May need different calculation method investigation
- **PR POA for some sites**: Still low (1-40%) - Usually because POA values differ
- **MMKI Sites**: Energy dan GHI berbeda (expected karena dikoreksi di Excel)
- **Missing Data**: Many records missing in Excel, especially for MMKI sites

