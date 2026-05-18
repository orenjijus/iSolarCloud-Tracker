# Analisis: PR Issues akibat Zero/Small Values untuk GHI/POA/Energy

**Tanggal Analisis**: 2025-01-XX  
**Masalah**: PR GHI dan PR POA menjadi sangat besar ketika GHI/POA/Energy = 0 atau sangat kecil (division by zero atau division by very small number)

---

## Executive Summary

### Root Cause
**PR (Performance Ratio) dihitung sebagai**: `PR = Energy / (GHI/POA * Capacity Factor)` atau formula serupa.

**Masalah terjadi ketika**:
1. **GHI/POA = 0 atau sangat kecil** (< 0.1 kWh/m²): Division by zero atau division by very small number menyebabkan PR menjadi sangat besar atau tidak valid
2. **Energy = 0 atau sangat kecil** (< 0.01 MWh): PR menjadi 0 atau tidak valid
3. **Tidak ada handling** untuk kasus-kasus ini di calculation logic

### Impact
- **PR POA = 8379.63%** (Garuda Metalindo IKP, 2025-09-15) - **CRITICAL**
- **PR POA = 9.79%** (MMKI II, 2025-09-13)
- **PR GHI = 15.81%** (MMKI II, 2025-04-22)
- Banyak kasus PR differences yang sebenarnya bukan calculation error, tapi akibat zero/small values

---

## Critical Cases Found

### 1. Garuda Metalindo (IKP) - 2025-09-15 ⚠️ **CRITICAL**

**Values**:
- Excel Energy: 1.34 MWh
- DB Energy: 1.34 MWh ✅
- Excel GHI: 5.04 kWh/m² ✅
- DB GHI: 5.04 kWh/m² ✅
- **Excel POA: 0.0000 kWh/m²** ❌
- **DB POA: 0.0005 kWh/m²** ❌ (sangat kecil!)
- Excel PR GHI: 0.81% ✅
- DB PR GHI: 0.81% ✅
- **Excel PR POA: 0.0000%** ❌
- **DB PR POA: 8379.6289%** ❌ **CRITICAL!**

**Root Cause**: 
- POA sangat kecil (0.0005 kWh/m²) di DB
- PR POA calculation: `Energy / (POA * Capacity)` = `1.34 / (0.0005 * X)` = sangat besar
- Excel set PR POA = 0 karena POA = 0 (handling zero value)
- DB tidak handle zero/small value, jadi PR POA menjadi sangat besar

**Action Required**: 
- **IMMEDIATE**: Fix PR POA calculation di DB untuk handle POA < 0.1 kWh/m²
- Set PR POA = NULL atau 0 ketika POA < threshold (misalnya 0.1 kWh/m²)

---

### 2. PT. MMKI 5.7 MWp - Phase 2 - 2025-09-13

**Values**:
- Excel Energy: 27.21 MWh ✅
- DB Energy: 27.21 MWh ✅
- Excel GHI: 6.80 kWh/m² ✅
- DB GHI: 6.80 kWh/m² ✅
- **Excel POA: 0.0000 kWh/m²** ❌
- **DB POA: 0.4878 kWh/m²** ⚠️ (kecil)
- Excel PR GHI: 0.70% ⚠️
- DB PR GHI: 0.70% ⚠️
- **Excel PR POA: 0.0000%** ❌
- **DB PR POA: 9.7946%** ❌

**Root Cause**: 
- Excel POA = 0, Excel set PR POA = 0 (handling zero)
- DB POA = 0.4878 (masih kecil), DB calculate PR POA = 9.79%
- Perbedaan handling zero/small values

**Action Required**: 
- Standardize handling untuk POA < threshold (misalnya 0.5 kWh/m²)
- Set PR POA = NULL atau 0 ketika POA < threshold

---

### 3. Shoetown Ligung Indonesia - Multiple Dates (January 2025)

**Pattern**: Energy = 0 di Excel atau DB menyebabkan PR = 0 di satu sisi

**Example - 2025-01-05**:
- Excel Energy: 46.30 MWh
- **DB Energy: 0.0000 MWh** ❌
- Excel GHI: 0.0000 kWh/m² ❌
- DB GHI: 0.0000 kWh/m² ❌
- Excel POA: 1.56 kWh/m² ✅
- DB POA: 0.47 kWh/m² ⚠️
- Excel PR GHI: 0.00% (karena GHI = 0)
- DB PR GHI: NULL (karena GHI = 0)
- **Excel PR POA: 11.42%** ⚠️
- **DB PR POA: 0.0000%** ❌ (karena Energy = 0)

**Root Cause**: 
- Missing energy data di DB untuk January 2025
- Ketika Energy = 0, PR = 0 (valid)
- Tapi perbedaan ini bukan calculation error, tapi missing data

**Action Required**: 
- Fix missing energy data di DB
- Set PR = NULL (bukan 0) ketika Energy = 0 untuk membedakan dengan valid PR = 0

---

### 4. PT. MMKI 4.292 MWP - Phase 3 - Multiple Dates (June 2025)

**Pattern**: GHI = 0 di Excel menyebabkan PR GHI = 0, tapi DB calculate PR GHI

**Example - 2025-06-09**:
- Excel Energy: 13.40 MWh ✅
- DB Energy: 13.40 MWh ✅
- **Excel GHI: 0.0000 kWh/m²** ❌
- **DB GHI: 3.98 kWh/m²** ✅
- **Excel POA: 0.0000 kWh/m²** ❌
- DB POA: NULL
- **Excel PR GHI: 0.0000%** ❌
- **DB PR GHI: 0.7841%** ✅
- Excel PR POA: 0.0000% (karena POA = 0)
- DB PR POA: NULL

**Root Cause**: 
- Excel GHI = 0 (missing data atau sensor issue)
- Excel set PR GHI = 0 (handling zero)
- DB GHI valid, DB calculate PR GHI = 0.78%
- Perbedaan handling missing GHI data

**Action Required**: 
- Investigate why Excel GHI = 0 when DB GHI has value
- Standardize: Set PR = NULL (bukan 0) ketika GHI/POA = 0

---

## Summary Statistics

### Cases dengan Zero/Small Values Causing PR Issues

| Site | PR GHI Issues (Zero GHI) | PR POA Issues (Zero POA) | PR Issues (Zero Energy) | Total |
|------|-------------------------|-------------------------|------------------------|-------|
| **Shoetown Ligung** | ~28 | ~297 | ~32 | **~357** |
| **PT. MMKI 5.7 MWp - Phase 2** | ~268 | ~273 | ~52 | **~593** |
| **PT. MMKI 1.75 MWp** | ~169 | ~128 | ~1 | **~298** |
| **PT. MMKI 4.292 MWP - Phase 3** | ~161 | ~60 | ~4 | **~225** |
| **PT. Pusan Manis Mulia** | ~4 | ~212 | ~3 | **~219** |
| **Garuda Metalindo (IKP)** | ~8 | ~189 | ~9 | **~206** |
| **Garuda Metalindo 1** | ~3 | ~118 | ~2 | **~123** |
| **Charoen Pokphand Bandung** | ~21 | ~64 | ~21 | **~106** |

**Total**: ~2,127 cases (dari ~2,081 PR large differences) = **~102%** (beberapa cases memiliki multiple issues)

**Kesimpulan**: **Hampir semua PR large differences disebabkan oleh zero/small values!**

---

## Recommended Solutions

### 1. Fix PR Calculation Logic

**Current Issue**: Tidak ada handling untuk zero/small values

**Recommended Fix**:

```sql
-- PR GHI Calculation
CASE 
    WHEN daily_ghi_kwh_m2 IS NULL OR daily_ghi_kwh_m2 < 0.1 THEN NULL
    WHEN daily_energy_mwh IS NULL OR daily_energy_mwh < 0.01 THEN NULL
    ELSE (daily_energy_mwh / (daily_ghi_kwh_m2 * capacity_factor)) * 100
END as pr_ghi_actual

-- PR POA Calculation
CASE 
    WHEN daily_poa_weighted_kwh_m2 IS NULL OR daily_poa_weighted_kwh_m2 < 0.1 THEN NULL
    WHEN daily_energy_mwh IS NULL OR daily_energy_mwh < 0.01 THEN NULL
    ELSE (daily_energy_mwh / (daily_poa_weighted_kwh_m2 * capacity_factor)) * 100
END as pr_poa_actual
```

**Thresholds**:
- **GHI/POA < 0.1 kWh/m²**: Set PR = NULL (invalid calculation)
- **Energy < 0.01 MWh**: Set PR = NULL (invalid calculation)

### 2. Update Comparison Logic

**Current Issue**: Compare PR values even when one or both are invalid (NULL or 0 due to zero values)

**Recommended Fix**:

```sql
-- Only compare PR when both values are valid (not NULL and not due to zero values)
CASE
    WHEN excel_pr_ghi IS NULL OR db_pr_ghi IS NULL THEN NULL
    WHEN excel_ghi_kwh_m2 < 0.1 OR db_ghi_kwh_m2 < 0.1 THEN NULL
    WHEN excel_energy_mwh < 0.01 OR db_energy_mwh < 0.01 THEN NULL
    -- Then apply tolerance check
    WHEN ABS(excel_pr_ghi - db_pr_ghi) <= 0.01 THEN TRUE
    ELSE FALSE
END as pr_ghi_match
```

### 3. Data Quality Improvements

1. **Identify Missing Data**:
   - Flag dates dengan GHI/POA = 0 untuk investigation
   - Flag dates dengan Energy = 0 untuk investigation

2. **Data Validation**:
   - Add validation rules: GHI/POA should not be 0 unless it's a valid zero (e.g., night time)
   - Add validation rules: Energy should not be 0 unless it's a valid zero (e.g., maintenance)

3. **Documentation**:
   - Document when PR = NULL is expected (zero/small values)
   - Document when PR = 0 is valid vs invalid

---

## Priority Actions

### Immediate (Week 1)

1. **Fix PR POA Calculation** (Garuda Metalindo IKP - 2025-09-15)
   - Add zero/small value handling
   - Set PR POA = NULL when POA < 0.1 kWh/m²
   - **Impact**: Fix critical anomaly (PR POA = 8379.63%)

2. **Update Comparison Logic**
   - Skip PR comparison when GHI/POA/Energy = 0 or very small
   - **Impact**: Reduce false positives in large differences report

### High Priority (Week 2)

3. **Standardize PR Calculation**
   - Apply same zero/small value handling across all sites
   - **Impact**: Prevent future PR calculation errors

4. **Fix Missing Data**
   - Update missing energy data (Shoetown Ligung - January 2025)
   - Investigate missing GHI/POA data
   - **Impact**: Improve data quality

### Medium Priority (Week 3)

5. **Data Quality Monitoring**
   - Add alerts for zero/small values
   - Add validation rules
   - **Impact**: Prevent data quality issues

---

## Updated Comparison Report

Setelah fix ini diimplementasikan:
- **Expected Reduction**: ~2,000+ false positive PR large differences
- **Remaining Issues**: Actual calculation differences (bukan akibat zero values)
- **Data Quality**: Improved dengan proper handling zero/small values

---

**Report Generated**: 2025-01-XX  
**Next Steps**: Implement zero/small value handling in PR calculations

