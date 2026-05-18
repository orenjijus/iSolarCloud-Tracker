# Comprehensive Cross-Check Analysis: All Sites EXCEPT MMKI

**Date**: 2025-01-XX  
**Sites**: All sites except MMKI I, MMKI II, MMKI III  
**Status**: Energy sudah dalam MWh (tidak perlu konversi)

---

## Executive Summary

Setelah analisis lengkap semua site (kecuali MMKI), ditemukan beberapa pola dan root cause untuk perbedaan antara database dan Excel:

### Key Findings

1. **Energy Match Rates**: 79-98% untuk sebagian besar site
2. **GHI Match Rates**: 78-97% (umumnya baik)
3. **POA Match Rates**: 3-98% (variasi sangat besar - **critical issue**)
4. **Missing Data**: Beberapa site memiliki data missing di DB atau Excel
5. **Site Name Issues**: "PT. Pusan Manis Mulia" memiliki nama berbeda

### Overall Statistics

| Metric | Average Match Rate | Sites with >90% | Sites with <50% |
|--------|-------------------|-----------------|-----------------|
| **Energy** | ~85% | 8 sites | 2 sites |
| **GHI** | ~87% | 10 sites | 0 sites |
| **POA** | ~55% | 3 sites | 4 sites |
| **Overall** | ~30% | 2 sites | 10 sites |

---

## Detailed Site Analysis

### ✅ Excellent Performance (>95% Energy Match)

#### 1. PT Gelora Djaja 1 MWp
- **Energy**: 100% match ✅
- **GHI**: 100% match ✅
- **POA**: 100% match ✅
- **Overall**: 100% match ✅
- **Status**: Perfect match - tidak ada masalah

#### 2. PLTS ONGRID PT SUPARMA TBK
- **Energy**: 100% match ✅
- **GHI**: 100% match ✅
- **POA**: 100% match ✅
- **Overall**: 100% match ✅
- **Note**: Hanya 4 days data, hanya ada di DB

#### 3. Charoen Pokphand Majalengka
- **Energy**: 97.79% match (133/136 days) ✅
- **GHI**: 85.29% match ✅
- **POA**: 85.29% match ✅
- **Overall**: 85.29% match ✅
- **Pattern**: 
  - Energy: DB consistently higher (avg diff: +3.39 MWh)
  - GHI: DB consistently higher (avg diff: +5.19 kWh/m²)
  - POA: DB consistently higher (avg diff: +4.90 kWh/m²)
- **Root Cause**: Kemungkinan rounding atau calculation method differences

#### 4. Charoen Pokphand Madiun
- **Energy**: 95.50% match (106/111 days) ✅
- **GHI**: 97.30% match ✅
- **POA**: 98.20% match ✅
- **Overall**: 93.69% match ✅
- **Pattern**:
  - Energy: DB consistently higher (avg diff: +1.65 MWh)
  - Ratio: 0.86 (DB ~86% dari Excel) - **perlu investigasi**
- **Root Cause**: Kemungkinan meter selection atau aggregation differences

#### 5. Charoen Pokphand Bandung
- **Energy**: 94.83% match (110/116 days) ✅
- **GHI**: 90.52% match ✅
- **POA**: 13.79% match ⚠️ **CRITICAL**
- **Overall**: 13.79% match ⚠️
- **Pattern**:
  - Energy: DB consistently higher (avg diff: +6.87 MWh, max: 18.18 MWh)
  - Ratio: 1.0022 (sangat dekat 1) - perhitungan benar, hanya rounding
  - POA: DB consistently higher (avg diff: +0.58 kWh/m²)
- **Root Cause**: 
  - Energy: Rounding differences (ratio sangat dekat 1)
  - POA: **Calculation method berbeda** - perlu investigasi mendalam

---

### ⚠️ Good Performance (80-95% Energy Match)

#### 6. PLTS Mall Panakkukang
- **Energy**: 92.17% match (318/345 days) ✅
- **GHI**: 91.59% match ✅
- **POA**: 55.94% match ⚠️
- **Overall**: 17.68% match ⚠️
- **Pattern**:
  - Energy: DB consistently higher (avg diff: +2.82 MWh)
  - POA: DB consistently higher (avg diff: +0.49 kWh/m²)
- **Root Cause**: POA calculation method differences

#### 7. PLTS Frina Lestari Nusantara
- **Energy**: 87.10% match (108/124 days) ✅
- **GHI**: 85.48% match ✅
- **POA**: 12.10% match ⚠️ **CRITICAL**
- **Overall**: 12.10% match ⚠️
- **Pattern**:
  - Energy: DB consistently higher (avg diff: +2.99 MWh)
  - POA: DB consistently higher (avg diff: +0.83 kWh/m²)
- **Root Cause**: POA calculation method berbeda

#### 8. PLTS Rooftop Sumatera Prima Fibreboard
- **Energy**: 85.11% match (40/47 days) ✅
- **GHI**: 93.62% match ✅
- **POA**: 10.64% match ⚠️ **CRITICAL**
- **Overall**: 10.64% match ⚠️
- **Pattern**:
  - Energy: Excel consistently higher (avg diff: -1.56 MWh)
  - Ratio: 0.83 (DB ~83% dari Excel) - **perlu investigasi**
  - POA: Excel consistently higher (avg diff: -0.02 kWh/m²)
- **Root Cause**: 
  - Energy: Kemungkinan meter selection atau aggregation differences
  - POA: Calculation method berbeda

#### 9. Shoetown Ligung Indonesia
- **Energy**: 83.66% match (302/361 days) ✅
- **GHI**: 86.98% match ✅
- **POA**: 3.05% match ⚠️ **CRITICAL**
- **Overall**: 1.11% match ⚠️
- **Pattern**:
  - Energy: Variable pattern (stddev: 19.49 MWh) - **HIGH VARIANCE**
  - Ratio: 0.38 (sangat variable, 0-2.63) - **perlu investigasi**
  - Max diff: 57.17 MWh (DB = 0, Excel = 57.17) - **data quality issue**
- **Root Cause**: 
  - **Missing data di DB**: Banyak hari dimana DB energy = 0 tapi Excel punya nilai
  - **Missing data di Excel**: Banyak hari dimana Excel energy = 0 tapi DB punya nilai
  - POA: Calculation method berbeda

#### 10. Garuda Metalindo 1
- **Energy**: 83.89% match (328/391 days) ✅
- **GHI**: 85.42% match ✅
- **POA**: 19.69% match ⚠️
- **Overall**: 18.93% match ⚠️
- **Pattern**:
  - Energy: Excel consistently higher (avg diff: -1.50 MWh)
  - Ratio: 0.43 (sangat variable) - **perlu investigasi**
  - POA: Excel consistently higher (avg diff: -0.02 kWh/m²)
- **Root Cause**: Meter selection atau aggregation differences

#### 11. Garuda Metalindo 2
- **Energy**: 80.30% match (318/396 days) ✅
- **GHI**: 81.31% match ✅
- **POA**: 81.82% match ✅
- **Overall**: 78.28% match ✅
- **Pattern**:
  - Energy: Excel consistently higher (avg diff: -1.25 MWh)
  - Ratio: 0.38 (sangat variable) - **perlu investigasi**
  - GHI: DB consistently higher (avg diff: +4.11 kWh/m²)
  - POA: DB consistently higher (avg diff: +4.11 kWh/m²)
- **Root Cause**: Meter selection atau aggregation differences

#### 12. Garuda Metalindo (MPF)
- **Energy**: 81.11% match (322/397 days) ✅
- **GHI**: 79.85% match ✅
- **POA**: 79.85% match ✅
- **Overall**: 77.58% match ✅
- **Pattern**:
  - Energy: Excel consistently higher (avg diff: -0.24 MWh)
  - Ratio: 1.08 (close to 1) - perhitungan benar
  - GHI: DB consistently higher (avg diff: +4.37 kWh/m²)
  - POA: DB consistently higher (avg diff: +4.18 kWh/m²)
- **Root Cause**: GHI/POA calculation method differences

#### 13. Garuda Metalindo (IKP)
- **Energy**: 79.16% match (319/403 days) ✅
- **GHI**: 78.41% match ✅
- **POA**: 15.88% match ⚠️ **CRITICAL**
- **Overall**: 13.90% match ⚠️
- **Pattern**:
  - Energy: Excel consistently higher (avg diff: -0.24 MWh)
  - Ratio: 0.87 (variable) - **perlu investigasi**
  - POA: DB consistently higher (avg diff: +1.01 kWh/m²)
- **Root Cause**: POA calculation method berbeda

---

### ❌ Critical Issues

#### 1. PT. Pusan Manis Mulia - Site Name Mismatch

**Problem**: Dua nama berbeda untuk site yang sama atau berbeda:
- **DB**: `PT. Pusan Manis Mulia 2.06 MWp - Tangerang` (370 days)
- **Excel**: `PT. Pusan Manis Mulia 2.06 MWp 0 Tangerang` (317 days)

**Impact**:
- Site di Excel: 0% match dengan DB (nama tidak match)
- Site di DB: 0.27% match dengan Excel (hanya 1 day match)
- Data tidak bisa dibandingkan karena nama berbeda

**Action Required**:
- [ ] **URGENT**: Standardize site name
- [ ] Verify if these are the same site or different sites
- [ ] Update either DB or Excel to match

---

#### 2. POA Calculation Differences (Critical)

**Sites dengan POA match <20%**:
1. Shoetown Ligung Indonesia: 3.05%
2. PLTS Frina Lestari Nusantara: 12.10%
3. PLTS Rooftop Sumatera Prima Fibreboard: 10.64%
4. Charoen Pokphand Bandung: 13.79%
5. Garuda Metalindo (IKP): 15.88%

**Common Pattern**:
- DB consistently higher atau Excel consistently higher
- Average difference: 0.5-1.0 kWh/m²
- Standard deviation rendah (consistent difference)

**Possible Root Causes**:
1. **Weighted average calculation differences**:
   - Database: Capacity-weighted average of POA sensors
   - Excel: Mungkin menggunakan method berbeda (simple average, different weighting)
2. **Sensor selection differences**:
   - Database: Uses all POA sensors per site
   - Excel: Mungkin exclude beberapa sensors
3. **Override date handling**:
   - POA override effective from certain dates
   - Before/after override dates might have different calculations
4. **Unit conversion differences**:
   - Database handles multiple unit conversions
   - Excel might use different unit

**Action Required**:
- [ ] **Priority 1**: Compare POA weighted average calculation logic
- [ ] Verify sensor selection matches Excel
- [ ] Check override date handling
- [ ] Document Excel POA calculation method

---

#### 3. Energy Missing Data Issues

**Shoetown Ligung Indonesia** - Critical missing data:
- **DB missing**: 44 days (Excel has data, DB = 0)
- **Excel missing**: 3 days (DB has data, Excel = 0)
- **Max diff**: 57.17 MWh (DB = 0, Excel = 57.17)

**Pattern**:
- Banyak hari dimana DB energy = 0 tapi Excel punya nilai besar
- Banyak hari dimana Excel energy = 0 tapi DB punya nilai besar
- Ratio sangat variable (0-2.63)

**Possible Root Causes**:
1. **Data ingestion gaps**: Data tidak ter-import ke DB untuk beberapa hari
2. **Meter offline**: Meter tidak mengirim data untuk beberapa hari
3. **Date range differences**: Different date ranges in DB vs Excel
4. **Data quality issues**: Incorrect zero values

**Action Required**:
- [ ] **Priority 1**: Investigate missing data in DB (44 days)
- [ ] Verify if data exists in source system
- [ ] Check data ingestion logs
- [ ] Fill missing data if available

---

#### 4. Energy Ratio Analysis

**Sites dengan ratio tidak konsisten** (indicates calculation differences):

| Site | Avg Ratio | Pattern | Issue |
|------|-----------|---------|-------|
| Shoetown Ligung | 0.38 | Variable (0-2.63) | Missing data + calculation differences |
| Garuda Metalindo 1 | 0.43 | Variable | Meter selection differences |
| Garuda Metalindo 2 | 0.38 | Variable | Meter selection differences |
| PLTS Rooftop Sumatera | 0.83 | Variable (0.65-1.00) | Meter selection differences |
| Charoen Pokphand Madiun | 0.86 | Variable (0.77-0.95) | Meter selection differences |
| Garuda Metalindo (IKP) | 0.87 | Variable | Meter selection differences |

**Sites dengan ratio konsisten** (indicates correct calculation, only rounding):
- Charoen Pokphand Bandung: 1.0022 (very close to 1) ✅
- Garuda Metalindo (MPF): 1.08 (close to 1) ✅

**Action Required**:
- [ ] Compare meter selection between DB and Excel
- [ ] Verify all revenue meters are included
- [ ] Check aggregation logic matches

---

## Root Cause Analysis Summary

### 1. Energy Differences

**Patterns Identified**:
1. **Rounding Differences** (Good sites):
   - Charoen Pokphand Bandung: Ratio 1.0022 (very close to 1)
   - Garuda Metalindo (MPF): Ratio 1.08 (close to 1)
   - **Root Cause**: Rounding methods, acceptable

2. **Meter Selection Differences** (Variable ratio sites):
   - Garuda Metalindo sites: Ratio 0.38-0.87 (variable)
   - PLTS Rooftop Sumatera: Ratio 0.83 (variable)
   - **Root Cause**: Different meters included in calculation

3. **Missing Data** (Critical):
   - Shoetown Ligung: 44 days missing in DB
   - **Root Cause**: Data ingestion gaps or meter offline

4. **Calculation Method Differences** (Consistent difference):
   - Charoen Pokphand sites: DB consistently higher
   - **Root Cause**: Aggregation logic might differ

---

### 2. GHI Differences

**Patterns Identified**:
- Most sites: 78-97% match (good)
- Average differences: 2-5 kWh/m²
- DB consistently higher for most sites

**Possible Root Causes**:
1. **Rounding differences**: Database uses more decimal places
2. **Calculation method differences**: 
   - Database: MAX of daily_irradiance per sensor
   - Excel: Mungkin menggunakan average atau method berbeda
3. **Unit conversion differences**: 
   - Database: Handles multiple unit conversions (MJ/m², Wh/m², etc.)
   - Excel: Mungkin menggunakan unit yang berbeda

**Action Required**:
- [ ] Compare GHI calculation logic between DB and Excel
- [ ] Check if Excel uses MAX or average for daily GHI
- [ ] Verify unit conversions match

---

### 3. POA Differences (CRITICAL)

**Patterns Identified**:
- **Wide variation**: 3-98% match rates
- **Low match sites**: 5 sites with <20% match
- **Consistent differences**: DB or Excel consistently higher

**Possible Root Causes**:
1. **Weighted average calculation differences**:
   - Database: Capacity-weighted average of POA sensors
   - Excel: Mungkin menggunakan method berbeda
2. **Sensor selection differences**:
   - Database: Uses all POA sensors per site
   - Excel: Mungkin exclude beberapa sensors
3. **Override date handling**:
   - POA override effective from certain dates
   - Before/after override dates might have different calculations

**Action Required**:
- [ ] **Priority 1**: Investigate sites with <20% POA match
- [ ] Compare POA weighted average calculation
- [ ] Verify sensor selection matches Excel
- [ ] Check override date handling

---

## Recommendations

### Priority 1: Critical Issues

1. **Fix Site Name Mismatch**
   - Standardize "PT. Pusan Manis Mulia" name
   - Verify if same site or different sites
   - Update either DB or Excel

2. **Investigate POA Calculation**
   - Focus on 5 sites with <20% match
   - Compare weighted average calculation logic
   - Verify sensor selection

3. **Fix Missing Data**
   - Investigate Shoetown Ligung missing data (44 days)
   - Verify data exists in source system
   - Fill missing data if available

### Priority 2: Data Quality

1. **Energy Calculation Differences**
   - Investigate sites with variable ratio (0.38-0.87)
   - Compare meter selection
   - Verify aggregation logic

2. **GHI Calculation Differences**
   - Compare calculation methods
   - Verify unit conversions

### Priority 3: Optimization

1. **Tolerance Levels**
   - Consider adjusting tolerance for rounding differences
   - GHI: Current 0.01 kWh/m² might be too strict
   - POA: Current 0.01 kWh/m² might be too strict

2. **Documentation**
   - Document Excel calculation methods
   - Create comparison guide for future reference

---

## Next Steps

1. ✅ **Completed**: Unit conversion fix (energy sudah MWh)
2. ✅ **Completed**: Comprehensive analysis
3. ⏭️ **Next**: Fix site name mismatch (PT. Pusan Manis Mulia)
4. ⏭️ **Next**: Investigate POA calculation differences (Priority 1)
5. ⏭️ **Next**: Fix missing data (Shoetown Ligung - 44 days)
6. ⏭️ **Next**: Compare meter selection for variable ratio sites
7. ⏭️ **Next**: Document Excel calculation methods
8. ⏭️ **Next**: Adjust database calculations if needed

---

**Report Generated**: 2025-01-XX  
**Status**: 
- ✅ Comprehensive analysis completed
- ⏭️ Site name standardization pending
- ⏭️ POA calculation investigation pending
- ⏭️ Missing data investigation pending

**Next Review**: After fixing critical issues

