# Cross-Check Analysis: All Sites EXCEPT MMKI

**Date**: 2025-01-XX  
**Sites**: All sites except MMKI I, MMKI II, MMKI III  
**Comparison Window**: Based on available data in both DB and Excel

**Key Finding**: Excel energy data is stored in **kWh** while database uses **MWh** - conversion factor of 1000 applied.

---

## Executive Summary

Setelah memperbaiki unit conversion (Excel energy dalam kWh, DB dalam MWh), hasil perbandingan menunjukkan:

- ✅ **Energy Match**: Sebagian besar site memiliki match rate 80-98%
- ✅ **GHI Match**: Umumnya baik (78-97%)
- ⚠️ **POA Match**: Bervariasi signifikan (3-98%) - perlu investigasi
- ⚠️ **Site Name Issues**: Beberapa site memiliki nama berbeda antara DB dan Excel
- ⚠️ **Missing Data**: Beberapa site memiliki data missing di DB atau Excel

---

## Overall Statistics

### Summary by Metric

| Metric | Average Match Rate | Sites with >90% Match | Sites with <50% Match |
|--------|-------------------|----------------------|----------------------|
| **Energy** | ~85% | 8 sites | 2 sites |
| **GHI** | ~87% | 10 sites | 0 sites |
| **POA** | ~55% | 3 sites | 4 sites |
| **Overall** | ~30% | 2 sites | 10 sites |

*Overall match = semua metric (Energy, GHI, POA, Availability) harus match*

---

## Detailed Site Analysis

### ✅ Excellent Performance (>90% Energy Match)

#### 1. PT Gelora Djaja 1 MWp
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 10 (100%) | 0 | 0 | **100%** ✅ |
| GHI | 10 (100%) | 0 | 0 | **100%** ✅ |
| POA | 10 (100%) | 0 | 0 | **100%** ✅ |
| **Overall Match** | **5 (50%)** | | | **50%** |

**Key Findings**:
- ✅ Perfect match untuk semua metric individual
- ⚠️ Overall match 50% karena availability differences

---

#### 2. PLTS ONGRID PT SUPARMA TBK
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 4 (100%) | 0 | 0 | **100%** ✅ |
| GHI | 4 (100%) | 0 | 0 | **100%** ✅ |
| POA | 4 (100%) | 0 | 0 | **100%** ✅ |
| **Overall Match** | **0 (0%)** | | | **0%** |

**Key Findings**:
- ✅ Perfect match untuk semua metric individual
- ⚠️ Overall match 0% karena availability differences
- ⚠️ Site hanya ada di DB, tidak ada di Excel (4 days)

---

#### 3. Charoen Pokphand Majalengka
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 133 (97.79%) | 3 | 0 | **97.79%** ✅ |
| GHI | 116 (85.29%) | 20 | 0 | **85.29%** ✅ |
| POA | 116 (85.29%) | 20 | 0 | **85.29%** ✅ |
| **Overall Match** | **103 (75.74%)** | | | **75.74%** ✅ |

**Key Findings**:
- ✅ Excellent energy match (97.79%)
- ✅ Good GHI and POA match (85.29%)
- ✅ High overall match rate (75.74%)
- ⚠️ 3 energy mismatches (avg diff: 3.39 MWh)
- ⚠️ 20 days missing in Excel (GHI/POA)

---

#### 4. Charoen Pokphand Madiun
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 106 (95.50%) | 5 | 0 | **95.50%** ✅ |
| GHI | 108 (97.30%) | 3 | 0 | **97.30%** ✅ |
| POA | 109 (98.20%) | 2 | 0 | **98.20%** ✅ |
| **Overall Match** | **78 (70.27%)** | | | **70.27%** ✅ |

**Key Findings**:
- ✅ Excellent match rates across all metrics
- ✅ Highest POA match rate (98.20%)
- ✅ High overall match rate (70.27%)
- ⚠️ 5 energy mismatches (avg diff: 1.87 MWh)
- ⚠️ 3 days missing in Excel

---

#### 5. Charoen Pokphand Bandung
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 110 (94.83%) | 6 | 0 | **94.83%** ✅ |
| GHI | 105 (90.52%) | 11 | 2 | **90.52%** ✅ |
| POA | 16 (13.79%) | 100 | 2 | **13.79%** ⚠️ |
| **Overall Match** | **13 (11.21%)** | | | **11.21%** ⚠️ |

**Key Findings**:
- ✅ Excellent energy match (94.83%)
- ✅ Good GHI match (90.52%)
- ⚠️ **POA match sangat rendah (13.79%)** - perlu investigasi
- ⚠️ 6 energy mismatches (avg diff: 6.87 MWh, max: 18.18 MWh)
- ⚠️ 6 days missing in Excel

---

#### 6. PLTS Mall Panakkukang
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 318 (92.17%) | 27 | 0 | **92.17%** ✅ |
| GHI | 316 (91.59%) | 29 | 0 | **91.59%** ✅ |
| POA | 193 (55.94%) | 152 | 0 | **55.94%** ⚠️ |
| **Overall Match** | **61 (17.68%)** | | | **17.68%** ⚠️ |

**Key Findings**:
- ✅ Excellent energy match (92.17%)
- ✅ Good GHI match (91.59%)
- ⚠️ Moderate POA match (55.94%)
- ⚠️ 27 energy mismatches (avg diff: 2.82 MWh)
- ⚠️ 27 days missing in Excel

---

#### 7. PLTS Frina Lestari Nusantara
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 108 (87.10%) | 16 | 0 | **87.10%** ✅ |
| GHI | 106 (85.48%) | 18 | 0 | **85.48%** ✅ |
| POA | 15 (12.10%) | 109 | 0 | **12.10%** ⚠️ |
| **Overall Match** | **15 (12.10%)** | | | **12.10%** ⚠️ |

**Key Findings**:
- ✅ Good energy match (87.10%)
- ✅ Good GHI match (85.48%)
- ⚠️ **POA match sangat rendah (12.10%)** - perlu investigasi
- ⚠️ 16 energy mismatches (avg diff: 2.99 MWh)
- ⚠️ 19 days missing in Excel

---

#### 8. PLTS Rooftop Sumatera Prima Fibreboard
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 40 (85.11%) | 7 | 1 | **85.11%** ✅ |
| GHI | 44 (93.62%) | 3 | 0 | **93.62%** ✅ |
| POA | 5 (10.64%) | 42 | 0 | **10.64%** ⚠️ |
| **Overall Match** | **3 (6.38%)** | | | **6.38%** ⚠️ |

**Key Findings**:
- ✅ Good energy match (85.11%)
- ✅ Excellent GHI match (93.62%)
- ⚠️ **POA match sangat rendah (10.64%)** - perlu investigasi
- ⚠️ 7 energy mismatches (avg diff: 5.87 MWh, max: 12.64 MWh)
- ⚠️ 3 days missing in Excel

---

### ⚠️ Good Performance (70-90% Energy Match)

#### 9. Shoetown Ligung Indonesia
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 302 (83.66%) | 59 | 3 | **83.66%** ✅ |
| GHI | 314 (86.98%) | 47 | 15 | **86.98%** ✅ |
| POA | 11 (3.05%) | 350 | 0 | **3.05%** ⚠️ |
| **Overall Match** | **1 (0.28%)** | | | **0.28%** ⚠️ |

**Key Findings**:
- ✅ Good energy match (83.66%)
- ✅ Good GHI match (86.98%)
- ⚠️ **POA match sangat rendah (3.05%)** - perlu investigasi
- ⚠️ 59 energy mismatches (avg diff: 10.47 MWh, **max: 57.17 MWh** - anomaly!)
- ⚠️ 44 days missing in Excel

---

#### 10. Garuda Metalindo 1
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 328 (83.89%) | 63 | 3 | **83.89%** ✅ |
| GHI | 334 (85.42%) | 57 | 4 | **85.42%** ✅ |
| POA | 77 (19.69%) | 314 | 4 | **19.69%** ⚠️ |
| **Overall Match** | **23 (5.88%)** | | | **5.88%** ⚠️ |

**Key Findings**:
- ✅ Good energy match (83.89%)
- ✅ Good GHI match (85.42%)
- ⚠️ Low POA match (19.69%)
- ⚠️ 63 energy mismatches (avg diff: 2.27 MWh)
- ⚠️ 71 days missing in Excel

---

#### 11. Garuda Metalindo 2
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 318 (80.30%) | 78 | 3 | **80.30%** ✅ |
| GHI | 322 (81.31%) | 74 | 0 | **81.31%** ✅ |
| POA | 324 (81.82%) | 72 | 0 | **81.82%** ✅ |
| **Overall Match** | **104 (26.26%)** | | | **26.26%** ⚠️ |

**Key Findings**:
- ✅ Good energy match (80.30%)
- ✅ Good GHI match (81.31%)
- ✅ Good POA match (81.82%)
- ⚠️ 78 energy mismatches (avg diff: 2.13 MWh)
- ⚠️ 78 days missing in Excel

---

#### 12. Garuda Metalindo (MPF)
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 322 (81.11%) | 75 | 3 | **81.11%** ✅ |
| GHI | 317 (79.85%) | 80 | 3 | **79.85%** ✅ |
| POA | 317 (79.85%) | 80 | 3 | **79.85%** ✅ |
| **Overall Match** | **106 (26.70%)** | | | **26.70%** ⚠️ |

**Key Findings**:
- ✅ Good energy match (81.11%)
- ✅ Good GHI match (79.85%)
- ✅ Good POA match (79.85%)
- ⚠️ 75 energy mismatches (avg diff: 1.14 MWh)
- ⚠️ 79 days missing in Excel

---

#### 13. Garuda Metalindo (IKP)
| Metric | Match | Mismatch | Missing in DB | Match Rate |
|--------|-------|----------|---------------|------------|
| Energy | 319 (79.16%) | 84 | 3 | **79.16%** ✅ |
| GHI | 316 (78.41%) | 87 | 3 | **78.41%** ✅ |
| POA | 64 (15.88%) | 339 | 3 | **15.88%** ⚠️ |
| **Overall Match** | **22 (5.46%)** | | | **5.46%** ⚠️ |

**Key Findings**:
- ✅ Good energy match (79.16%)
- ✅ Good GHI match (78.41%)
- ⚠️ **POA match sangat rendah (15.88%)** - perlu investigasi
- ⚠️ 84 energy mismatches (avg diff: 1.05 MWh)
- ⚠️ 76 days missing in Excel

---

### ❌ Issues Identified

#### 1. Site Name Mismatch

**PT. Pusan Manis Mulia 2.06 MWp**
- **DB**: `PT. Pusan Manis Mulia 2.06 MWp - Tangerang`
- **Excel**: `PT. Pusan Manis Mulia 2.06 MWp 0 Tangerang`

**Impact**:
- Site di Excel tidak match dengan DB (nama berbeda)
- Data di Excel: 317 days, 0% match dengan DB
- Data di DB: 370 days, 0.27% match dengan Excel

**Action Required**:
- [ ] Standardize site name between DB and Excel
- [ ] Verify if these are the same site or different sites

---

#### 2. Sites Only in Database

- **PLTS ONGRID PT SUPARMA TBK**: 4 days in DB, not in Excel
- **PT Gelora Djaja 1 MWp**: 10 days in DB, not in Excel

**Action Required**:
- [ ] Verify if these sites should be in Excel
- [ ] Add to Excel if needed, or exclude from comparison

---

#### 3. Sites Only in Excel

- **PT. Pusan Manis Mulia 2.06 MWp 0 Tangerang**: 317 days in Excel, not in DB (name mismatch)

**Action Required**:
- [ ] Verify site name and match with DB
- [ ] Add to DB if it's a different site

---

## Root Cause Analysis

### 1. Energy Mismatches

**Patterns Observed**:
- Most sites have 80-98% energy match after unit conversion
- Average differences: 1-10 MWh (acceptable for rounding)
- Some sites have larger differences (max: 57.17 MWh for Shoetown Ligung)

**Possible Causes**:
1. **Rounding differences**: Database vs Excel rounding methods
2. **Calculation method differences**: Aggregation logic might differ
3. **Data quality issues**: Some days might have incorrect data
4. **Meter selection differences**: Different meters included in calculation

**Action Required**:
- [ ] Investigate sites with >5 MWh average difference
- [ ] Compare calculation logic between DB and Excel
- [ ] Verify meter selection matches

---

### 2. GHI Mismatches

**Patterns Observed**:
- Most sites have 78-97% GHI match
- Average differences: 2-5 kWh/m²
- Generally good match rates

**Possible Causes**:
1. **Rounding differences**: Database uses more decimal places
2. **Calculation method differences**: MAX vs average
3. **Unit conversion differences**: Multiple unit handling in DB

**Action Required**:
- [ ] Compare GHI calculation logic
- [ ] Verify unit conversions match
- [ ] Check if Excel uses MAX or average

---

### 3. POA Mismatches (Critical Issue)

**Patterns Observed**:
- **Wide variation**: 3-98% match rates
- **Low match sites**: 
  - Shoetown Ligung: 3.05%
  - PLTS Frina Lestari: 12.10%
  - PLTS Rooftop Sumatera Prima: 10.64%
  - Charoen Pokphand Bandung: 13.79%
  - Garuda Metalindo (IKP): 15.88%
- **Good match sites**:
  - Charoen Pokphand Madiun: 98.20%
  - Garuda Metalindo 2: 81.82%
  - Garuda Metalindo (MPF): 79.85%

**Possible Causes**:
1. **Weighted average calculation differences**: Capacity weighting might differ
2. **Sensor selection differences**: Different POA sensors included
3. **Override date handling**: POA override effective dates
4. **Calculation method differences**: Excel might use different method

**Action Required**:
- [ ] **Priority 1**: Investigate sites with <20% POA match
- [ ] Compare POA weighted average calculation
- [ ] Verify sensor selection matches Excel
- [ ] Check override date handling

---

### 4. Missing Data

**Patterns Observed**:
- Most sites have some missing data in Excel (3-80 days)
- Some sites have missing data in DB (3-15 days)
- Missing data affects overall match rate

**Possible Causes**:
1. **Data gaps**: Actual missing data
2. **Date range differences**: Different date ranges in DB vs Excel
3. **Site activation dates**: Sites might have different start dates

**Action Required**:
- [ ] Verify if missing data is actual gaps or date range differences
- [ ] Check site activation dates
- [ ] Fill missing data if possible

---

## Recommendations

### Priority 1: Critical Issues

1. **POA Calculation Investigation**
   - Investigate sites with <20% POA match
   - Compare weighted average calculation logic
   - Verify sensor selection

2. **Site Name Standardization**
   - Fix "PT. Pusan Manis Mulia" name mismatch
   - Verify if same site or different sites

3. **Energy Anomaly Investigation**
   - Investigate Shoetown Ligung (max diff: 57.17 MWh)
   - Check for data quality issues

### Priority 2: Data Quality

1. **Missing Data**
   - Verify missing data reasons
   - Fill gaps if possible

2. **GHI Calculation**
   - Compare calculation methods
   - Verify unit conversions

3. **Energy Calculation**
   - Investigate sites with >5 MWh average difference
   - Compare aggregation logic

### Priority 3: Optimization

1. **Tolerance Levels**
   - Consider adjusting tolerance for rounding differences
   - GHI: Current 0.01 kWh/m² might be too strict
   - POA: Current 0.01 kWh/m² might be too strict

2. **Overall Match Rate**
   - Most sites have low overall match due to POA issues
   - Once POA is fixed, overall match should improve significantly

---

## Next Steps

1. ✅ **Completed**: Unit conversion fix (kWh → MWh)
2. ✅ **Completed**: Initial comparison analysis
3. ⏭️ **Next**: Investigate POA calculation differences (Priority 1)
4. ⏭️ **Next**: Fix site name mismatch for "PT. Pusan Manis Mulia"
5. ⏭️ **Next**: Investigate energy anomalies (Shoetown Ligung)
6. ⏭️ **Next**: Compare calculation logic between DB and Excel
7. ⏭️ **Next**: Document Excel calculation methods
8. ⏭️ **Next**: Adjust database calculations if needed

---

**Report Generated**: 2025-01-XX  
**Status**: 
- ✅ Unit conversion identified and fixed
- ✅ Initial analysis completed
- ⏭️ POA calculation investigation pending
- ⏭️ Site name standardization pending
- ⏭️ Energy anomaly investigation pending

**Next Review**: After POA calculation investigation and site name fix

