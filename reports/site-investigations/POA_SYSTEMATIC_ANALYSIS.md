# POA Systematic Analysis - Detailed Findings

**Date**: 2025-01-XX  
**Analysis**: 5 Sites with Low POA Match (<20%)  
**Note**: Shoetown January excluded (anomaly period)

---

## Executive Summary

Setelah analisis detail, ditemukan bahwa perbedaan POA **SANGAT SISTEMATIS** untuk sebagian besar site, dengan pola yang jelas dan konsisten.

### Key Findings

| Site | Systematic Level | Avg Ratio | Avg Diff % | Pattern |
|------|-----------------|-----------|------------|---------|
| **PLTS Frina Lestari** | ✅ **HIGHLY SYSTEMATIC** | 0.999 | -0.07% | Almost perfect |
| **Garuda Metalindo (IKP)** | ✅ **MOSTLY SYSTEMATIC** | 0.993 | -0.66% | DB slightly lower |
| **Charoen Pokphand Bandung** | ✅ **SYSTEMATIC** | 1.022 | +2.16% | DB consistently higher |
| **Shoetown** | ⚠️ **NOT SYSTEMATIC** | 0.973 | -2.74% | Variable |
| **PLTS Rooftop Sumatera** | ❌ **NOT SYSTEMATIC** | 1.322 | +32.16% | Very variable |

---

## Detailed Analysis by Site

### ✅ 1. PLTS Frina Lestari Nusantara - HIGHLY SYSTEMATIC

**Statistics**:
- **Total Days**: 105 days
- **Mismatch Count**: 90 days (85.7%)
- **Average Ratio**: 0.9993 (DB/Excel)
- **Average Difference**: -0.07% (DB slightly lower)
- **Standard Deviation**: 0.0155 (very low - highly consistent)
- **Systematic Level**: ✅ **HIGHLY SYSTEMATIC**

**Pattern**:
- Ratio: 0.9675 - 1.1037 (very tight range)
- DB consistently ~0.07% lower than Excel
- Difference: -0.13 to +0.17 kWh/m² (very small)
- **Conclusion**: Perhitungan hampir perfect, hanya rounding differences

**Root Cause**: 
- Calculation method sama atau sangat mirip
- Perbedaan hanya karena rounding atau precision

**Action**: ✅ **No action needed** - perbedaan sangat kecil dan acceptable

---

### ✅ 2. Garuda Metalindo (IKP) - MOSTLY SYSTEMATIC

**Statistics**:
- **Total Days**: 308 days
- **Mismatch Count**: 256 days (83.1%)
- **Average Ratio**: 0.9934 (DB/Excel)
- **Average Difference**: -0.66% (DB slightly lower)
- **Standard Deviation**: 0.0569 (low - mostly consistent)
- **Systematic Level**: ✅ **MOSTLY SYSTEMATIC**

**Pattern**:
- Ratio: 0.9391 - 1.8908 (some outliers)
- DB consistently ~0.66% lower than Excel
- Difference: -0.30 to +3.28 kWh/m²
- **Conclusion**: Perhitungan sangat mirip, ada beberapa outlier

**Root Cause**:
- Calculation method sama atau sangat mirip
- Beberapa hari memiliki outlier (kemungkinan data quality issues)

**Action**: 
- [ ] Investigate outlier days (ratio > 1.5)
- [ ] Verify data quality for those days
- ✅ Overall: Acceptable (difference < 1%)

---

### ✅ 3. Charoen Pokphand Bandung - SYSTEMATIC

**Statistics**:
- **Total Days**: 103 days
- **Mismatch Count**: 89 days (86.4%)
- **Average Ratio**: 1.0216 (DB/Excel)
- **Average Difference**: +2.16% (DB consistently higher)
- **Standard Deviation**: 0.0266 (very low - highly consistent)
- **Systematic Level**: ✅ **SYSTEMATIC**

**Pattern**:
- Ratio: 0.9812 - 1.0842 (tight range)
- DB consistently ~2.16% higher than Excel
- Difference: -0.11 to +0.44 kWh/m²
- **Conclusion**: Perhitungan sistematis, DB selalu sedikit lebih tinggi

**Sample Days** (Top differences):
- 2025-08-02: DB 6.32 vs Excel 5.88 (ratio: 1.075, diff: +7.52%)
- 2025-08-01: DB 5.89 vs Excel 5.45 (ratio: 1.080, diff: +8.03%)
- 2025-08-03: DB 5.51 vs Excel 5.08 (ratio: 1.084, diff: +8.42%)

**Root Cause Hypothesis**:
1. **Sensor selection**: DB mungkin include sensor yang Excel exclude
2. **Capacity weighting**: Perbedaan capacity values
3. **Calculation method**: Slight difference in weighted average calculation

**Action Required**:
- [ ] Document which sensors Excel uses
- [ ] Compare sensor capacity values
- [ ] Verify if Excel excludes any sensors
- ⚠️ **Priority**: Medium (difference 2% is acceptable but should be investigated)

---

### ⚠️ 4. Shoetown Ligung Indonesia - NOT SYSTEMATIC

**Statistics** (Excluding January):
- **Total Days**: 286 days
- **Mismatch Count**: 283 days (98.9%)
- **Average Ratio**: 0.9726 (DB/Excel)
- **Average Difference**: -2.74% (DB lower)
- **Standard Deviation**: 0.2834 (high - not systematic)
- **Systematic Level**: ⚠️ **NOT SYSTEMATIC**

**Pattern**:
- Ratio: 0.1449 - 2.8772 (very wide range!)
- DB sometimes higher, sometimes lower
- Difference: -3.81 to +3.32 kWh/m²
- **Conclusion**: Perhitungan tidak konsisten, ada variasi besar

**Root Cause Hypothesis**:
1. **Sensor selection changes**: Excel mungkin menggunakan sensor berbeda di hari berbeda
2. **Sensor activation dates**: Beberapa sensor aktif di tanggal berbeda
3. **Data quality issues**: Beberapa hari memiliki data quality problems

**Action Required**:
- [ ] **Priority**: Investigate why ratio is so variable
- [ ] Check sensor activation dates
- [ ] Verify if Excel uses different sensors on different days
- [ ] Compare sensor-by-sensor values for sample days

---

### ❌ 5. PLTS Rooftop Sumatera Prima Fibreboard - NOT SYSTEMATIC (CRITICAL)

**Statistics**:
- **Total Days**: 39 days
- **Mismatch Count**: 35 days (89.7%)
- **Average Ratio**: 1.3216 (DB/Excel)
- **Average Difference**: +32.16% (DB much higher!)
- **Standard Deviation**: 1.2342 (very high - not systematic)
- **Systematic Level**: ❌ **NOT SYSTEMATIC**

**Pattern**:
- Ratio: 0.9712 - 7.2513 (extremely wide range!)
- DB sometimes 3% lower, sometimes 625% higher!
- Difference: -0.17 to +4.81 kWh/m²
- **Conclusion**: Perhitungan sangat tidak konsisten, ada masalah besar

**Root Cause Hypothesis**:
1. **Sensor selection**: Excel mungkin exclude beberapa sensors
2. **NULL capacity sensor**: One sensor has NULL capacity (excluded from DB)
3. **Calculation method**: Excel mungkin menggunakan method yang sangat berbeda
4. **Data quality**: Beberapa hari memiliki data quality issues

**Action Required**:
- [ ] **Priority 1**: Investigate this site immediately
- [ ] Check which sensors Excel uses
- [ ] Verify NULL capacity sensor handling
- [ ] Compare calculation methods
- [ ] Check data quality for outlier days

---

## Summary by Systematic Level

### ✅ Highly/Mostly Systematic (Good - Acceptable Differences)

1. **PLTS Frina Lestari**: -0.07% difference (almost perfect)
2. **Garuda Metalindo (IKP)**: -0.66% difference (acceptable)
3. **Charoen Pokphand Bandung**: +2.16% difference (acceptable, but investigate)

**Conclusion**: Perhitungan sudah benar, perbedaan hanya rounding atau slight method differences.

### ⚠️ Not Systematic (Need Investigation)

1. **Shoetown**: -2.74% average, but very variable (ratio 0.14-2.88)
2. **PLTS Rooftop Sumatera**: +32.16% average, extremely variable (ratio 0.97-7.25)

**Conclusion**: Ada masalah dengan sensor selection atau calculation method.

---

## Recommendations

### Priority 1: PLTS Rooftop Sumatera Prima Fibreboard
- [ ] **URGENT**: Investigate why ratio is so variable (0.97-7.25)
- [ ] Check sensor selection and NULL capacity handling
- [ ] Verify calculation method differences

### Priority 2: Shoetown
- [ ] Investigate variable ratio (0.14-2.88)
- [ ] Check sensor activation dates
- [ ] Verify if Excel uses different sensors on different days

### Priority 3: Charoen Pokphand Bandung
- [ ] Document Excel sensor selection
- [ ] Verify 2.16% systematic difference (acceptable but should be documented)

### Priority 4: PLTS Frina Lestari & Garuda Metalindo (IKP)
- [ ] ✅ **No action needed** - differences are acceptable (< 1%)

---

**Last Updated**: 2025-01-XX  
**Status**: Systematic analysis completed, action items defined

