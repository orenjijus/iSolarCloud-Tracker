# Final POA Analysis Report

**Date**: 2025-01-XX  
**Analysis**: POA Calculation Differences - Systematic vs Non-Systematic

---

## Executive Summary

Setelah analisis mendalam, ditemukan bahwa:

1. **3 dari 5 site** memiliki perhitungan POA yang **SISTEMATIS** (baik)
2. **2 dari 5 site** memiliki perhitungan POA yang **TIDAK SISTEMATIS** (perlu investigasi)

---

## ✅ Systematic Sites (Good - No Action Needed)

### 1. PLTS Frina Lestari Nusantara
- **Match Rate**: 12.10% (15/124 days)
- **Average Ratio**: 0.9993 (DB/Excel)
- **Average Difference**: -0.07%
- **Systematic Level**: ✅ **HIGHLY SYSTEMATIC**
- **Status**: ✅ **Perfect - No action needed**

### 2. Garuda Metalindo (IKP)
- **Match Rate**: 15.88% (64/403 days)
- **Average Ratio**: 0.9934 (DB/Excel)
- **Average Difference**: -0.66%
- **Systematic Level**: ✅ **MOSTLY SYSTEMATIC**
- **Status**: ✅ **Good - No action needed**

### 3. Charoen Pokphand Bandung
- **Match Rate**: 13.79% (16/116 days)
- **Average Ratio**: 1.0216 (DB/Excel)
- **Average Difference**: +2.16% (DB consistently higher)
- **Systematic Level**: ✅ **SYSTEMATIC**
- **Status**: ⚠️ **Acceptable - Should document difference**

**Note**: Match rate rendah karena threshold 0.01, tapi perbedaan sistematis dan kecil (< 2.2%)

---

## ⚠️ Non-Systematic Sites (Need Investigation)

### 4. Shoetown Ligung Indonesia (Excluding January)

**Statistics**:
- **Match Rate**: 1.04% (3/289 days)
- **Average Ratio**: 0.9726 (DB/Excel)
- **Average Difference**: -2.74%
- **Ratio Range**: 0.14 - 2.88 (very variable)
- **Systematic Level**: ⚠️ **NOT SYSTEMATIC**

**Monthly Pattern**:
- **Good months**: April, June, July (ratio 0.91-0.94, stddev < 0.02)
- **Variable months**: February, May, September, October, November

**Root Cause Hypothesis**:
- Sensor activation dates berbeda
- Excel mungkin menggunakan sensor berbeda di bulan berbeda

**Action Required**:
- [ ] Check sensor activation dates
- [ ] Verify if Excel uses different sensors in different months
- [ ] Compare sensor selection by month

---

### 5. PLTS Rooftop Sumatera Prima Fibreboard ❌

**Statistics**:
- **Match Rate**: 10.64% (5/47 days)
- **Average Ratio**: 1.3216 (DB/Excel)
- **Average Difference**: +32.16%
- **Ratio Range**: 0.97 - 7.25 (extremely variable)
- **Systematic Level**: ❌ **NOT SYSTEMATIC**

**Critical Outlier Days**:
- **Oct 12, 2025**: DB 5.58 vs Excel 0.77 (ratio: 7.25, diff: +625%)
- **Oct 13, 2025**: DB 2.96 vs Excel 0.52 (ratio: 5.69, diff: +469%)
- **Oct 5, 2025**: DB 2.94 vs Excel 0.00 (Excel missing)

**Monthly Pattern**:
- **October 2025**: Ratio 0.97-7.25 (very variable, stddev: 1.554)
- **November 2025**: Ratio 0.99-1.16 (much better, stddev: 0.051)

**Root Cause Analysis**:

1. **NULL Capacity Sensor** (1680199_5_11_2):
   - Has 43 days of data
   - Capacity: NULL (excluded from DB calculation)
   - **Finding**: On outlier days (Oct 12-13), this sensor has NO data, so it's not the cause

2. **Sensor Availability on Oct 12**:
   - Only 2 sensors have data: 1680199_5_13_1 (5.58) and 1680199_5_14_1 (5.59)
   - DB weighted average: 5.58 (correct calculation)
   - Excel value: 0.77 (very low - likely wrong!)

3. **November Improvement**:
   - November ratio much better (0.99-1.16)
   - Suggests Excel may have changed calculation method or sensor selection

**Hypothesis**:
- Excel mungkin menggunakan sensor yang berbeda atau calculation method berbeda
- Excel values sangat rendah pada beberapa hari (0.77, 0.52) - kemungkinan data quality issue
- November lebih baik, menunjukkan mungkin ada perubahan di Excel atau sensor activation

**Action Required**:
- [ ] **Priority 1**: Verify Excel calculation method for this site
- [ ] Check why Excel values are so low on Oct 12-13 (0.77, 0.52)
- [ ] Verify if Excel uses different sensors
- [ ] Check if NULL capacity sensor should have capacity value
- [ ] Document why November is better than October

---

## 📊 Summary Table

| Site | Match Rate | Avg Ratio | Avg Diff % | Systematic | Status |
|------|------------|-----------|------------|------------|--------|
| PLTS Frina Lestari | 12.10% | 0.999 | -0.07% | ✅ HIGHLY | ✅ Perfect |
| Garuda Metalindo | 15.88% | 0.993 | -0.66% | ✅ MOSTLY | ✅ Good |
| Charoen Pokphand | 13.79% | 1.022 | +2.16% | ✅ YES | ⚠️ Acceptable |
| Shoetown | 1.04% | 0.973 | -2.74% | ❌ NO | ⚠️ Investigate |
| PLTS Rooftop Sumatera | 10.64% | 1.322 | +32.16% | ❌ NO | ❌ Critical |

---

## 🎯 Recommendations

### Priority 1: PLTS Rooftop Sumatera Prima Fibreboard
- [ ] **URGENT**: Verify Excel calculation method
- [ ] Check why Excel values are so low on Oct 12-13 (0.77, 0.52)
- [ ] Verify if Excel uses different sensors
- [ ] Document why November is better than October

### Priority 2: Shoetown
- [ ] Check sensor activation dates
- [ ] Verify if Excel uses different sensors in different months
- [ ] Compare sensor selection by month

### Priority 3: Documentation
- [ ] Document Excel POA calculation method for all sites
- [ ] Document Charoen Pokphand 2.16% systematic difference
- [ ] Compare sensor selection between DB and Excel

---

## 📁 Files Created

### Analysis Reports
1. `POA_SYSTEMATIC_ANALYSIS.md` - Systematic analysis
2. `POA_OUTLIER_ANALYSIS.md` - Outlier days analysis
3. `POA_INVESTIGATION_SUMMARY.md` - Investigation summary
4. `FINAL_POA_ANALYSIS_REPORT.md` - This file

### SQL Queries
1. `poa_systematic_check.sql` - Systematic check queries
2. `investigate_poa_outliers.sql` - Outlier investigation queries

---

**Last Updated**: 2025-01-XX  
**Status**: Analysis completed, action items defined

