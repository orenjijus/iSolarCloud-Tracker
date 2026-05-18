# POA Investigation Summary - Final Findings

**Date**: 2025-01-XX  
**Status**: Root Causes Identified

---

## ✅ Summary of Findings

### 1. Systematic Sites (3 sites) - ✅ Good

| Site | Avg Ratio | Avg Diff % | Systematic Level | Status |
|------|-----------|------------|------------------|--------|
| **PLTS Frina Lestari** | 0.999 | -0.07% | HIGHLY SYSTEMATIC | ✅ Perfect |
| **Garuda Metalindo (IKP)** | 0.993 | -0.66% | MOSTLY SYSTEMATIC | ✅ Good |
| **Charoen Pokphand Bandung** | 1.022 | +2.16% | SYSTEMATIC | ⚠️ Acceptable |

**Conclusion**: Perhitungan sudah benar, perbedaan hanya rounding atau slight method differences.

---

### 2. Non-Systematic Sites (2 sites) - ⚠️ Need Investigation

#### A. PLTS Rooftop Sumatera Prima Fibreboard

**Statistics**:
- Average Ratio: 1.322 (DB/Excel)
- Average Difference: +32.16%
- Ratio Range: 0.97 - 7.25 (extremely variable)
- Systematic Level: ❌ NOT SYSTEMATIC

**Critical Outlier Days**:
- **Oct 12, 2025**: DB 5.58 vs Excel 0.77 (ratio: 7.25, diff: +625%)
- **Oct 13, 2025**: DB 2.96 vs Excel 0.52 (ratio: 5.69, diff: +469%)
- **Oct 5, 2025**: DB 2.94 vs Excel 0.00 (Excel missing)

**Root Cause Analysis**:

1. **NULL Capacity Sensor** (1680199_5_11_2):
   - Has 43 days of data
   - Capacity: NULL (excluded from DB)
   - **Finding**: On Oct 12, this sensor has NO data, so it's not the cause

2. **Sensor Availability on Oct 12**:
   - Only 2 sensors have data: 1680199_5_13_1 (5.58) and 1680199_5_14_1 (5.59)
   - DB weighted average: 5.58 (correct)
   - Excel value: 0.77 (very low!)

3. **Monthly Pattern**:
   - October 2025: Ratio 0.97-7.25 (very variable)
   - November 2025: Ratio 0.99-1.16 (much better!)

**Hypothesis**:
- Excel mungkin menggunakan sensor yang berbeda atau calculation method berbeda
- Excel values sangat rendah pada beberapa hari (0.77, 0.52) - kemungkinan data quality issue
- November lebih baik, menunjukkan mungkin ada perubahan di Excel atau sensor activation

**Action Required**:
- [ ] **Priority 1**: Verify Excel calculation method for this site
- [ ] Check if Excel uses different sensors
- [ ] Investigate why Excel values are so low on Oct 12-13 (0.77, 0.52)
- [ ] Check if NULL capacity sensor should have capacity value

---

#### B. Shoetown Ligung Indonesia (Excluding January)

**Statistics**:
- Average Ratio: 0.973 (DB/Excel)
- Average Difference: -2.74%
- Ratio Range: 0.14 - 2.88 (very variable)
- Systematic Level: ⚠️ NOT SYSTEMATIC

**Monthly Pattern**:

| Month | Avg Ratio | Stddev | Pattern |
|-------|-----------|--------|---------|
| **April** | 0.943 | 0.019 | ✅ Good |
| **June** | 0.918 | 0.020 | ✅ Good |
| **July** | 0.912 | 0.013 | ✅ Excellent |
| **February** | 1.196 | 0.614 | ⚠️ Variable |
| **May** | 1.034 | 0.496 | ⚠️ Variable |
| **September** | 0.919 | 0.230 | ⚠️ Variable |
| **October** | 0.937 | 0.203 | ⚠️ Variable |
| **November** | 1.098 | 0.216 | ⚠️ Variable |

**Root Cause Hypothesis**:
- Sensor activation dates berbeda
- Excel mungkin menggunakan sensor berbeda di bulan berbeda
- Beberapa bulan baik (April, June, July), beberapa variable

**Action Required**:
- [ ] Check sensor activation dates
- [ ] Verify if Excel uses different sensors in different months
- [ ] Compare sensor selection by month

---

## 🎯 Key Findings

### PLTS Rooftop Sumatera Prima

**Critical Issue**: Excel values sangat rendah pada beberapa hari
- Oct 12: Excel 0.77 vs DB 5.58 (ratio 7.25)
- Oct 13: Excel 0.52 vs DB 2.96 (ratio 5.69)

**Not caused by**:
- NULL capacity sensor (no data on those days)
- DB calculation (correct weighted average)

**Likely causes**:
- Excel menggunakan sensor berbeda
- Excel calculation method berbeda
- Data quality issue di Excel

### Shoetown

**Pattern**: Ratio variable per bulan
- Some months good (April, June, July)
- Some months variable (February, May, September, October, November)

**Likely causes**:
- Sensor activation dates berbeda
- Excel menggunakan sensor berbeda di bulan berbeda

---

## 📋 Action Items

### Priority 1: PLTS Rooftop Sumatera Prima
- [ ] **URGENT**: Verify Excel calculation method
- [ ] Check why Excel values are so low on Oct 12-13 (0.77, 0.52)
- [ ] Verify if Excel uses different sensors
- [ ] Check if NULL capacity sensor should have capacity value

### Priority 2: Shoetown
- [ ] Check sensor activation dates
- [ ] Verify if Excel uses different sensors in different months
- [ ] Compare sensor selection by month

### Priority 3: Documentation
- [ ] Document Excel POA calculation method for all sites
- [ ] Compare sensor selection between DB and Excel
- [ ] Document any differences found

---

**Last Updated**: 2025-01-XX  
**Status**: Root causes identified, action items defined

