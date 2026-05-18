# POA Outlier Analysis - Detailed Findings

**Date**: 2025-01-XX  
**Focus**: PLTS Rooftop Sumatera Prima & Shoetown (Non-Systematic Sites)

---

## 🔴 Critical Finding: PLTS Rooftop Sumatera Prima

### Outlier Days Identified

**Extreme Outliers** (Ratio > 5x):
1. **2025-10-12**: DB 5.58 vs Excel 0.77 (ratio: 7.25, diff: +625%)
2. **2025-10-13**: DB 2.96 vs Excel 0.52 (ratio: 5.69, diff: +469%)
3. **2025-10-05**: DB 2.94 vs Excel 0.00 (Excel missing!)

**Root Cause Identified** ✅:

#### Sensor with NULL Capacity
- **Device ID**: `1680199_5_11_2`
- **Capacity**: NULL (excluded from DB calculation)
- **Status**: Sensor has data but excluded because NULL capacity
- **Impact**: Excel mungkin menggunakan sensor ini, DB tidak

#### Sensor Availability Pattern
- **Oct 1-3**: Only 2 sensors (1680199_5_13_1, 1680199_5_14_1) - no NULL capacity sensor
- **Oct 4+**: 4 sensors including NULL capacity sensor (1680199_5_11_2)
- **Oct 12-13**: NULL capacity sensor has very low values (0.00-0.77) but Excel shows these values

**Hypothesis**:
- Excel menggunakan sensor dengan NULL capacity (1680199_5_11_2)
- DB exclude sensor ini karena NULL capacity
- Ketika sensor ini punya nilai rendah, Excel juga rendah, tapi DB tetap tinggi (karena exclude sensor ini)

### Monthly Pattern

| Month | Avg Ratio | Min Ratio | Max Ratio | Stddev | Pattern |
|-------|-----------|-----------|-----------|--------|---------|
| **October 2025** | 1.514 | 0.97 | **7.25** | 1.554 | Very variable |
| **November 2025** | 1.021 | 0.99 | 1.16 | 0.051 | **Much better!** |

**Finding**: November lebih baik (ratio 0.99-1.16) dibanding Oktober (ratio 0.97-7.25)

**Possible Cause**: 
- Sensor activation dates berbeda
- Excel mungkin menggunakan sensor berbeda di bulan berbeda
- NULL capacity sensor handling berbeda

---

## ⚠️ Shoetown - Monthly Pattern Analysis

### Monthly Ratio Pattern (Excluding January)

| Month | Avg Ratio | Min Ratio | Max Ratio | Stddev | Pattern |
|-------|-----------|-----------|-----------|--------|---------|
| **February** | 1.196 | 0.92 | 2.86 | 0.614 | Variable |
| **March** | 0.950 | 0.25 | 1.01 | 0.136 | Better |
| **April** | 0.943 | 0.91 | 0.98 | 0.019 | **Good!** |
| **May** | 1.034 | 0.64 | 2.88 | 0.496 | Variable |
| **June** | 0.918 | 0.89 | 0.96 | 0.020 | **Good!** |
| **July** | 0.912 | 0.90 | 0.94 | 0.013 | **Excellent!** |
| **August** | 0.920 | 0.55 | 0.96 | 0.071 | Mostly good |
| **September** | 0.919 | 0.44 | 1.36 | 0.230 | Variable |
| **October** | 0.937 | 0.14 | 1.25 | 0.203 | Variable |
| **November** | 1.098 | 0.74 | 1.61 | 0.216 | Variable |

**Pattern Identified**:
- **Good months**: April, June, July (ratio 0.91-0.94, stddev < 0.02)
- **Variable months**: February, May, September, October, November (ratio 0.64-2.88, stddev > 0.1)

**Root Cause Hypothesis**:
- Sensor activation dates berbeda
- Excel mungkin menggunakan sensor berbeda di bulan berbeda
- Beberapa sensor aktif di tanggal berbeda

---

## 🎯 Key Findings Summary

### PLTS Rooftop Sumatera Prima

**Critical Issue**: Sensor dengan NULL capacity (1680199_5_11_2)
- DB exclude sensor ini (NULL capacity)
- Excel mungkin menggunakan sensor ini
- Ketika sensor ini punya nilai rendah, Excel rendah, DB tinggi

**Outlier Days**:
- Oct 12: Excel 0.77 (sangat rendah) vs DB 5.58 → ratio 7.25
- Oct 13: Excel 0.52 (sangat rendah) vs DB 2.96 → ratio 5.69
- Oct 5: Excel 0.00 (missing) vs DB 2.94

**Action Required**:
- [ ] **Priority 1**: Check if Excel uses NULL capacity sensor (1680199_5_11_2)
- [ ] Add capacity value for this sensor if it should be included
- [ ] Or verify if Excel should exclude this sensor

### Shoetown

**Pattern**: Ratio variable per bulan
- Some months good (April, June, July)
- Some months variable (February, May, September, October, November)

**Root Cause Hypothesis**:
- Sensor activation dates berbeda
- Excel mungkin menggunakan sensor berbeda di bulan berbeda

**Action Required**:
- [ ] Check sensor activation dates
- [ ] Verify if Excel uses different sensors in different months
- [ ] Compare sensor selection by month

---

## 📋 Recommendations

### Priority 1: PLTS Rooftop Sumatera Prima
1. **Check NULL Capacity Sensor**:
   - [ ] Verify if sensor 1680199_5_11_2 should have capacity value
   - [ ] Check if Excel uses this sensor
   - [ ] Add capacity if needed, or verify Excel should exclude it

2. **Fix Outlier Days**:
   - [ ] Investigate Oct 12-13 (Excel values very low: 0.77, 0.52)
   - [ ] Check if these are data quality issues
   - [ ] Verify sensor data for those days

### Priority 2: Shoetown
1. **Monthly Pattern Analysis**:
   - [ ] Compare sensor selection by month
   - [ ] Check sensor activation dates
   - [ ] Verify if Excel uses different sensors in different months

---

**Last Updated**: 2025-01-XX  
**Status**: Outlier days identified, root causes hypothesized

