# Investigation Findings - Detailed Analysis

**Date**: 2025-01-XX  
**Status**: Critical Findings Identified

---

## 🔍 Critical Finding 1: Shoetown Revenue Meters Stuck (Not Incrementing)

### Problem
- **DB shows**: Energy = 0 for 11 days in January 2025
- **Source system**: Meter data **EXISTS** for those dates!
- **Root Cause**: **Revenue meters are stuck** - values not incrementing between days

### Root Cause Identified ✅
**Revenue Meters** (SLI-RM-01-A, SLI-RM-01-F):
- Detected as cumulative: ✅ Correct
- Values constant within day: ✅ Correct
- **BUT**: Values same between days (not incrementing!)
- Result: max - prev_max = 0 → calculated energy = 0

**Non-Revenue Meters** (SLI-RM-02-A, SLI-RM-02-F):
- **ARE incrementing** (86.38 → 88.92, 92.79 → 95.80)
- Calculated energy: 2.54 + 3.01 = 5.55 kWh

**Hypothesis**: Excel might be using non-revenue meters or different calculation method

### Evidence
**January 1-10, 2025 - Meter Data Available**:
- Multiple meters have data (288 data points per day per meter)
- Meters: ISO_1479456_7_11_2, 7_12_2, 7_13_2, 7_20_1, 7_3_1, 7_4_1
- Some meters show constant values (min = max) - **cumulative meters**
- Example: ISO_1479456_7_11_2 has positive_active_energy = 60810052000.0 (constant)

### Issue Identified
**Cumulative Meter Detection Problem**:
- Meters with constant values (min = max) are likely cumulative meters
- Database calculation might not be detecting them correctly
- Or meter selection might be wrong (including non-revenue meters)

### Action Required
- [ ] **URGENT**: Check why cumulative meter detection fails for Shoetown
- [ ] Verify meter selection (which meters are revenue meters?)
- [ ] Check if constant values are being filtered out incorrectly
- [ ] Review energy calculation logic for cumulative meters

---

## 🔍 Critical Finding 2: Madiun Meter Data Missing in Source

### Problem
- **DB shows**: Energy = 0 for July 29 - August 20, 2025 (23 days)
- **Source system**: **NO meter data** for those dates
- **Root Cause**: Meter was offline or data not collected

### Evidence
**Query Result**: Empty (no meter data found in source for missing dates)

### Confirmation
- User confirmed: Meter Madiun is the real problem in the field
- This validates the finding - meter was actually offline

### Action Required
- [ ] Document meter offline period for field team
- [ ] Verify if Excel data for those dates is correct (manual entry?)
- [ ] Check if data can be recovered from backup

---

## 🔍 Critical Finding 3: POA Calculation - Sensor Selection Issue

### Problem
**Shoetown Example (2025-10-01)**:
- **DB calculated**: 5.3709 kWh/m²
- **Excel**: 4.67 kWh/m²
- **Difference**: 0.70 kWh/m² (DB 15% higher)

### POA Sensors in Database (Shoetown):
1. `1479456_5_15_2`: Capacity 4524 kWp, POA 4.18 kWh/m²
2. `1479456_5_16_2`: Capacity 4524 kWp, POA 4.18 kWh/m²
3. `1479456_5_17_1`: Capacity 928 kWp, POA 4.43 kWh/m²
4. `1479456_5_18_1`: Capacity 76328 kWp, POA 5.52 kWh/m² (largest, dominates calculation)

**Weighted Average Calculation**:
- Total weighted: 463,532.72
- Total capacity: 86,304 kWp
- **Calculated**: 5.3709 kWh/m² ✅ (matches DB)

### Hypothesis
**Excel might be**:
1. Using different sensors (exclude some sensors)
2. Using simple average instead of weighted average
3. Using different capacity values
4. Using different date range (some sensors have different first_date)

### POA Sensor Coverage by Site

#### Shoetown Ligung Indonesia
- 4 sensors (2 with capacity 4524, 1 with 928, 1 with 76328)
- Coverage: 278-312 days (different sensors have different coverage)
- **Issue**: Some sensors start later (1479456_5_24_1, 1479456_5_25_1 start Oct 3)

#### PLTS Rooftop Sumatera Prima Fibreboard
- 4 sensors
- **Issue**: One sensor has NULL capacity (1680199_5_11_2)
- Coverage: 43-47 days (different sensors have different coverage)

#### Charoen Pokphand Bandung
- 2 sensors (8556 and 60636 kWp)
- Coverage: 114 days (both sensors)

#### PLTS Frina Lestari Nusantara
- 4 sensors
- Coverage: 124 days (all sensors)

#### Garuda Metalindo (IKP)
- 2 sensors (16815 and 1593 kWp)
- Coverage: 317 days (both sensors)

### Action Required
- [ ] **Priority 1**: Document which sensors Excel uses for each site
- [ ] Check if Excel excludes sensors with NULL capacity
- [ ] Verify if Excel uses different date ranges (sensor activation dates)
- [ ] Compare weighted average vs simple average

---

## 📊 Date Range Coverage Analysis

### Sites with Missing Energy Days

| Site | Total Days | Days with Energy | Missing Energy Days | % Coverage |
|------|------------|------------------|---------------------|------------|
| **Madiun** | 111 | 86 | **25** | 77.5% |
| Shoetown | 361 | 334 | 27 | 92.5% |
| Bandung | 116 | 108 | 8 | 93.1% |
| Majalengka | 136 | 108 | 28 | 79.4% |

### Key Findings
- **Madiun**: 25 missing days (22.5%) - confirmed meter offline
- **Shoetown**: 27 missing days (7.5%) - but data exists in source! (calculation issue)
- **Majalengka**: 28 missing days (20.6%) - need to check

---

## 🎯 Root Cause Summary

### 1. Shoetown Energy = 0 (But Data Exists)
**Root Cause**: Cumulative meter detection or meter selection issue
- Meter data exists in source
- Some meters have constant values (cumulative)
- DB calculation not processing them correctly

### 2. Madiun Energy = 0 (No Data in Source)
**Root Cause**: Meter offline (confirmed by user)
- No meter data in source for missing dates
- Excel might have manual entry

### 3. POA Differences (All Sites)
**Root Cause**: Sensor selection or calculation method differences
- DB uses all POA sensors with capacity weighting
- Excel might exclude some sensors or use different method
- Some sensors have NULL capacity (excluded from DB calculation)

---

## 📋 Immediate Action Items

### Priority 1: Shoetown Calculation Issue
- [ ] **URGENT**: Investigate why cumulative meters not detected
- [ ] Check meter selection (revenue vs non-revenue)
- [ ] Verify energy calculation for constant-value meters
- [ ] Test calculation with manual query

### Priority 2: POA Sensor Selection
- [ ] Document Excel POA sensor selection for each site
- [ ] Check if Excel excludes NULL capacity sensors
- [ ] Verify sensor activation dates
- [ ] Compare calculation methods

### Priority 3: Missing Data Recovery
- [ ] Madiun: Verify Excel data source (manual entry?)
- [ ] Shoetown: Fix calculation to use existing meter data
- [ ] Majalengka: Check why 28 days missing

---

**Last Updated**: 2025-01-XX  
**Status**: Critical issues identified, investigation ongoing

