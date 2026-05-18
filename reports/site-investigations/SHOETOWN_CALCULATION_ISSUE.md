# Shoetown Calculation Issue - Root Cause Identified

**Date**: 2025-01-XX  
**Status**: Root Cause Found

---

## 🔴 Critical Issue: Cumulative Meters Not Incrementing

### Problem
**Shoetown January 2, 2025**:
- **DB shows**: Energy = 0 MWh
- **Excel shows**: Energy = 30.85 MWh
- **Meter data exists**: YES (288 data points per meter per day)

### Root Cause Identified

**Revenue Meters** (from seed_meter_config):
1. `SLI-RM-01-A` (esn_code: 1479456_7_12_2) - Revenue meter
2. `SLI-RM-01-F` (esn_code: 1479456_7_3_1) - Revenue meter

**Meter Values (January 1-2, 2025)**:

#### SLI-RM-01-A (1479456_7_12_2):
- **Positive Energy**:
  - Jan 1: 58,454.69 kWh (constant all day)
  - Jan 2: 58,454.69 kWh (constant all day) - **SAME VALUE!**
  - Detected as cumulative: ✅ TRUE
  - Calculated daily energy: **0.00 kWh** (max - prev_max = 0)

- **Negative Energy**:
  - Jan 1: 40.73 kWh (constant all day)
  - Jan 2: 40.73 kWh (constant all day) - **SAME VALUE!**
  - Calculated daily energy: **0.00 kWh**

#### SLI-RM-01-F (1479456_7_3_1):
- **Positive Energy**:
  - Jan 1: 103,771.74 kWh (constant all day)
  - Jan 2: 103,771.74 kWh (constant all day) - **SAME VALUE!**
  - Calculated daily energy: **0.00 kWh**

- **Negative Energy**:
  - Jan 1: 51.98 kWh (constant all day)
  - Jan 2: 51.98 kWh (constant all day) - **SAME VALUE!**
  - Calculated daily energy: **0.00 kWh**

### Issue Analysis

**Problem**: Cumulative meters detected correctly, but values didn't increment between days!

**Possible Causes**:
1. **Meter Stuck**: Meters not incrementing (hardware issue)
2. **Data Quality**: Values not updating in source system
3. **Wrong Meters**: Excel might be using different meters
4. **Calculation Method**: Excel might use different calculation (not just revenue meters)

### Non-Revenue Meters (Have Variation)

**SLI-RM-02-A (1479456_7_13_2)** - NOT a revenue meter:
- Positive Energy: 86.38 → 88.92 kWh (incrementing!)
- Calculated: 2.54 kWh

**SLI-RM-02-F (1479456_7_4_1)** - NOT a revenue meter:
- Positive Energy: 92.79 → 95.80 kWh (incrementing!)
- Calculated: 3.01 kWh

**SLI-PQM-01-A (1479456_7_11_2)** - NOT a revenue meter:
- Positive Energy: 60,810,052 → 60,810,052 kWh (constant)
- Negative Energy: 43,073 → 43,073 kWh (constant)

**SLI-PQM-01-F (1479456_7_20_1)** - NOT a revenue meter:
- Positive Energy: 104,038.02 → 104,038.02 kWh (constant)
- Negative Energy: 51.36 → 51.36 kWh (constant)

### Hypothesis

**Excel might be using**:
- Non-revenue meters (SLI-RM-02-A, SLI-RM-02-F) which ARE incrementing
- Or a combination of all meters
- Or different calculation method

**Database is using**:
- Only revenue meters (SLI-RM-01-A, SLI-RM-01-F)
- Which are NOT incrementing (stuck meters)

### Action Required

1. **Verify Meter Status**:
   - [ ] Check if revenue meters (SLI-RM-01-A, SLI-RM-01-F) are actually working
   - [ ] Verify if meters are stuck or if this is data quality issue
   - [ ] Check field meter readings for those dates

2. **Compare with Excel**:
   - [ ] Document which meters Excel uses for Shoetown
   - [ ] Check if Excel uses non-revenue meters
   - [ ] Verify Excel calculation method

3. **Fix Calculation**:
   - [ ] If revenue meters are stuck, use non-revenue meters as fallback
   - [ ] Or fix meter data in source system
   - [ ] Update meter configuration if needed

---

## 📊 Summary

**Root Cause**: Revenue meters for Shoetown are not incrementing (stuck at same values), causing calculated daily energy = 0, while Excel likely uses different meters or calculation method.

**Impact**: 11 days in January 2025 show 0 energy in DB but have values in Excel.

**Solution**: Need to verify which meters Excel uses and either fix revenue meters or adjust calculation logic.

---

**Last Updated**: 2025-01-XX

