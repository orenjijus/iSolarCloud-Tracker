# Updated Analysis After Capacity Fix

**Date**: 2025-01-XX  
**Update**: Capacity values updated for Shoetown and PLTS Rooftop Sumatera Prima

---

## Capacity Updates

### Shoetown Ligung Indonesia

**POA Sensors**:
- `SLI-IRR-1-Aold` (1479456_5_16_2): **4524 kWp** ✅ (updated from 452.4)
- `SLI-IRR-2-Aold` (1479456_5_15_2): **4524 kWp** ✅ (updated from 452.4)
- `SLI-IRR-1-A` (1479456_5_24_1): **928 kWp** ✅
- `SLI-IRR-2-A` (1479456_5_25_1): **76328 kWp** ✅

**Note**: New sensors have very different capacities (928 vs 76328). Need to verify if this is correct.

### PLTS Rooftop Sumatera Prima Fibreboard

**POA Sensors**:
- `IRR-01 (-31)` (1680199_5_11_2): **NULL** ⚠️ (still NULL - need to check if updated)
- `IRR-02 (149)` (1680199_5_12_2): 11625 kWp
- `IRR-04 (152)` (1680199_5_13_1): 23188 kWp
- `IRR-03(-28)` (1680199_5_14_1): 18972 kWp

**Note**: User mentioned they updated capacity for Sumatera Prima, but sensor 1680199_5_11_2 still shows NULL. Need to verify.

---

## Impact on POA Calculation

### Shoetown - Before vs After Capacity Update

**Before Update** (Old sensors with 452.4 kWp):
- Weighted average calculation used incorrect capacity
- POA values would be incorrect

**After Update** (Old sensors with 4524 kWp):
- Weighted average calculation now uses correct capacity
- **Action Required**: Re-run dbt models to apply updated capacity

**After Corrective Action** (New sensors):
- Oct 1: Last day with old sensors (4524 kWp each)
- Oct 2: Gap (no sensors)
- Oct 3: First day with new sensors (928 + 76328 kWp)
  - **Outlier**: DB 0.60 vs Excel 4.14 (new sensors calibrating)
- Oct 4+: Sensors stabilized

### PLTS Rooftop Sumatera Prima

**If NULL capacity sensor gets capacity value**:
- Sensor 1680199_5_11_2 will be included in weighted average
- POA calculation will change
- May improve match with Excel (if Excel uses this sensor)

---

## Next Steps

1. **Re-run dbt Models**:
   - [ ] Run `dbt run --select mart_site_performance_daily`
   - [ ] Verify POA calculation uses updated capacities

2. **Verify Capacity Values**:
   - [ ] Confirm Shoetown new sensor capacities (928 vs 76328)
   - [ ] Check if PLTS Rooftop Sumatera Prima NULL sensor was updated

3. **Re-analyze POA Match**:
   - [ ] After re-running models, re-check POA match rates
   - [ ] Verify if differences improved

4. **Document Corrective Action**:
   - [ ] Document sensor replacement dates
   - [ ] Document gap periods (Oct 2 for POA, Oct 3-12 for GHI)

---

**Last Updated**: 2025-01-XX  
**Status**: Capacity updated, waiting for dbt re-run

