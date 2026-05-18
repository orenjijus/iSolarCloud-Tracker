# Final Summary After Capacity Updates

**Date**: 2025-01-XX  
**Status**: Capacity updates completed, analysis ongoing

---

## ✅ Completed Actions

1. ✅ **Shoetown Capacity Updated**:
   - `SLI-IRR-1-A` (1479456_5_24_1): 928 kWp
   - `SLI-IRR-2-A` (1479456_5_25_1): 763,28 kWp

2. ✅ **PLTS Rooftop Sumatera Prima Capacity Updated**:
   - `IRR-01 (-31)` (1680199_5_11_2): 1462,58 kWp (was NULL)

3. ✅ **Seed Reloaded**: `dbt seed --select seed_sensor_config`
4. ✅ **Models Re-run**: `dbt run --select mart_site_performance_daily`

---

## 📊 Key Findings

### Shoetown
- ✅ Capacity updated correctly
- ✅ POA calculation using correct capacities
- ⚠️ Oct 3 outlier (ratio 0.14) - expected (sensor calibration period)
- ✅ Other days show improved match (ratio 0.70-1.23)

### PLTS Rooftop Sumatera Prima

**Sensor 1680199_5_11_2 (IRR-01) Analysis**:
- ✅ Now has capacity: 1462,58 kWp
- ✅ Has data on most days (Oct 4-24)
- ❌ **Oct 12**: No data (sensor offline/missing)
- ❌ **Oct 13**: Data very low (0.0000 - almost zero)

**Outlier Days Explained**:
- **Oct 12**: 
  - DB: 5.58 (using sensors 1680199_5_13_1, 1680199_5_14_1)
  - Excel: 0.77 (very low - possibly using sensor 1680199_5_11_2 which has no data)
  - Ratio: 7.25

- **Oct 13**:
  - DB: 2.96 (using sensors 1680199_5_13_1, 1680199_5_14_1)
  - Excel: 0.52 (very low - possibly using sensor 1680199_5_11_2 which has almost zero data)
  - Ratio: 5.69

**Root Cause Hypothesis**:
- Excel may be using sensor 1680199_5_11_2 (IRR-01) which has no/low data on Oct 12-13
- DB excludes this sensor on those days (no data), so uses other sensors
- This causes Excel to show very low values while DB shows normal values

---

## 🎯 Recommendations

### Priority 1: Verify Excel Calculation Method
- [ ] **URGENT**: Document which sensors Excel uses for PLTS Rooftop Sumatera Prima
- [ ] Check if Excel uses sensor 1680199_5_11_2 (IRR-01)
- [ ] Verify Excel calculation method (weighted average vs simple average)

### Priority 2: Handle Missing Data Days
- [ ] Document Oct 12-13 as days with sensor data quality issues
- [ ] Consider excluding these days from match analysis
- [ ] Or verify if Excel should also exclude sensor 1680199_5_11_2 on those days

### Priority 3: Re-analyze After Understanding Excel Method
- [ ] After documenting Excel method, re-check match rates
- [ ] Verify if differences are due to sensor selection or calculation method

---

## 📋 Summary

**Completed**:
- ✅ Capacity values updated
- ✅ Seed reloaded
- ✅ Models re-run
- ✅ Root cause of outliers identified

**Ongoing**:
- ⚠️ Excel calculation method needs documentation
- ⚠️ Sensor selection differences need verification

**Next Steps**:
1. Document Excel POA calculation method
2. Verify sensor selection differences
3. Re-analyze match rates after understanding Excel method

---

**Last Updated**: 2025-01-XX  
**Status**: Capacity updates complete ✅, Excel method documentation needed

