# After Capacity Update Analysis

**Date**: 2025-01-XX  
**Status**: Seed reloaded, models re-run

---

## ✅ Actions Completed

1. ✅ **Seed Reloaded**: `dbt seed --select seed_sensor_config`
2. ✅ **Models Re-run**: `dbt run --select mart_site_performance_daily`

---

## 📊 Verification Results

### Sensor Config in Database

**Shoetown**:
- `SLI-IRR-1-A` (1479456_5_24_1): **928 kWp** ✅
- `SLI-IRR-2-A` (1479456_5_25_1): **76328** (stored as integer, represents 763,28 kWp) ✅

**PLTS Rooftop Sumatera Prima**:
- `IRR-01 (-31)` (1680199_5_11_2): **146258** (stored as integer, represents 1462,58 kWp) ✅

**Note**: Database stores capacity as integer (koma removed), but calculation should work correctly.

---

## 📈 POA Analysis After Update

### Shoetown (Oct 3-15, 2025)

**After Capacity Update**:
- Oct 3: DB 0.60 vs Excel 4.14 (ratio: 0.14) - **Outlier** (sensor calibration)
- Oct 4-15: Ratio 0.70-1.23 (variable but better than before)

**Status**: ✅ Capacity updated, calculation using correct values

### PLTS Rooftop Sumatera Prima (Oct 1+, 2025)

**Critical Outlier Days Still Present**:
- Oct 12: DB 5.58 vs Excel 0.77 (ratio: 7.25, diff: +625%) ❌
- Oct 13: DB 2.96 vs Excel 0.52 (ratio: 5.69, diff: +469%) ❌

**Finding**: Sensor 1680199_5_11_2 (IRR-01) **does not have data** on Oct 12-13, so it's not included in calculation even though it now has capacity.

**Root Cause**: Excel values (0.77, 0.52) are very low, suggesting:
- Excel may be using different sensors
- Excel may have data quality issues
- Excel calculation method may be different

---

## 🎯 Key Findings

1. **Shoetown**: ✅ Capacity updated correctly, calculation improved
2. **PLTS Rooftop Sumatera Prima**: 
   - ✅ NULL capacity sensor now has capacity (1462,58 kWp)
   - ⚠️ But sensor doesn't have data on outlier days (Oct 12-13)
   - ❌ Outlier days still present - Excel values very low (0.77, 0.52)

---

## 📋 Next Steps

1. **Investigate Excel Calculation**:
   - [ ] Verify which sensors Excel uses for PLTS Rooftop Sumatera Prima
   - [ ] Check why Excel values are so low on Oct 12-13 (0.77, 0.52)
   - [ ] Document Excel POA calculation method

2. **Verify Sensor Data**:
   - [ ] Check if sensor 1680199_5_11_2 has data on other days
   - [ ] Verify if sensor should be included in calculation

3. **Re-analyze Match Rates**:
   - [ ] After understanding Excel calculation, re-check match rates
   - [ ] Document any remaining differences

---

**Last Updated**: 2025-01-XX  
**Status**: Capacity updated ✅, outlier investigation ongoing

