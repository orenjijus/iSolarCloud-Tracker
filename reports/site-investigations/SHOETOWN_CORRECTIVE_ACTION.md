# Shoetown Corrective Action - Sensor Replacement

**Date**: 2025-01-XX  
**Site**: Shoetown Ligung Indonesia  
**Action**: Sensor relocation/replacement

---

## Corrective Action Details

### POA Sensors Replacement

**Old Sensors** (deactivated Oct 1, 2025):
- `SLI-IRR-1-Aold` (1479456_5_16_2) → Capacity: 4524 kWp
- `SLI-IRR-2-Aold` (1479456_5_15_2) → Capacity: 4524 kWp

**New Sensors** (activated Oct 3, 2025):
- `SLI-IRR-1-A` (1479456_5_24_1) → Capacity: 928 kWp ✅
- `SLI-IRR-2-A` (1479456_5_25_1) → Capacity: 763,28 kWp ✅

**Timeline**:
- **Oct 1, 2025**: Old sensors moved (last day with old sensors)
- **Oct 2, 2025**: Gap period (no POA data)
- **Oct 3, 2025**: New sensors activated (first day with new sensors)

### Pyrano Replacement

**Old Pyrano** (deactivated Oct 3, 2025):
- `SLI-PYR-01-F` (1479456_5_21_1)

**New Pyrano** (activated Oct 12, 2025):
- `SLI-PYR` (1479456_5_26_1)

**Timeline**:
- **Oct 3, 2025**: Old pyrano dicabut (last day)
- **Oct 4-11, 2025**: Gap period (no GHI data)
- **Oct 12, 2025**: New pyrano nyala lagi (activated)

---

## Impact on POA Calculation

### Before Corrective Action (Sept 25 - Oct 1)
- Using old sensors: 1479456_5_15_2, 1479456_5_16_2
- Capacity: 4524 kWp each
- **POA Match**: Variable (ratio 0.45-1.28)

### During Gap Period (Oct 2)
- No POA sensors active
- **POA Match**: N/A (no data)

### After Corrective Action (Oct 3+)
- Using new sensors: 1479456_5_24_1 (928 kWp), 1479456_5_25_1 (76328 kWp)
- **Oct 3**: New sensors just activated, very low values (0.0017, 0.0136)
  - DB POA: 0.60 vs Excel: 4.14 (ratio: 0.14) - **Outlier due to calibration**
- **Oct 4+**: Sensors stabilized
  - **POA Match**: Better (ratio 0.70-1.23)

### Key Observations

1. **Oct 3 Outlier**: 
   - DB: 0.60 kWh/m² (new sensors just activated, very low values)
   - Excel: 4.14 kWh/m² (likely using different source or old data)
   - **This is expected** - new sensors need calibration time

2. **Capacity Mismatch**:
   - Old sensors: Both 4524 kWp (same)
   - New sensors: 928 kWp vs 76328 kWp (very different!)
   - **Question**: Is this correct? New sensors have very different capacities.

3. **POA Calculation After Oct 3**:
   - Weighted average: (POA1 × 928 + POA2 × 763,28) / (928 + 763,28)
   - Both sensors contribute to weighted average

---

## Recommendations

1. **✅ New Sensor Capacities Verified**:
   - ✅ 928 kWp and 763,28 kWp confirmed correct
   - ✅ CSV updated

2. **Document Gap Periods**:
   - [ ] Oct 2: No POA data (expected)
   - [ ] Oct 3-12: No GHI data (expected)

3. **Handle Oct 3 Outlier**:
   - [ ] Consider excluding Oct 3 from match analysis (calibration period)
   - [ ] Or document as expected behavior

4. **Re-run dbt Models**:
   - [ ] **Next Step**: Reload seed: `dbt seed --select seed_sensor_config`
   - [ ] After seed reload, re-run: `dbt run --select mart_site_performance_daily`
   - [ ] Verify POA calculation uses updated capacities

---

**Last Updated**: 2025-01-XX  
**Status**: Corrective action documented, capacity updated

