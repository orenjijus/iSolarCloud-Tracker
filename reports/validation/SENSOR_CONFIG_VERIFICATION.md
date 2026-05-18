# Sensor Config Verification

**Date**: 2025-01-XX  
**Status**: Verifying capacity updates

---

## Shoetown (1479456) - POA Sensors

### Old Sensors (deactivated Oct 1, 2025)
- `SLI-IRR-1-Aold` (1479456_5_16_2): **452,4 kWp** ✅
- `SLI-IRR-2-Aold` (1479456_5_15_2): **452,4 kWp** ✅

### New Sensors (activated Oct 3, 2025)
- `SLI-IRR-1-A` (1479456_5_24_1): **928 kWp** ✅ (updated in CSV)
- `SLI-IRR-2-A` (1479456_5_25_1): **763,28 kWp** ✅ (updated in CSV)

**Note**: CSV updated to reflect correct capacities.

---

## PLTS Rooftop Sumatera Prima (1680199) - POA Sensors

### All POA Sensors
- `IRR-01 (-31)` (1680199_5_11_2): **1462,58 kWp** ✅ (updated in CSV - line 114)
- `IRR-02 (149)` (1680199_5_12_2): 1162,5 kWp
- `IRR-04 (152)` (1680199_5_13_1): 231,88 kWp
- `IRR-03(-28)` (1680199_5_14_1): 189,72 kWp

**Note**: NULL capacity sensor now has capacity value (1462,58 kWp).

---

## CSV File Status

**File**: `dbt/seeds/seed_sensor_config.csv`

**Updates Made**:
1. ✅ Line 83: SLI-IRR-2-A (1479456_5_25_1) = 763,28 kWp
2. ✅ Line 84: SLI-IRR-1-A (1479456_5_24_1) = 928 kWp
3. ✅ Line 114: IRR-01 (-31) (1680199_5_11_2) = 1462,58 kWp

**Next Steps**:
1. [ ] Reload seed in database: `dbt seed --select seed_sensor_config`
2. [ ] Re-run dbt models: `dbt run --select mart_site_performance_daily`
3. [ ] Verify POA calculation uses updated capacities
4. [ ] Re-analyze POA match rates

---

**Last Updated**: 2025-01-XX  
**Status**: CSV updated, waiting for seed reload and model re-run

