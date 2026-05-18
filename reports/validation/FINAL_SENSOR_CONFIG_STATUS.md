# Final Sensor Config Status

**Date**: 2025-01-XX  
**Status**: CSV Updated, Waiting for Seed Reload

---

## ✅ Updates Completed

### Shoetown (1479456)

**CSV File Updated** (`dbt/seeds/seed_sensor_config.csv`):
- ✅ Line 83: `SLI-IRR-2-A` (1479456_5_25_1) = **763,28 kWp** (was 452,4)
- ✅ Line 84: `SLI-IRR-1-A` (1479456_5_24_1) = **928 kWp** (was 452,4)

**Corrective Action**:
- Old sensors (deactivated Oct 1): 452,4 kWp each
- New sensors (activated Oct 3): 928 kWp and 763,28 kWp

### PLTS Rooftop Sumatera Prima (1680199)

**CSV File Updated**:
- ✅ Line 114: `IRR-01 (-31)` (1680199_5_11_2) = **1462,58 kWp** (was NULL)

**Impact**:
- This sensor will now be included in POA weighted average calculation
- May improve match with Excel (if Excel uses this sensor)

---

## ⚠️ Next Steps Required

### 1. Reload Seed in Database
```bash
dbt seed --select seed_sensor_config
```

**Why**: CSV file updated but database seed table not yet reloaded.

**Current Status in DB**:
- `SLI-IRR-2-A` (1479456_5_25_1): Still shows 76328 (needs reload)
- `IRR-01 (-31)` (1680199_5_11_2): Still shows NULL (needs reload)

### 2. Re-run dbt Models
```bash
dbt run --select mart_site_performance_daily
```

**Why**: POA calculation needs to use updated capacities.

### 3. Verify Changes
- [ ] Check if POA calculation improved
- [ ] Re-analyze POA match rates
- [ ] Verify NULL capacity sensor now included

---

## 📊 Expected Impact

### Shoetown
- **Before**: New sensors had wrong capacity (452,4 kWp each)
- **After**: Correct capacity (928 kWp and 763,28 kWp)
- **Impact**: POA weighted average will be more accurate

### PLTS Rooftop Sumatera Prima
- **Before**: Sensor 1680199_5_11_2 excluded (NULL capacity)
- **After**: Sensor included with 1462,58 kWp capacity
- **Impact**: POA calculation will include this sensor, may improve match

---

**Last Updated**: 2025-01-XX  
**Status**: CSV updated ✅, Waiting for seed reload and model re-run

