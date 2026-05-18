# Final Investigation Summary - Priority 1 Issues

**Date**: 2025-01-XX  
**Status**: Root Causes Identified

---

## ✅ Completed Investigations

### 1. Site Name Fix
- **Status**: ✅ **FIXED**
- Site name Pusan sudah match antara DB dan Excel

---

## 🔴 Critical Issues Identified

### Issue 1: Shoetown Revenue Meters Stuck (Not Incrementing)

**Problem**:
- DB shows Energy = 0 for 11 days in January 2025
- Excel shows Energy = 30.85 MWh (example: Jan 2)
- Meter data EXISTS in source system

**Root Cause** ✅:
- **Revenue meters** (SLI-RM-01-A, SLI-RM-01-F) are **stuck** - values not incrementing
- Jan 1: 58,454.69 kWh
- Jan 2: 58,454.69 kWh (SAME - not incrementing!)
- Calculation: max - prev_max = 0 → Energy = 0

**Non-Revenue Meters** (SLI-RM-02-A, SLI-RM-02-F):
- **ARE incrementing** (86.38 → 88.92, 92.79 → 95.80)
- These meters show actual generation

**Hypothesis**:
- Excel might be using non-revenue meters (which are working)
- Or Excel uses different calculation method
- Or revenue meters need to be fixed/replaced

**Action Required**:
- [ ] **URGENT**: Verify which meters Excel uses for Shoetown
- [ ] Check if revenue meters are actually stuck (hardware issue)
- [ ] Consider using non-revenue meters as fallback
- [ ] Update meter configuration if needed

**Files**:
- `SHOETOWN_CALCULATION_ISSUE.md` - Detailed analysis
- `investigate_shoetown_differences.sql` - Investigation queries

---

### Issue 2: Madiun Meter Offline (Confirmed)

**Problem**:
- DB shows Energy = 0 for 23 days (July 29 - August 20, 2025)
- Excel has data for those dates (manual entry?)

**Root Cause** ✅:
- **Meter was offline** - confirmed by user
- No meter data in source system for missing dates
- This is a field issue, not calculation issue

**Action Required**:
- [ ] Document meter offline period
- [ ] Verify Excel data source (manual entry?)
- [ ] Check if data can be recovered

**Files**:
- `investigate_madiun_meter_issue.sql` - Investigation queries

---

### Issue 3: POA Calculation Differences (5 Sites)

**Affected Sites**:
1. Shoetown Ligung: 3.05% match
2. PLTS Frina Lestari: 12.10% match
3. PLTS Rooftop Sumatera Prima: 10.64% match
4. Charoen Pokphand Bandung: 13.79% match
5. Garuda Metalindo (IKP): 15.88% match

**Pattern**:
- All sites: DB consistently higher than Excel
- Average difference: 0.6-1.1 kWh/m²
- Consistent (not random)

**Database Calculation**:
- Weighted average: SUM(poa_value × capacity) / SUM(capacity)
- Uses all POA sensors with capacity from seed_sensor_config

**Example (Shoetown 2025-10-01)**:
- DB: 5.3709 kWh/m²
- Excel: 4.67 kWh/m²
- Difference: 0.70 kWh/m² (15% higher)

**POA Sensors Found**:
- Shoetown: 4 sensors (2×4524, 1×928, 1×76328 kWp)
- Some sensors have NULL capacity (excluded from DB)
- Some sensors have different activation dates

**Action Required**:
- [ ] **Priority**: Document Excel POA calculation method
- [ ] Verify which sensors Excel uses for each site
- [ ] Check if Excel excludes NULL capacity sensors
- [ ] Compare weighted average vs simple average

**Files**:
- `investigate_poa_calculation.sql` - POA analysis queries
- `check_poa_sensors_per_site.sql` - Sensor listing

---

## 📊 Summary Statistics

### Energy Match Rates
- **Madiun**: 95.50% (106/111 days) - Good! (missing days due to meter offline)
- **Shoetown**: 87.19% (279/320 days) - Good! (missing days due to stuck meters)

### POA Match Rates (Critical)
- **Shoetown**: 3.05% - CRITICAL
- **PLTS Frina Lestari**: 12.10% - CRITICAL
- **PLTS Rooftop Sumatera Prima**: 10.64% - CRITICAL
- **Charoen Pokphand Bandung**: 13.79% - CRITICAL
- **Garuda Metalindo (IKP)**: 15.88% - CRITICAL

---

## 🎯 Next Steps

### Immediate (This Week)

1. **Shoetown Meter Issue**:
   - [ ] Verify which meters Excel uses
   - [ ] Check if revenue meters need repair/replacement
   - [ ] Consider fallback to non-revenue meters

2. **POA Calculation**:
   - [ ] Document Excel POA calculation method
   - [ ] Compare sensor selection
   - [ ] Verify calculation method differences

3. **Madiun**:
   - [ ] Document meter offline period
   - [ ] Verify Excel data source

### Follow-up (Next Week)

1. **Fix Shoetown Calculation**:
   - [ ] Update meter configuration if needed
   - [ ] Or fix revenue meters
   - [ ] Re-run models and verify

2. **Fix POA Calculation**:
   - [ ] Adjust database calculation if needed
   - [ ] Or document differences
   - [ ] Update documentation

---

## 📁 Files Created

### Investigation Queries
1. `investigate_madiun_meter_issue.sql` - Madiun meter investigation
2. `investigate_shoetown_differences.sql` - Shoetown investigation
3. `investigate_poa_calculation.sql` - POA calculation analysis
4. `check_poa_sensors_per_site.sql` - POA sensor listing

### Analysis Reports
1. `SHOETOWN_CALCULATION_ISSUE.md` - Shoetown root cause analysis
2. `INVESTIGATION_FINDINGS.md` - All findings summary
3. `DETAILED_INVESTIGATION_SUMMARY.md` - Detailed analysis
4. `PRIORITY_1_ACTION_PLAN.md` - Action plan
5. `FINAL_INVESTIGATION_SUMMARY.md` - This file

---

**Last Updated**: 2025-01-XX  
**Status**: Root causes identified, action items defined

