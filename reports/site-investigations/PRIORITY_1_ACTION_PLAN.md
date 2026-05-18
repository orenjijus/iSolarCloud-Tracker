# Priority 1 Action Plan

**Date**: 2025-01-XX  
**Status**: In Progress

---

## ✅ Completed

### 1. Site Name Fix - PT. Pusan Manis Mulia
- **Status**: ✅ **FIXED**
- **Before**: 
  - DB: `PT. Pusan Manis Mulia 2.06 MWp - Tangerang`
  - Excel: `PT. Pusan Manis Mulia 2.06 MWp 0 Tangerang`
- **After**: ✅ Names now match
- **Verification**: Query confirms match

---

## 🔍 In Progress

### 2. Meter Issue Investigation - Madiun (Real Problem)

**Finding**: User confirmed that **Madiun meter is the real problem in the field** (not Shoetown). Shoetown differences are unexpected.

**Meter Configuration**:

#### Charoen Pokphand Madiun (site_id: ISO_SITE_1637816) ⚠️ **PROBLEM SITE**
- **Revenue Meters**:
  - `CPMAD-EM-POI-01` (esn_code: 1637816_7_15_1)
  - `CPMAD-EM-POI-02` (esn_code: 1637816_7_16_1)
- **Missing Data Pattern**:
  - **3 days missing in Excel** (2025-09-05, 2025-09-06, 2025-11-14) - DB has data
  - **Many days missing in DB** (July 29 - August 20, 2025) - Excel has data
  - **2 days mismatch** (2025-08-21, 2025-08-22) - small differences (0.14-0.40 MWh)
- **Energy Match**: 95.50% (106/111 days) - Actually good!
- **GHI Match**: 97.30% - Excellent
- **POA Match**: 98.20% - Excellent

**Root Cause Hypothesis**:
- Meter Madiun mengalami masalah di lapangan (offline, data tidak terkirim)
- Data ingestion gap di DB untuk periode Juli-Agustus 2025
- Excel mungkin menggunakan data manual atau backup

**Action Required**:
- [ ] **Priority**: Investigate Madiun meter data availability in source system
- [ ] Check data ingestion logs for missing periods (July 29 - August 20, 2025)
- [ ] Verify if meter was offline or data ingestion issue
- [ ] Check if Excel data for missing DB dates is correct
- [ ] Fill missing data in DB if available in source

#### Shoetown Ligung Indonesia (site_id: ISO_SITE_1479456) - Unexpected Differences
- **Revenue Meters**:
  - `SLI-RM-01-A` (esn_code: 1479456_7_12_2)
  - `SLI-RM-01-F` (esn_code: 1479456_7_3_1)
- **Energy Match**: 87.19% (279/320 days) - **Actually good!**
- **Missing Data Pattern**:
  - 11 days missing in DB (3.44%) - DB = 0, Excel has values
  - 23 days missing in Excel (7.19%) - Excel = 0, DB has values
  - 7 days mismatch (2.19%) - avg diff: 9.01 MWh
- **POA Match**: 3.05% - **CRITICAL ISSUE** (separate from energy)

**Finding**: 
- Energy match sebenarnya cukup baik (87.19%)
- POA match sangat rendah (3.05%) - ini yang membuat overall match rendah
- Missing data di kedua arah (DB dan Excel) - kemungkinan data quality issues

**Action Required**:
- [ ] Investigate missing data in DB (11 days)
- [ ] Investigate missing data in Excel (23 days)
- [ ] Investigate POA calculation (separate issue - see section 3)

---

### 3. POA Calculation Investigation - 5 Sites with <20% Match

**Sites Affected**:
1. **Shoetown Ligung Indonesia**: 3.05% match
2. **PLTS Frina Lestari Nusantara**: 12.10% match
3. **PLTS Rooftop Sumatera Prima Fibreboard**: 10.64% match
4. **Charoen Pokphand Bandung**: 13.79% match
5. **Garuda Metalindo (IKP)**: 15.88% match

**Common Pattern Identified**:
- ✅ **All sites show "DB_CONSISTENTLY_HIGHER" pattern**
- Average difference: 0.6-1.1 kWh/m²
- Max difference: 5.5-7.3 kWh/m²
- Standard deviation: Low (consistent difference)

**Root Cause Hypothesis**:

1. **Weighted Average Calculation Differences**:
   - Database: Capacity-weighted average of POA sensors
   - Excel: Mungkin menggunakan method berbeda (simple average, different weighting, atau exclude beberapa sensors)

2. **Sensor Selection Differences**:
   - Database: Uses all POA sensors per site
   - Excel: Mungkin exclude beberapa sensors atau menggunakan subset

3. **Override Date Handling**:
   - POA override effective from certain dates
   - Before/after override dates might have different calculations

4. **Unit Conversion Differences**:
   - Database handles multiple unit conversions
   - Excel might use different unit or conversion method

**Investigation Steps**:

#### Step 1: Compare POA Calculation Logic
- [ ] Document Excel POA calculation method
- [ ] Compare with database weighted average calculation
- [ ] Check if Excel uses capacity weighting or simple average

#### Step 2: Verify Sensor Selection
- [ ] List all POA sensors for each site in database
- [ ] Verify which sensors Excel uses
- [ ] Check if Excel excludes any sensors

#### Step 3: Check Override Dates
- [ ] Verify POA override effective dates
- [ ] Compare calculations before/after override dates
- [ ] Check if Excel handles overrides differently

#### Step 4: Analyze Specific Days
- [ ] Select sample days with large differences
- [ ] Compare sensor-by-sensor values
- [ ] Calculate weighted average manually to verify

**Query Created**: `investigate_poa_calculation.sql`
- Summary of POA differences per site
- Detail of top mismatches
- Ratio analysis to detect multiplier issues

---

## 📋 Next Steps

### Immediate Actions (This Week)

1. **Meter Investigation**:
   - [ ] Query meter data for Madiun and Shoetown
   - [ ] Check if Madiun meter affects Shoetown
   - [ ] Verify meter assignment and data flow

2. **POA Calculation**:
   - [ ] Run `investigate_poa_calculation.sql` for detailed analysis
   - [ ] Document Excel POA calculation method
   - [ ] Compare with database calculation logic
   - [ ] Identify specific differences

3. **Missing Data**:
   - [ ] Investigate missing data in Shoetown (44 days)
   - [ ] Check if related to Madiun meter issue
   - [ ] Verify data exists in source system

### Follow-up Actions (Next Week)

1. **Fix POA Calculation**:
   - [ ] Adjust database calculation if needed
   - [ ] Or document Excel calculation differences
   - [ ] Re-run models and verify

2. **Fix Missing Data**:
   - [ ] Fill missing data if available
   - [ ] Update data ingestion process if needed

3. **Documentation**:
   - [ ] Document findings
   - [ ] Update calculation guide
   - [ ] Create comparison report

---

## 📊 Current Status Summary

| Priority | Item | Status | Progress |
|----------|------|--------|----------|
| 1 | Site Name Fix | ✅ Complete | 100% |
| 1 | Meter Issue (Madiun) | 🔍 In Progress | 20% |
| 1 | POA Calculation | 🔍 In Progress | 30% |
| 1 | Missing Data | 🔍 In Progress | 10% |

---

**Last Updated**: 2025-01-XX  
**Next Review**: After meter investigation and POA calculation analysis

