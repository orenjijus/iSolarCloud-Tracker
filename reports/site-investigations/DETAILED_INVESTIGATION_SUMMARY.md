# Detailed Investigation Summary

**Date**: 2025-01-XX  
**Status**: In Progress

---

## Key Findings

### 1. Madiun Meter Issue (Real Problem in Field) ⚠️

**Site**: Charoen Pokphand Madiun  
**Status**: Meter bermasalah di lapangan (confirmed by user)

**Statistics**:
- **Energy Match**: 95.50% (106/111 days) - Actually good!
- **GHI Match**: 97.30% - Excellent
- **POA Match**: 98.20% - Excellent
- **Overall Match**: 93.69% - Excellent

**Missing Data**:
- **3 days missing in Excel**: 2025-09-05, 2025-09-06, 2025-11-14
  - DB has data, Excel = 0
  - Energy: 2.64-4.43 MWh
- **Many days missing in DB**: July 29 - August 20, 2025 (23 days)
  - Excel has data, DB = 0
  - All data missing (energy, GHI, POA)

**Mismatches**:
- Only 2 days with small differences:
  - 2025-08-21: DB 1.32 MWh vs Excel 1.72 MWh (diff: 0.40 MWh)
  - 2025-08-22: DB 2.73 MWh vs Excel 2.87 MWh (diff: 0.14 MWh)

**Root Cause**:
- Meter Madiun mengalami masalah di lapangan
- Data ingestion gap di DB untuk periode Juli-Agustus 2025
- Excel mungkin menggunakan data manual atau backup

**Action Required**:
- [ ] **Priority 1**: Check meter data availability in source system for missing dates
- [ ] Verify if meter was offline or data ingestion issue
- [ ] Fill missing data in DB if available in source
- [ ] Document meter issue for field team

---

### 2. Shoetown Differences (Unexpected) 

**Site**: Shoetown Ligung Indonesia  
**Status**: User surprised by differences

**Statistics**:
- **Energy Match**: 87.19% (279/320 days) - **Actually good!**
- **GHI Match**: 86.98% - Good
- **POA Match**: 3.05% - **CRITICAL ISSUE**
- **Overall Match**: 1.11% - Low due to POA

**Missing Data Breakdown**:
- **11 days missing in DB** (3.44%):
  - DB = 0, Excel has values
  - Dates: Mostly January 2025
  - Energy values: 9.88-57.17 MWh
- **23 days missing in Excel** (7.19%):
  - Excel = 0, DB has values
  - Dates: Scattered throughout 2025
  - Energy values: 8.55-12.39 MWh
- **7 days mismatch** (2.19%):
  - Both have data but different
  - Avg diff: 9.01 MWh
  - Max diff: 21.92 MWh

**Key Insight**:
- Energy match sebenarnya cukup baik (87.19%)
- POA match sangat rendah (3.05%) - ini yang membuat overall match rendah
- Missing data di kedua arah menunjukkan data quality issues

**Action Required**:
- [ ] Investigate missing data in DB (11 days in January 2025)
- [ ] Investigate missing data in Excel (23 days scattered)
- [ ] Investigate POA calculation (see section 3)

---

### 3. POA Calculation Issue (5 Sites with <20% Match)

**Affected Sites**:
1. Shoetown Ligung Indonesia: 3.05% match
2. PLTS Frina Lestari Nusantara: 12.10% match
3. PLTS Rooftop Sumatera Prima Fibreboard: 10.64% match
4. Charoen Pokphand Bandung: 13.79% match
5. Garuda Metalindo (IKP): 15.88% match

**Common Pattern**:
- ✅ **All sites show "DB_CONSISTENTLY_HIGHER" pattern**
- Average difference: 0.6-1.1 kWh/m²
- Max difference: 5.5-7.3 kWh/m²
- Standard deviation: Low (consistent difference)

**Database POA Calculation**:
```sql
-- Weighted average: SUM(poa_value * capacity) / SUM(capacity)
SUM(max_daily_poa_kwh_m2 * poa_capacity_kwp) / NULLIF(SUM(poa_capacity_kwp), 0)
```

**Root Cause Hypothesis**:
1. **Weighted Average Method Differences**:
   - Database: Capacity-weighted average of all POA sensors
   - Excel: Mungkin menggunakan method berbeda (simple average, different weighting, atau exclude beberapa sensors)

2. **Sensor Selection Differences**:
   - Database: Uses all POA sensors per site (from seed_sensor_config where sensor_type = 'POA')
   - Excel: Mungkin exclude beberapa sensors atau menggunakan subset

3. **Override Date Handling**:
   - POA override effective from certain dates
   - Before/after override dates might have different calculations

4. **Unit Conversion Differences**:
   - Database handles multiple unit conversions (MJ/m², Wh/m², etc.)
   - Excel might use different unit or conversion method

**Action Required**:
- [ ] **Priority 1**: Document Excel POA calculation method
- [ ] Compare sensor selection between DB and Excel
- [ ] Check if Excel uses capacity weighting or simple average
- [ ] Verify override date handling
- [ ] Run `check_poa_sensors_per_site.sql` to see which sensors are used

---

## Investigation Queries Created

1. **`investigate_madiun_meter_issue.sql`**:
   - Missing data analysis for Madiun
   - Summary statistics
   - Meter data availability check

2. **`investigate_shoetown_differences.sql`**:
   - Missing data pattern analysis
   - Top mismatches
   - Meter data availability check

3. **`investigate_poa_calculation.sql`**:
   - POA differences summary per site
   - Top mismatches detail
   - Ratio analysis

4. **`check_poa_sensors_per_site.sql`**:
   - List POA sensors per site
   - Check sensor data availability
   - Verify weighted average calculation

---

## Next Steps

### Immediate (This Week)

1. **Madiun Meter Issue**:
   - [ ] Run `investigate_madiun_meter_issue.sql`
   - [ ] Check source system for missing data (July 29 - August 20, 2025)
   - [ ] Verify meter status during missing period
   - [ ] Fill missing data if available

2. **Shoetown Missing Data**:
   - [ ] Run `investigate_shoetown_differences.sql`
   - [ ] Check source system for missing DB dates (11 days in January)
   - [ ] Verify Excel missing dates (23 days)

3. **POA Calculation**:
   - [ ] Run `check_poa_sensors_per_site.sql` for all 5 sites
   - [ ] Document Excel POA calculation method
   - [ ] Compare sensor selection

### Follow-up (Next Week)

1. **Fix Missing Data**:
   - [ ] Fill missing data in DB if available
   - [ ] Update data ingestion process if needed

2. **Fix POA Calculation**:
   - [ ] Adjust database calculation if needed
   - [ ] Or document Excel calculation differences
   - [ ] Re-run models and verify

---

**Last Updated**: 2025-01-XX  
**Status**: Investigation in progress

