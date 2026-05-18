# Analisis Detail: Large Differences Excel vs Database

**Tanggal Analisis**: 2025-01-XX  
**Total Large Differences**: 3,828 records  
**Total Sites**: 15 sites dengan large differences

---

## Executive Summary

### Overall Statistics
- **Total Records dengan Large Differences**: 3,828 records
- **Sites dengan Most Issues**: 
  1. PT. MMKI 5.7 MWp - Phase 2: **797 records**
  2. PT. MMKI 1.75 MWp - Painting Building: **669 records**
  3. Shoetown Ligung Indonesia: **656 records**

### Critical Anomalies Found
1. **PR POA = 8379.63%** (Garuda Metalindo IKP, 2025-09-15) - **CRITICAL** ⚠️⚠️⚠️
2. **Energy = 0 di DB** (Shoetown Ligung, multiple dates) - Missing data issue
3. **Energy = 0 di Excel** (Multiple sites) - Missing data issue
4. **PR GHI = 15.81%** (MMKI II, 2025-04-22) - Excel: 0.77%, DB: 15.82%

---

## Summary per Site

| Rank | Site | Total | Energy | GHI | POA | PR GHI | PR POA | Availability |
|------|------|-------|--------|-----|-----|--------|--------|--------------|
| 1 | **PT. MMKI 5.7 MWp - Phase 2** | **797** | 52 | 187 | 16 | 268 | 273 | 1 |
| 2 | **PT. MMKI 1.75 MWp - Painting Building** | **669** | 1 | 191 | 152 | 169 | 128 | 28 |
| 3 | **Shoetown Ligung Indonesia** | **656** | 32 | 6 | 290 | 28 | 297 | 3 |
| 4 | **PT. Pusan Manis Mulia 2.06 MWp - Tangerang** | **461** | 3 | 1 | 241 | 4 | 212 | 0 |
| 5 | **Garuda Metalindo (IKP)** | **403** | 9 | 4 | 193 | 8 | 189 | 0 |
| 6 | **PT. MMKI 4.292 MWP - Phase 3** | **328** | 4 | 101 | 1 | 161 | 60 | 1 |
| 7 | **Garuda Metalindo 1** | **259** | 2 | 1 | 134 | 3 | 118 | 1 |
| 8 | **Charoen Pokphand Bandung** | **184** | 21 | 1 | 70 | 21 | 64 | 7 |
| 9 | **PLTS Frina Lestari Nusantara** | **115** | 9 | 0 | 45 | 9 | 44 | 8 |
| 10 | **PLTS Rooftop Sumatera Prima Fibreboard** | **48** | 3 | 0 | 18 | 2 | 14 | 11 |
| 11 | **PLTS Mall Panakkukang** | **37** | 0 | 1 | 2 | 32 | 1 | 1 |
| 12 | **Charoen Pokphand Madiun** | **17** | 4 | 0 | 1 | 4 | 4 | 4 |
| 13 | **Garuda Metalindo (MPF)** | **16** | 4 | 0 | 0 | 6 | 6 | 0 |
| 14 | **Charoen Pokphand Majalengka** | **15** | 5 | 0 | 0 | 5 | 5 | 0 |
| 15 | **Garuda Metalindo 2** | **11** | 2 | 2 | 0 | 4 | 2 | 1 |

---

## Top 50 Largest Differences

### Critical Anomalies (Top 10)

1. **PR POA: 8379.63%** (Garuda Metalindo IKP, 2025-09-15)
   - Excel: 0.0000
   - DB: 8379.6289
   - **Issue**: Division by zero atau calculation error - **CRITICAL**

2. **Energy: 57.17 MWh** (Shoetown Ligung, 2025-01-04)
   - Excel: 57.1700 MWh
   - DB: 0.0000 MWh
   - **Issue**: Missing data di DB

3. **Energy: 54.50 MWh** (Shoetown Ligung, 2025-01-03)
   - Excel: 54.5000 MWh
   - DB: 0.0000 MWh
   - **Issue**: Missing data di DB

4. **Energy: 46.30 MWh** (Shoetown Ligung, 2025-01-05)
   - Excel: 46.3000 MWh
   - DB: 0.0000 MWh
   - **Issue**: Missing data di DB

5. **Energy: 30.85 MWh** (Shoetown Ligung, 2025-01-02)
   - Excel: 30.8500 MWh
   - DB: 0.0000 MWh
   - **Issue**: Missing data di DB

6. **Energy: 21.92 MWh** (Shoetown Ligung, 2025-03-06)
   - Excel: 13.4100 MWh
   - DB: 35.3252 MWh
   - **Issue**: Large calculation difference

7. **Energy: 20.49 MWh** (MMKI II, 2025-02-13)
   - Excel: 27.1079 MWh
   - DB: 6.6178 MWh
   - **Issue**: Large calculation difference (74% relative diff)

8. **Energy: 18.52 MWh** (MMKI III, 2025-07-21)
   - Excel: 28.0279 MWh
   - DB: 9.5092 MWh
   - **Issue**: Large calculation difference (66% relative diff)

9. **Energy: 18.45 MWh** (MMKI II, 2025-02-21)
   - Excel: 24.8153 MWh
   - DB: 6.3614 MWh
   - **Issue**: Large calculation difference (74% relative diff)

10. **PR GHI: 15.81%** (MMKI II, 2025-04-22)
    - Excel: 0.0077 (0.77%)
    - DB: 15.8157 (1581.57%)
    - **Issue**: Calculation error atau missing reference value

---

## Detailed Analysis by Site

### 1. PT. MMKI 5.7 MWp - Phase 2 ⚠️ **MOST CRITICAL**

**Total Large Differences**: 797 records

**Breakdown**:
- PR POA: 273 records (34.3%)
- PR GHI: 268 records (33.6%)
- GHI: 187 records (23.5%)
- Energy: 52 records (6.5%)
- POA: 16 records (2.0%)
- Availability: 1 record (0.1%)

**Key Issues**:
1. **PR Calculations**: 541 records (68%) dengan large differences
   - PR GHI: Max diff = 15.81% (Excel: 0.77%, DB: 1581.57%)
   - PR POA: Max diff = 9.79% (Excel: 0%, DB: 979.46%)
   - **Root Cause**: Kemungkinan calculation formula berbeda atau missing reference values

2. **Energy**: 52 records dengan large differences
   - Pattern: Excel > DB (74% relative difference)
   - Dates: February 2025 (multiple dates)
   - Max diff: 20.49 MWh (2025-02-13)
   - **Root Cause**: Kemungkinan meter selection atau aggregation method berbeda

3. **GHI**: 187 records dengan large differences
   - Pattern: Mixed (Excel > DB dan Excel < DB)
   - **Root Cause**: Calculation method atau fallback logic berbeda

**Priority Dates untuk Investigation**:
- 2025-02-13: Energy diff = 20.49 MWh
- 2025-02-21: Energy diff = 18.45 MWh
- 2025-04-22: PR GHI diff = 15.81%
- 2025-09-13: PR POA diff = 9.79%

---

### 2. PT. MMKI 1.75 MWp - Painting Building

**Total Large Differences**: 669 records

**Breakdown**:
- GHI: 191 records (28.6%)
- POA: 152 records (22.7%)
- PR GHI: 169 records (25.3%)
- PR POA: 128 records (19.1%)
- Availability: 28 records (4.2%)
- Energy: 1 record (0.1%)

**Key Issues**:
1. **GHI & POA**: 343 records (51%) dengan large differences
   - GHI: Max diff = 7.13 kWh/m² (2025-07-06)
   - POA: Max diff = 1.10 kWh/m² (2025-04-29)
   - **Root Cause**: Calculation method differences

2. **PR Calculations**: 297 records (44%) dengan large differences
   - PR POA: Max diff = 12.50% (2025-09-14)
   - **Root Cause**: PR calculation formula berbeda

**Priority Dates untuk Investigation**:
- 2025-07-06: GHI diff = 7.13 kWh/m²
- 2025-09-14: PR POA diff = 12.50%
- 2025-04-29: POA diff = 1.10 kWh/m²

---

### 3. Shoetown Ligung Indonesia

**Total Large Differences**: 656 records

**Breakdown**:
- POA: 290 records (44.2%)
- PR POA: 297 records (45.3%)
- Energy: 32 records (4.9%)
- PR GHI: 28 records (4.3%)
- GHI: 6 records (0.9%)
- Availability: 3 records (0.5%)

**Key Issues**:
1. **Energy Missing Data**: Multiple dates dengan Energy = 0 di DB
   - 2025-01-02: Excel 30.85 MWh, DB 0 MWh
   - 2025-01-03: Excel 54.50 MWh, DB 0 MWh
   - 2025-01-04: Excel 57.17 MWh, DB 0 MWh
   - 2025-01-05: Excel 46.30 MWh, DB 0 MWh
   - **Root Cause**: Missing data di database untuk early January 2025

2. **POA**: 290 records (44%) dengan large differences
   - **Root Cause**: Calculation method sangat berbeda

3. **PR POA**: 297 records (45%) dengan large differences
   - Max diff = 11.42% (2025-01-05)
   - **Root Cause**: PR calculation berbeda

**Priority Dates untuk Investigation**:
- 2025-01-02 to 2025-01-05: Missing energy data di DB
- 2025-01-05: PR POA diff = 11.42%
- Multiple dates: POA large differences

---

### 4. PT. Pusan Manis Mulia 2.06 MWp - Tangerang

**Total Large Differences**: 461 records

**Breakdown**:
- POA: 241 records (52.3%)
- PR POA: 212 records (46.0%)
- Energy: 3 records (0.7%)
- PR GHI: 4 records (0.9%)
- GHI: 1 record (0.2%)

**Key Issues**:
1. **POA**: 241 records (52%) dengan large differences
   - **Root Cause**: Calculation method sangat berbeda

2. **PR POA**: 212 records (46%) dengan large differences
   - **Root Cause**: PR calculation berbeda

**Action Required**: Investigate POA dan PR POA calculation methods

---

### 5. Garuda Metalindo (IKP) ⚠️ **CRITICAL ANOMALY**

**Total Large Differences**: 403 records

**Breakdown**:
- POA: 193 records (47.9%)
- PR POA: 189 records (46.9%)
- Energy: 9 records (2.2%)
- PR GHI: 8 records (2.0%)
- GHI: 4 records (1.0%)

**CRITICAL ISSUE**:
- **PR POA = 8379.63%** (2025-09-15)
  - Excel: 0.0000
  - DB: 8379.6289
  - **Root Cause**: Division by zero atau calculation error
  - **Action**: **IMMEDIATE INVESTIGATION REQUIRED**

**Priority Dates untuk Investigation**:
- **2025-09-15**: PR POA = 8379.63% - **CRITICAL**
- Multiple dates: POA dan PR POA large differences

---

## Analysis by Metric Type

### Energy Large Differences

**Total**: 147 records

**Top Sites**:
1. PT. MMKI 5.7 MWp - Phase 2: 52 records
2. Shoetown Ligung Indonesia: 32 records
3. Charoen Pokphand Bandung: 21 records

**Patterns**:
- **Missing Data**: Banyak records dengan Energy = 0 di Excel atau DB
  - Shoetown Ligung: Multiple dates dengan DB = 0
  - Multiple sites: Excel = 0, DB has value
- **Calculation Differences**: MMKI II memiliki pattern Excel > DB (74% relative diff)
- **Dates**: February 2025 untuk MMKI II

**Priority for Investigation**:
- Shoetown Ligung: January 2025 (missing data di DB)
- MMKI II: February 2025 (calculation differences)

---

### GHI Large Differences

**Total**: 495 records

**Top Sites**:
1. PT. MMKI 1.75 MWp - Painting Building: 191 records
2. PT. MMKI 5.7 MWp - Phase 2: 187 records
3. PT. MMKI 4.292 MWP - Phase 3: 101 records

**Patterns**:
- **MMKI Sites**: Semua MMKI sites memiliki banyak GHI large differences
- **Max Difference**: 7.13 kWh/m² (MMKI I, 2025-07-06)
- **Root Cause**: Calculation method atau fallback logic berbeda

**Priority for Investigation**:
- MMKI I: July 2025 (max diff = 7.13 kWh/m²)
- MMKI II: Multiple dates
- MMKI III: Multiple dates

---

### POA Large Differences

**Total**: 1,393 records

**Top Sites**:
1. Shoetown Ligung Indonesia: 290 records
2. PT. Pusan Manis Mulia: 241 records
3. Garuda Metalindo (IKP): 193 records
4. PT. MMKI 1.75 MWp: 152 records
5. Garuda Metalindo 1: 134 records

**Patterns**:
- **Widespread Issue**: Banyak site memiliki POA large differences
- **Max Difference**: 4.81 kWh/m² (PLTS Rooftop Sumatera)
- **Root Cause**: Calculation method (weighted average) berbeda

**Priority for Investigation**:
- Shoetown Ligung: Multiple dates
- PT. Pusan Manis Mulia: Multiple dates
- Garuda Metalindo sites: Multiple dates

---

### PR GHI Large Differences

**Total**: 678 records

**Top Sites**:
1. PT. MMKI 5.7 MWp - Phase 2: 268 records
2. PT. MMKI 1.75 MWp - Painting Building: 169 records
3. PT. MMKI 4.292 MWP - Phase 3: 161 records

**Patterns**:
- **MMKI Sites**: Semua MMKI sites memiliki banyak PR GHI large differences
- **Max Difference**: 15.81% (MMKI II, 2025-04-22)
  - Excel: 0.77%
  - DB: 1581.57%
  - **Issue**: Calculation error atau missing reference value

**Priority for Investigation**:
- MMKI II: 2025-04-22 (PR GHI = 15.81%)
- MMKI I: Multiple dates
- MMKI III: Multiple dates

---

### PR POA Large Differences

**Total**: 1,403 records

**Top Sites**:
1. Shoetown Ligung Indonesia: 297 records
2. PT. MMKI 5.7 MWp - Phase 2: 273 records
3. PT. Pusan Manis Mulia: 212 records
4. Garuda Metalindo (IKP): 189 records
5. PT. MMKI 1.75 MWp: 128 records

**CRITICAL ANOMALY**:
- **PR POA = 8379.63%** (Garuda Metalindo IKP, 2025-09-15)
  - Excel: 0.0000
  - DB: 8379.6289
  - **Root Cause**: Division by zero atau calculation error

**Patterns**:
- **Widespread Issue**: Banyak site memiliki PR POA large differences
- **Max Differences**: 
  - Garuda Metalindo IKP: 8379.63% (CRITICAL)
  - MMKI I: 12.50%
  - Shoetown Ligung: 11.42%

**Priority for Investigation**:
- **Garuda Metalindo IKP: 2025-09-15** - **CRITICAL**
- MMKI I: 2025-09-14 (PR POA = 12.50%)
- Shoetown Ligung: Multiple dates

---

### Availability Large Differences

**Total**: 65 records

**Top Sites**:
1. PT. MMKI 1.75 MWp - Painting Building: 28 records
2. PLTS Rooftop Sumatera: 11 records
3. PLTS Frina Lestari: 8 records

**Patterns**:
- **Relatively Low**: Availability memiliki fewer large differences
- **Max Difference**: < 1.0 (10%)

**Priority**: Low (relatively minor issue)

---

## Date Patterns

### Missing Data Patterns

**Energy = 0 di DB** (Shoetown Ligung):
- 2025-01-02, 2025-01-03, 2025-01-04, 2025-01-05
- **Issue**: Missing data di database untuk early January 2025

**Energy = 0 di Excel** (Multiple sites):
- Multiple dates across different sites
- **Issue**: Missing data di Excel

### Calculation Difference Patterns

**MMKI II Energy** (February 2025):
- Pattern: Excel > DB (74% relative difference)
- Dates: 2025-02-12 to 2025-02-25
- **Issue**: Systematic calculation difference

**MMKI Sites GHI**:
- Pattern: Mixed (Excel > DB dan Excel < DB)
- Dates: Multiple dates across 2025
- **Issue**: Calculation method atau fallback logic berbeda

---

## Priority Actions

### 🔴 Critical (Immediate Action Required)

1. **Garuda Metalindo IKP - PR POA = 8379.63%** (2025-09-15)
   - **Action**: Investigate division by zero atau calculation error
   - **Priority**: **HIGHEST**

2. **Shoetown Ligung - Missing Energy Data** (January 2025)
   - **Action**: Check why energy data missing di DB for early January
   - **Priority**: **HIGH**

3. **MMKI II - PR GHI = 15.81%** (2025-04-22)
   - **Action**: Investigate calculation error atau missing reference value
   - **Priority**: **HIGH**

### 🟡 High Priority

4. **MMKI II - Energy Calculation Differences** (February 2025)
   - **Action**: Investigate meter selection atau aggregation method
   - **Priority**: **HIGH**

5. **POA Calculation Differences** (Multiple sites)
   - **Action**: Compare weighted average calculation method
   - **Priority**: **MEDIUM-HIGH**

6. **PR Calculation Differences** (Multiple sites)
   - **Action**: Compare PR calculation formulas
   - **Priority**: **MEDIUM-HIGH**

### 🟢 Medium Priority

7. **GHI Calculation Differences** (MMKI sites)
   - **Action**: Compare calculation method dan fallback logic
   - **Priority**: **MEDIUM**

8. **Missing Data** (Multiple sites)
   - **Action**: Update missing data di Excel atau DB
   - **Priority**: **MEDIUM**

---

## Recommendations

### Immediate Actions

1. **Fix Critical Anomaly**:
   - Investigate Garuda Metalindo IKP PR POA = 8379.63%
   - Check for division by zero in PR calculation
   - Verify reference values (capacity, target values)

2. **Fix Missing Data**:
   - Update Shoetown Ligung energy data for January 2025
   - Verify data ingestion process

3. **Investigate Calculation Differences**:
   - Compare PR calculation formulas between Excel and DB
   - Compare POA weighted average calculation
   - Compare GHI calculation method

### Long-term Actions

1. **Document Calculation Methods**:
   - Document Excel calculation formulas
   - Document Database calculation formulas
   - Create comparison matrix

2. **Standardize Calculations**:
   - Align calculation methods between Excel and DB
   - Update database calculations if needed
   - Update Excel calculations if needed

3. **Data Quality Improvements**:
   - Implement data validation rules
   - Add alerts for large differences
   - Regular comparison reports

---

## Query Usage

### To get all large differences:
```sql
\i dbt/analyses/compare_excel_vs_db_large_differences.sql
```

### To get summary per site:
```sql
\i dbt/analyses/compare_excel_vs_db_large_differences_summary.sql
```

### To filter by site:
```sql
-- Add WHERE clause
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
```

### To filter by metric:
```sql
-- Add WHERE clause
WHERE metric_type = 'ENERGY'
```

### To filter by date range:
```sql
-- Add WHERE clause
WHERE date_key >= '2025-01-01' AND date_key <= '2025-03-31'
```

---

**Report Generated**: 2025-01-XX  
**Total Large Differences**: 3,828 records  
**Critical Anomalies**: 1 (PR POA = 8379.63%)  
**Sites Requiring Immediate Attention**: 3 (MMKI II, Shoetown Ligung, Garuda Metalindo IKP)

