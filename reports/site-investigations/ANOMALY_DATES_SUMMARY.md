# Summary: Anomaly Dates per Site untuk Investigation

**Tanggal Analisis**: 2025-01-XX  
**Tujuan**: Daftar tanggal-tanggal dengan large differences yang perlu diinvestigasi

---

## 🔴 CRITICAL - Immediate Action Required

### 1. Garuda Metalindo (IKP) - PR POA = 8379.63%
**Date**: **2025-09-15**  
**Issue**: PR POA calculation error (division by zero atau missing reference value)  
**Action**: **IMMEDIATE INVESTIGATION**

**Top Dates untuk Investigation**:
- **2025-09-15**: PR POA = 8379.63% (CRITICAL)
- 2025-09-26: PR POA = 2.14%
- 2025-09-29: PR POA = 0.85%
- 2025-10-30: GHI = 4.74 kWh/m²
- 2025-09-29: GHI = 3.48 kWh/m², POA = 3.28 kWh/m²

---

## 🟡 HIGH PRIORITY - Sites dengan Most Issues

### 2. PT. MMKI 5.7 MWp - Phase 2 (797 large differences)

#### Energy Issues (February 2025)
**Top Dates**:
- **2025-02-13**: Energy diff = 20.49 MWh (Excel: 27.11, DB: 6.62)
- **2025-02-21**: Energy diff = 18.45 MWh (Excel: 24.82, DB: 6.36)
- **2025-02-20**: Energy diff = 16.51 MWh (Excel: 22.33, DB: 5.82)
- **2025-02-12**: Energy diff = 16.23 MWh (Excel: 21.70, DB: 5.47)
- **2025-02-25**: Energy diff = 15.59 MWh (Excel: 20.85, DB: 5.26)
- **2025-02-14**: Energy diff = 14.20 MWh (Excel: 18.97, DB: 4.78)
- **2025-03-12**: Energy diff = 13.85 MWh (Excel: 18.55, DB: 4.70)
- **2025-02-19**: Energy diff = 13.76 MWh (Excel: 18.59, DB: 4.83)
- **2025-02-24**: Energy diff = 11.75 MWh (Excel: 15.81, DB: 4.07)
- **2025-02-18**: Energy diff = 11.70 MWh (Excel: 15.72, DB: 4.02)

**Pattern**: Excel > DB (74% relative difference) - Systematic calculation difference

#### PR GHI Issues
**Top Dates**:
- **2025-04-22**: PR GHI diff = 15.81% (Excel: 0.77%, DB: 1581.57%) - **CRITICAL**
- 2025-05-06: PR GHI diff = 3.96%
- 2025-04-18: PR GHI diff = 3.83%
- 2025-06-27: PR GHI diff = 1.59%
- 2025-09-09: PR GHI diff = 1.16%

#### PR POA Issues
**Top Dates**:
- **2025-09-13**: PR POA diff = 9.79% (Excel: 0%, DB: 979.46%)
- 2025-09-20: PR POA diff = 3.31%
- 2025-09-11: PR POA diff = 1.08%
- 2025-06-01: PR POA diff = 1.07%

#### GHI Issues
**Top Dates**:
- 2025-07-06: GHI diff = 6.92 kWh/m²
- 2025-10-23: GHI diff = 6.78 kWh/m²
- 2025-08-08: GHI diff = 6.45 kWh/m²
- 2025-04-29: GHI diff = 5.78 kWh/m²

---

### 3. PT. MMKI 1.75 MWp - Painting Building (669 large differences)

#### GHI Issues
**Top Dates**:
- **2025-07-06**: GHI diff = 7.13 kWh/m² (Excel: 4.99, DB: 12.12)
- **2025-10-23**: GHI diff = 6.78 kWh/m²
- **2025-08-08**: GHI diff = 6.52 kWh/m²
- **2025-08-22**: GHI diff = 6.02 kWh/m²
- **2025-04-30**: GHI diff = 5.58 kWh/m²
- **2025-05-01**: GHI diff = 5.35 kWh/m²
- **2025-04-23**: GHI diff = 5.29 kWh/m²
- **2025-04-27**: GHI diff = 5.26 kWh/m²
- **2025-04-28**: GHI diff = 5.26 kWh/m²
- **2025-04-25**: GHI diff = 5.19 kWh/m²

**Pattern**: April-May 2025 dan July-August 2025

#### PR POA Issues
**Top Dates**:
- **2025-09-14**: PR POA diff = 12.50% (Excel: 7.84%, DB: 20.34%)
- 2025-09-13: PR POA diff = 0.46%
- 2025-04-29: PR POA diff = 0.18%

#### POA Issues
**Top Dates**:
- 2025-04-29: POA diff = 1.10 kWh/m²
- 2025-09-13: POA diff = 0.98 kWh/m²
- 2025-04-30: POA diff = 0.64 kWh/m²

---

### 4. Shoetown Ligung Indonesia (656 large differences)

#### Energy Missing Data (January 2025) - **CRITICAL**
**Top Dates**:
- **2025-01-04**: Energy diff = 57.17 MWh (Excel: 57.17, DB: 0) - **MISSING DATA**
- **2025-01-03**: Energy diff = 54.50 MWh (Excel: 54.50, DB: 0) - **MISSING DATA**
- **2025-01-05**: Energy diff = 46.30 MWh (Excel: 46.30, DB: 0) - **MISSING DATA**
- **2025-01-02**: Energy diff = 30.85 MWh (Excel: 30.85, DB: 0) - **MISSING DATA**
- **2025-03-06**: Energy diff = 21.92 MWh (Excel: 13.41, DB: 35.33)
- **2025-01-24**: Energy diff = 12.39 MWh (Excel: 0, DB: 12.39)
- **2025-01-25**: Energy diff = 12.13 MWh (Excel: 0, DB: 12.13)
- **2025-01-19**: Energy diff = 11.34 MWh (Excel: 0, DB: 11.34)
- **2025-01-12**: Energy diff = 10.93 MWh (Excel: 0, DB: 10.93)
- **2025-01-22**: Energy diff = 10.76 MWh (Excel: 0, DB: 10.76)

**Pattern**: Missing data di DB untuk early January 2025

#### PR POA Issues
**Top Dates**:
- **2025-01-05**: PR POA diff = 11.42% (Excel: 11.42%, DB: 0%)
- **2025-01-19**: PR POA diff = 8.20% (Excel: 0%, DB: 8.20%)
- **2025-01-29**: PR POA diff = 7.87% (Excel: 0%, DB: 7.87%)
- 2025-10-03: PR POA diff = 5.54%
- 2025-01-04: PR POA diff = 4.91%

#### POA Issues
**Top Dates**:
- 2025-03-02: POA diff = 3.81 kWh/m²
- 2025-10-03: POA diff = 3.54 kWh/m²
- 2025-02-22: POA diff = 3.32 kWh/m²
- 2025-05-12: POA diff = 3.13 kWh/m²

---

## 📋 Quick Reference: Date Ranges dengan Most Issues

### Energy Issues
- **MMKI II**: February 2025 (2025-02-12 to 2025-02-25)
- **Shoetown Ligung**: January 2025 (2025-01-02 to 2025-01-05) - Missing data

### GHI Issues
- **MMKI I**: April-May 2025, July-August 2025
- **MMKI II**: April-May 2025, July-August 2025, October 2025

### POA Issues
- **Shoetown Ligung**: February-March 2025, May 2025, September-October 2025
- **PT. Pusan Manis Mulia**: Multiple dates (241 records)
- **Garuda Metalindo IKP**: May 2025, July 2025, September 2025

### PR Issues
- **MMKI II**: April 2025 (PR GHI), September 2025 (PR POA)
- **MMKI I**: September 2025 (PR POA)
- **Shoetown Ligung**: January 2025 (PR POA)
- **Garuda Metalindo IKP**: September 2025 (PR POA) - **CRITICAL**

---

## 🎯 Priority Investigation List

### Week 1 (Immediate)
1. **Garuda Metalindo IKP - 2025-09-15**: PR POA = 8379.63%
2. **Shoetown Ligung - January 2025**: Missing energy data di DB
3. **MMKI II - 2025-04-22**: PR GHI = 15.81%

### Week 2 (High Priority)
4. **MMKI II - February 2025**: Energy calculation differences (systematic)
5. **MMKI I - April-May 2025**: GHI calculation differences
6. **MMKI I - 2025-09-14**: PR POA = 12.50%

### Week 3 (Medium Priority)
7. **Shoetown Ligung - Multiple dates**: POA calculation differences
8. **MMKI II - September 2025**: PR POA issues
9. **Multiple sites**: POA weighted average calculation

---

## 📊 Summary Statistics

| Site | Total Anomalies | Critical Dates | Priority |
|------|----------------|----------------|----------|
| PT. MMKI 5.7 MWp - Phase 2 | 797 | 2025-02-13, 2025-04-22 | HIGH |
| PT. MMKI 1.75 MWp - Painting Building | 669 | 2025-07-06, 2025-09-14 | HIGH |
| Shoetown Ligung Indonesia | 656 | 2025-01-02 to 2025-01-05 | HIGH |
| Garuda Metalindo (IKP) | 403 | **2025-09-15** | **CRITICAL** |
| PT. Pusan Manis Mulia | 461 | Multiple dates | MEDIUM |

---

## 🔍 How to Use This Report

1. **For Critical Issues**: Start with dates marked as **CRITICAL**
2. **For Systematic Issues**: Focus on date ranges (e.g., February 2025 for MMKI II Energy)
3. **For Missing Data**: Check dates where Excel or DB value = 0
4. **For Calculation Differences**: Compare calculation formulas for dates with large differences

---

**Report Generated**: 2025-01-XX  
**Next Review**: After investigation of critical dates

