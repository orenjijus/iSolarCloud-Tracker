# Fact Tables Documentation Index

## Overview

Dokumentasi lengkap untuk 5-Minute Calculated Metrics Fact Tables yang digunakan untuk availability calculation dengan tracking detail.

**Status**: ✅ **FULLY IMPLEMENTED**

**Quick Links**:
- 🚀 [Quick Start](#quick-start-guide)
- 📊 [Fact Tables Overview](#fact-tables)
- ✅ [Validation](#validation--testing)
- 📚 [Documentation](#main-documentation)

---

## Main Documentation

### 0. [FACT_TABLES_OVERVIEW.md](./FACT_TABLES_OVERVIEW.md) ⭐ START HERE
**Purpose**: Overview lengkap (executive + implementation + validation summary)

**Contents**:
- Executive summary
- Fact tables overview
- Implementation summary
- Validation summary
- Quick links ke design, quick reference, Excel crosscheck

**Best for**: Getting complete overview dan quick start

---

### 1. [FACT_5MIN_CALCULATED_METRICS_DESIGN.md](./FACT_5MIN_CALCULATED_METRICS_DESIGN.md)
**Purpose**: Design document lengkap dengan requirements, structure, dan implementation plan

**Contents**:
- Calculated metrics requirements
- Override logic (POA override, GHI fallback)
- Fact table structures
- Daily aggregation logic
- Implementation order
- Benefits of separate fact tables
- TODO list

**Best for**: Understanding the overall design and architecture

---

### 2. [FACT_TABLES_IMPLEMENTATION_SUMMARY.md](./FACT_TABLES_IMPLEMENTATION_SUMMARY.md)
**Purpose**: Summary implementasi dengan status dan next steps

**Contents**:
- Overview fact tables
- POA fallback untuk MIT
- Cara menggunakan
- Validation queries
- Status implementasi
- Next steps

**Best for**: Quick overview of what was implemented

---

### 3. [FACT_TABLES_QUICK_REFERENCE.md](./FACT_TABLES_QUICK_REFERENCE.md)
**Purpose**: Quick reference guide untuk common queries dan commands

**Contents**:
- Quick overview table
- MIT fallback priority
- Common use cases dengan SQL examples
- Build commands
- Key columns reference
- Calculation formulas
- Troubleshooting

**Best for**: Daily reference when working with fact tables

---

### 4. [models/facts/README.md](../models/facts/README.md)
**Purpose**: Detailed technical documentation untuk fact tables

**Contents**:
- Detailed schema untuk setiap fact table
- Dependencies
- Indexes
- Calculation logic
- Data flow
- Maintenance guide
- Related documentation

**Best for**: Technical deep dive dan maintenance

---

## Validation & Testing

### 1. [FACT_TABLES_VALIDATION_RESULTS.md](./FACT_TABLES_VALIDATION_RESULTS.md)
**Purpose**: Validation checklist dan expected results

**Contents**:
- Validation checklist dengan status
- Expected results untuk setiap validation
- How to run validation
- Crosscheck steps dengan Excel

**Best for**: Understanding what to validate dan expected results

---

### 2. [FACT_TABLES_VALIDATION_ANALYSIS.md](./FACT_TABLES_VALIDATION_ANALYSIS.md)
**Purpose**: Detailed validation analysis dengan hasil actual

**Contents**:
- Validation results untuk semua 7 queries
- Analysis per site
- Sites dengan availability < 100%
- POA fallback usage
- GHI fallback usage
- Inverter availability detail

**Best for**: Understanding actual validation results dan data quality

---

### 3. Excel Crosscheck Documentation

#### [EXCEL_CROSSCHECK_GUIDE.md](./EXCEL_CROSSCHECK_GUIDE.md)
**Purpose**: Step-by-step guide untuk crosscheck dengan Excel

**Contents**:
- Export data dari database
- Export data dari Excel
- Comparison process
- Common issues to check
- Troubleshooting

**Best for**: Step-by-step crosscheck process

---

#### [EXCEL_CROSSCHECK_RESULTS.md](./EXCEL_CROSSCHECK_RESULTS.md)
**Purpose**: Data ready for Excel comparison

**Contents**:
- Daily availability summary (recent 7 days)
- Sites dengan availability < 100%
- Inverter availability detail
- Comparison template
- Key points untuk crosscheck

**Best for**: Quick reference untuk data yang perlu dibandingkan dengan Excel

---

#### [EXCEL_CROSSCHECK_COMPARISON_TABLE.md](./EXCEL_CROSSCHECK_COMPARISON_TABLE.md)
**Purpose**: Comparison table template

**Contents**:
- Comparison table template
- Query untuk export
- Sites dengan availability < 100%
- What to check in Excel

**Best for**: Side-by-side comparison dengan Excel

---

#### [analyses/crosscheck_with_excel.sql](../analyses/crosscheck_with_excel.sql)
**Purpose**: SQL queries untuk crosscheck dengan Excel

**Queries**:
1. Daily availability summary (export ready)
2. Detailed 5-minute data untuk sites < 100%
3. Inverter-level detail
4. MIT calculation detail
5. Export untuk Excel comparison (CSV format ready)

**Best for**: Running queries untuk export data ke Excel

---

#### [analyses/compare_with_excel.sql](../analyses/compare_with_excel.sql) ⭐ NEW
**Purpose**: SQL queries untuk compare fact tables dengan Excel data di schema public

**Queries**:
1. Compare MMKI Sites
2. Compare Other Sites
3. Summary Comparison
4. Detailed comparison for specific dates

**Best for**: Direct comparison dengan Excel data yang sudah ada di database

---

#### [EXCEL_COMPARISON_RESULTS.md](./EXCEL_COMPARISON_RESULTS.md) ⭐ NEW
**Purpose**: Detailed comparison results antara Excel dan Database

**Contents**:
- Comparison summary statistics
- Dates dengan perbedaan signifikan
- Analysis why database lebih akurat
- Recommendations

**Best for**: Understanding perbedaan antara Excel dan Database

---

#### [EXCEL_COMPARISON_SUMMARY.md](./EXCEL_COMPARISON_SUMMARY.md) ⭐ NEW
**Purpose**: Quick summary comparison results

**Contents**:
- Overall statistics
- Key findings
- Main issues
- Conclusion dan recommendations

**Best for**: Quick overview comparison results

---

### 4. [analyses/validate_fact_tables.sql](../analyses/validate_fact_tables.sql)
**Purpose**: 7 comprehensive validation queries

**Queries**:
1. Check MIT calculation dan fallback sources
2. Check POA fallback usage
3. Check inverter availability per site
4. Check daily availability summary
5. Check which inverters are down
6. Compare MIT sources across sites
7. Check GHI fallback sites (MMKI II, III)

**Best for**: Comprehensive validation setelah build

---

### 5. [analyses/quick_validation.sql](../analyses/quick_validation.sql)
**Purpose**: 7 quick validation queries

**Queries**:
1. Row counts per fact table
2. MIT sources distribution
3. POA fallback usage (recent 7 days)
4. GHI fallback sites check
5. Inverter availability summary
6. Site-level availability
7. Daily availability summary

**Best for**: Quick checks dan monitoring

---

## Fact Tables

### 1. `fact_sensor_calculations_5min`
- **Grain**: `timestamp × sensor_id`
- **Purpose**: Sensor-level metrics dengan MIT calculation
- **Key Features**: GHI fallback, POA fallback untuk MIT
- **Key Columns**: `mit`, `mit_irradiance_source`, `is_fallback`

### 2. `fact_inverter_calculations_5min`
- **Grain**: `timestamp × inverter_id`
- **Purpose**: Inverter-level availability
- **Key Features**: Availability calculation dengan MIT
- **Key Columns**: `inverter_availability`, `inverter_availability_with_mit`

### 3. `fact_site_calculations_5min`
- **Grain**: `timestamp × site_id`
- **Purpose**: Site-level aggregation
- **Key Features**: Power available ratio, unavailability ratio
- **Key Columns**: `power_available_ratio`, `unavailability_ratio`

---

## Quick Start Guide

### 1. Build Fact Tables
```bash
cd dbt
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min
```

### 2. Validate
```sql
-- Run queries from analyses/validate_fact_tables.sql
-- Or use quick_validation.sql for quick checks
```

### 3. Query Data
```sql
-- See FACT_TABLES_QUICK_REFERENCE.md for common queries
-- Or see models/facts/README.md for detailed examples
```

---

## Key Concepts

### MIT Fallback Priority
1. GHI (pyranometer) dari site yang sama
2. POA dari site yang sama (fallback)
3. GHI dari site lain (configured fallback)

### Availability Calculation
- Inverter level: `CASE WHEN active_power > 0 THEN 1 ELSE 0 END`
- Site level: `available_inverters / total_inverters`
- Daily: `SUM(power_available_ratio) / 12.0` hours

### Data Flow
```
mart_sensor_measurements_5min
    ↓
fact_sensor_calculations_5min (GHI/POA → MIT)
    ↓
mart_inverter_performance_5min
    ↓
fact_inverter_calculations_5min (active_power + MIT → availability)
    ↓
fact_site_calculations_5min (aggregate → ratios)
    ↓
mart_site_performance_daily (daily aggregation)
```

---

## Related Documentation

### Architecture
- [SITE_PERFORMANCE_ARCHITECTURE.md](./SITE_PERFORMANCE_ARCHITECTURE.md)
- [MART_DESIGN_DECISION.md](./MART_DESIGN_DECISION.md)

### Implementation
- [SITE_PERFORMANCE_DAILY_IMPLEMENTATION.md](./SITE_PERFORMANCE_DAILY_IMPLEMENTATION.md)
- [SITE_PERFORMANCE_IMPLEMENTATION_PLAN.md](./SITE_PERFORMANCE_IMPLEMENTATION_PLAN.md)

### Data Quality
- [DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md](./DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md)

---

## Support

Untuk pertanyaan atau issues:
1. Check [FACT_TABLES_QUICK_REFERENCE.md](./FACT_TABLES_QUICK_REFERENCE.md) troubleshooting section
2. Review validation queries di `analyses/validate_fact_tables.sql`
3. Check design document [FACT_5MIN_CALCULATED_METRICS_DESIGN.md](./FACT_5MIN_CALCULATED_METRICS_DESIGN.md)

