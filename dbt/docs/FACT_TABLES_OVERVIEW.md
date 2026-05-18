# Fact Tables — Overview (Executive + Implementation + Validation)

Ringkasan lengkap fact tables 5 menit: executive summary, implementasi, dan hasil validasi. Digabung dari: FACT_TABLES_COMPLETE_SUMMARY, FACT_TABLES_IMPLEMENTATION_SUMMARY, FACT_TABLES_VALIDATION_SUMMARY.

---

## 1. Executive Summary

**Status**: ✅ **FULLY IMPLEMENTED**

Tiga fact tables 5 menit untuk availability dengan tracking detail: inverter mana yang mati per 5 menit, validasi availability dengan detail per inverter, fallback logic MIT (GHI → POA → GHI_FALLBACK).

**Build Results** (contoh):
- `fact_sensor_calculations_5min`: ~5.8M rows
- `fact_inverter_calculations_5min`: ~8.6M rows
- `fact_site_calculations_5min`: ~964K rows

**Build**: `dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min`

---

## 2. Fact Tables Overview

| Fact Table | Grain | Purpose | Key Columns |
|------------|-------|---------|--------------|
| **fact_sensor_calculations_5min** | timestamp × sensor_id | Sensor-level MIT calculation | mit, mit_irradiance_source, is_fallback |
| **fact_inverter_calculations_5min** | timestamp × inverter_id | Inverter-level availability | inverter_availability, inverter_availability_with_mit |
| **fact_site_calculations_5min** | timestamp × site_id | Site-level aggregation | power_available_ratio, unavailability_ratio |

**MIT Fallback Priority**: 1) GHI dari site yang sama, 2) POA dari site yang sama (fallback), 3) GHI dari site lain (GHI_FALLBACK). Kolom `mit_irradiance_source`: 'GHI', 'POA', 'GHI_FALLBACK'.

---

## 3. Implementation Summary

- **Dependencies**: mart_sensor_measurements_5min, mart_inverter_performance_5min, seed_sensor_config, seed_sensor_site_mapping, dim_assets.
- **Validation**: analyses/validate_fact_tables.sql, analyses/quick_validation.sql, analyses/crosscheck_with_excel.sql.
- **Docs**: FACT_5MIN_CALCULATED_METRICS_DESIGN, FACT_TABLES_QUICK_REFERENCE, FACT_TABLES_VALIDATION_RESULTS, EXCEL_CROSSCHECK_GUIDE, FACT_TABLES_DOCUMENTATION_INDEX.

---

## 4. Validation Summary

**Status**: ✅ VALIDATION PASSED (contoh run 2025-11-17)

| Metric | Result |
|--------|--------|
| Total Rows | 15.4M+ rows |
| Date Range | Apr 2024 - Nov 2025 |
| Sites | 16-17 sites |
| POA Fallback | 15.73% usage, 15 sites |
| GHI Fallback | 2.62% usage, 2 sites (MMKI II/III) |
| Inverter Tracking | Working (identify down inverters per 5 min) |
| Daily Availability | Calculated correctly |

**Key Findings**: POA fallback aktif di 15 sites; GHI fallback hanya MMKI II/III (GHI dari MMKI I); inverter down dapat diidentifikasi per 5 menit; daily availability siap untuk crosscheck dengan Excel.

**Next Steps**: Crosscheck dengan Excel untuk site < 100%; lihat FACT_TABLES_VALIDATION_ANALYSIS untuk analisis detail.

---

## 5. Quick Links

- **Quick reference**: [FACT_TABLES_QUICK_REFERENCE.md](./FACT_TABLES_QUICK_REFERENCE.md)
- **Design**: [FACT_5MIN_CALCULATED_METRICS_DESIGN.md](./FACT_5MIN_CALCULATED_METRICS_DESIGN.md)
- **Documentation index**: [FACT_TABLES_DOCUMENTATION_INDEX.md](./FACT_TABLES_DOCUMENTATION_INDEX.md)
- **Validation results**: [FACT_TABLES_VALIDATION_RESULTS.md](./FACT_TABLES_VALIDATION_RESULTS.md)
- **Excel crosscheck (panduan & hasil)**: [reports/excel-crosscheck/README.md](../../reports/excel-crosscheck/README.md)

---

*Dokumen master overview fact tables. File asli: FACT_TABLES_COMPLETE_SUMMARY, FACT_TABLES_IMPLEMENTATION_SUMMARY, FACT_TABLES_VALIDATION_SUMMARY — diarsipkan di dbt/docs/archive/.*
