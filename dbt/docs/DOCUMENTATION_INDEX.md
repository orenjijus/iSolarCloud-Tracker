# MMSR dbt Project - Documentation Index

## 📚 Documentation Overview

This repository contains comprehensive documentation for the MMSR Solar Data dbt project. Use this index to find the right document for your needs.

---

## 🎯 Getting Started

### For New Users
1. **Start Here**: [`README_dbt.md`](../README_dbt.md) - Project overview and quick start guide
2. **Architecture**: [`DBT_TRANSFORMATION_SUMMARY.md`](../DBT_TRANSFORMATION_SUMMARY.md) - High-level architecture explanation

### For Developers
- [`RUN_DBT.md`](../RUN_DBT.md) - How to run dbt transformations
- [`dbt_project.yml`](../dbt_project.yml) - Configuration file

---

## 📊 Data Architecture

### Database Design
- [`docs/database-design.md`](database-design.md) - Complete database schema design
- [`docs/Existing-context/Existing_DATABASE.md`](Existing-context/Existing_DATABASE.md) - Existing database structure
- [`docs/Existing-context/Existing_ERD.md`](Existing-context/Existing_ERD.md) - Entity relationship diagrams

### Migration & Setup
- [`DBT_TRANSFORMATION_SUMMARY.md`](../DBT_TRANSFORMATION_SUMMARY.md) - Migration strategy
- [`docs/DBT_MIGRATION_GUIDE.md`](DBT_MIGRATION_GUIDE.md) - Step-by-step migration guide
- [`RUN_DBT.md`](../RUN_DBT.md) - Operational guide

---

## 🔑 Key Topics

### 5-Minute Calculated Metrics Fact Tables ⭐ NEW
**Quick Reference** → [`FACT_TABLES_QUICK_REFERENCE.md`](FACT_TABLES_QUICK_REFERENCE.md)
- Quick overview dan common queries
- Build commands
- Calculation formulas
- Troubleshooting

**Implementation Summary** → [`FACT_TABLES_IMPLEMENTATION_SUMMARY.md`](FACT_TABLES_IMPLEMENTATION_SUMMARY.md)
- Status implementasi
- Fact tables overview
- Validation queries

**Design Document** → [`FACT_5MIN_CALCULATED_METRICS_DESIGN.md`](FACT_5MIN_CALCULATED_METRICS_DESIGN.md)
- Complete design dengan requirements
- Override logic (POA override, GHI fallback)
- Fact table structures

**Technical Documentation** → [`models/facts/README.md`](../models/facts/README.md)
- Detailed schema
- Dependencies
- Maintenance guide

**Documentation Index** → [`FACT_TABLES_DOCUMENTATION_INDEX.md`](FACT_TABLES_DOCUMENTATION_INDEX.md)
- Complete index untuk semua fact tables documentation

**Validation Results** → [`FACT_TABLES_VALIDATION_RESULTS.md`](FACT_TABLES_VALIDATION_RESULTS.md)
- Validation checklist dan expected results
- How to run validation queries
- Crosscheck steps dengan Excel

**TL;DR**: Fact tables untuk availability calculation dengan tracking detail per 5 menit, termasuk POA fallback untuk MIT

### Foreign Keys & Relationships
**Quick Answers** → [`FK_IN_DBT_QUICK_REFERENCE.md`](FK_IN_DBT_QUICK_REFERENCE.md)
- One-page reference for quick arguments
- Common Q&A
- Elevator pitch (30 seconds)

**Full Analysis** → [`FOREIGN_KEYS_IN_DBT_ANALYSIS.md`](FOREIGN_KEYS_IN_DBT_ANALYSIS.md)
- Complete technical explanation
- Industry best practices
- Performance comparisons
- Decision matrix
- Implementation details

**TL;DR**: Raw tables have FK, mart tables don't (by design, industry standard)

### Architecture Decisions
- **[`architecture/README.md`](architecture/README.md)** - Ringkasan arsitektur dbt (incremental, staging, mart)
- **[`architecture/ARCHITECTURE_DECISIONS.md`](architecture/ARCHITECTURE_DECISIONS.md)** - Keputusan final, materialisasi, performa, workflow
- [`docs/architecture.md`](../../docs/architecture.md) - System architecture (high-level)
- [`Existing-context/Ideas_Database_PowerBI.md`](../../docs/Existing-context/Ideas_Database_PowerBI.md) - PowerBI integration ideas

---

## 🛠️ Operations

### Configuration
- [`HYPSCALE_CONFIGURATION.md`](../HYPSCALE_CONFIGURATION.md) - Hyperscale settings
- [`STORAGE_OPTIMIZATION_STRATEGY.md`](../STORAGE_OPTIMIZATION_STRATEGY.md) - Storage optimization
- [`profiles.yml`](../profiles.yml) - Database connection profiles

### Permissions & Security
- [`GRANT_PERMISSIONS.sql`](../GRANT_PERMISSIONS.sql) - Permission setup
- [`GET_PERMISSIONS.md`](../GET_PERMISSIONS.md) - Permission management

### Troubleshooting
- **[`FUSIONSOLAR_TROUBLESHOOTING.md`](FUSIONSOLAR_TROUBLESHOOTING.md)** - FusionSolar tidak update / tidak sampai mart (master)
- [`FIX_SENSOR_METER_DATA_ISSUE.md`](FIX_SENSOR_METER_DATA_ISSUE.md) - Specific issue resolution
- **dim_assets**: [`DIM_ASSETS.md`](DIM_ASSETS.md) - Memastikan dim_assets ter-refresh

---

## 📈 Analysis Documents

### Kondisi Data (Staging → Mart) untuk Report
- **[`DATA_CONDITION_REPORT.md`](DATA_CONDITION_REPORT.md)** - Kerangka analisis kondisi data dari staging hingga mart; template report dan cara interpretasi
- **Query analisis:** `queries/analysis_data_condition_staging_to_mart.sql` - Jalankan per blok untuk snapshot volume, coverage, dan kualitas data

### Staging & Views Analysis
- [`ANALISIS_STAGING_VIEW_VS_INCREMENTAL.md` Phylogenetic tree](../ANALISIS_STAGING_VIEW_VS_INCREMENTAL.md) - Staging vs incremental comparison
- [`ANALISIS_REVISI_STAGING_VIEW.md`](../ANALISIS_REVISI_STAGING_VIEW.md) - Staging view revisions
- [`ANALISIS_MART_INCREMENTAL_FINAL.md`](../ANALISIS_MART_INCREMENTAL_FINAL.md) - Mart incremental analysis

### Insights
- `analyses/` directory - Various analysis documents

---

## 🎨 Business Intelligence

### Excel vs DB Crosscheck (master di reports)
- **[`reports/excel-crosscheck/README.md`](../../reports/excel-crosscheck/README.md)** - **Master** panduan + hasil crosscheck Database vs Excel (daily, monthly, toleransi)
- [`EXCEL_CROSSCHECK_GUIDE.md`](EXCEL_CROSSCHECK_GUIDE.md) - Crosscheck availability dari fact tables (sisi dbt)

### PowerBI
- [`docs/POWER_BI_IMPLEMENTATION_PLAN.md`](../../docs/POWER_BI_IMPLEMENTATION_PLAN.md) - **Complete Power BI implementation plan** ⭐
- [`docs/POWER_BI_QUICK_REFERENCE.md`](../../docs/POWER_BI_QUICK_REFERENCE.md) - Quick reference guide
- [`docs/CALCULATION_FLOW_5MIN_TO_YTD.md`](CALCULATION_FLOW_5MIN_TO_YTD.md) - **Calculation methodology from 5-min to YTD** ⭐
- [`docs/METER_POLARITY_DETECTION.md`](METER_POLARITY_DETECTION.md) - **Meter polarity detection and correction** ⚠️
- [`docs/mmsr-dashboard-prd.md`](../../docs/mmsr-dashboard-prd.md) - PowerBI dashboard PRD
- [`docs/weekly-log/README.md`](../../docs/weekly-log/README.md) - Weekly log (Power BI)
- [`docs/Existing-context/Ideas_Database_PowerBI.md`](../../docs/Existing-context/Ideas_Database_PowerBI.md) - PowerBI ideas

---

## 📁 Project Context

### Existing System
- [`docs/Existing-context/Existing_API_TO_DB_RUNBOOK.md`](Existing-context/Existing_API_TO_DB_RUNBOOK.md) - API to DB process
- [`docs/Existing-context/Existing_Codegraph.md`](Existing-context/Existing_Codegraph.md) - Code structure
- [`docs/Existing-context/Existing_DATABASE.md`](Existing-context/Existing_DATABASE.md) - Database structure
- [`docs/Existing-context/Existing_ERD.md`](Existing-context/Existing_ERD.md) - ERD
- [`docs/Existing-context/Ideas_Database_PowerBI.md`](Existing-context/Ideas_Database_PowerBI.md) - PowerBI ideas

### Agent Workflows
- `docs/Agent-ideas/` - Various agent workflow definitions

---

## 🔍 Quick Navigation by Topic

### "I need to understand..."
| Topic | Document |
|-------|----------|
| **Fact tables for availability** | `docs/FACT_TABLES_QUICK_REFERENCE.md` |
| **Fact tables design** | `docs/FACT_5MIN_CALCULATED_METRICS_DESIGN.md` |
| **Why no FK in mart tables?** | `docs/FK_IN_DBT_QUICK_REFERENCE.md` |
| **Complete distribution explanation** | `docs/FOREIGN_KEYS_IN_DBT_ANALYSIS.md` |
| **How to run dbt?** | `RUN_DBT.md` |
| **Project overview** | `README_dbt.md` |
| **Database schema** | `docs/database-design.md` |
| **PowerBI setup** | `docs/POWER_BI_IMPLEMENTATION_PLAN.md` |
| **PowerBI quick reference** | `docs/POWER_BI_QUICK_REFERENCE.md` |
| **Calculation methodology** | `docs/CALCULATION_FLOW_5MIN_TO_YTD.md` |
| **Meter polarity detection** | `docs/METER_POLARITY_DETECTION.md` |
| **Troubleshooting** | `TROUBLESHOOTING.md` |
| **Permissions** | `GET_PERMISSIONS.md` |
| **Kondisi data staging→mart (report)** | `docs/DATA_CONDITION_REPORT.md` |
| **Cara query data dari staging** | `docs/QUERYING_STAGING_DATA_GUIDE.md` |

### "I need to argue/explain..."
| Situation | Document |
|-----------|----------|
| **"Why no ERD/FK?"** | `docs/FK_IN_DBT_QUICK_REFERENCE.md` (Q&A section) |
| **Technical deep-dive** | `docs/FOREIGN_KEYS_IN_DBT_ANALYSIS.md` (complete analysis) |
| **Architecture decisions** | `FINAL_ARCHITECTURE_SUMMARY.md` |
| **Counter-arguments** | `COUNTER_ARGUMENT_ARCHITECT.md` |

---

## 📝 Document Categories

### 🎓 Educational
- `FOREIGN_KEYS_IN_DBT_ANALYSIS.md` - Learn about FK in dbt
- `DBT_TRANSFORMATION_SUMMARY.md` - Understand transformations
- `docs/DBT_MIGRATION_GUIDE.md` - Migration concepts

### 🚀 Operational
- `RUN_DBT.md` - Run transformations
- `TROUBLESHOOTING.md` - Fix issues
- `GET_PERMISSIONS.md` - Manage permissions

### 💼 Business/Reporting
- `docs/POWER_BI_IMPLEMENTATION_PLAN.md` - **Complete Power BI implementation plan** ⭐
- `docs/POWER_BI_QUICK_REFERENCE.md` - Quick reference guide
- `docs/CALCULATION_FLOW_5MIN_TO_YTD.md` - **Calculation methodology** ⭐
- `docs/METER_POLARITY_DETECTION.md` - **Meter polarity detection and correction** ⚠️
- `docs/mmsr-dashboard-prd.md` - PowerBI PRD
- `docs/Ideas_Database_PowerBI.md` - PowerBI ideas

### 🎯 Quick Reference
- `FK_IN_DBT_QUICK_REFERENCE.md` - 1-page FK reference
- `README_dbt.md` - Project quick start

---

## 🌐 External Resources

### dbt Documentation
- [dbt Official Docs](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [dbt Discourse](https://discourse.getdbt.com/)

### Related Documentation Files
- `SmartPVMS 25.1.0 Northbound API Reference_Lite.pdf` - API reference
- `iSolarCloud_Documentation_lit.pdf` - iSolarCloud docs

---

## 📊 Documentation Stats

| Category | Count |
|----------|-------|
| **Architecture Docs** | 8 |
| **Operations Guides** | 5 |
| **Analysis Documents** | 6 |
| **Quick References** | 2 |
| **External References** | 2 |

**Total**: ~25 documentation files

---

## 🆘 Need Help?

1. **Quick question?** → Check `FK_IN_DBT_QUICK_REFERENCE.md`
2. **Technical deep-dive?** → Check `FOREIGN_KEYS_IN_DBT_ANALYSIS.md`
3. **How to run?** → Check `RUN_DBT.md`
4. **Something broken?** → Check `TROUBLESHOOTING.md`
5. **Still stuck?** → Check all docs in `docs/` directory

---

**Last Updated**: 2024
**Maintained By**: Data Engineering Team

---

## Quick Start Path for Different Roles

### 👨‍💼 Business User / Stakeholder
1. `README_dbt.md` - What does this do?
2. `docs/mmsr-dashboard-prd.md` - PowerBI integration
3. `FINAL_ARCHITECTURE_SUMMARY.md` - High-level decisions

### 👨‍💻 Developer
1. `README_dbt.md` - Setup and overview
2. `RUN_DBT.md` - How to run
3. `DBT_TRANSFORMATION_SUMMARY.md` - Architecture details
4. `TROUBLESHOOTING.md` - When things go wrong

### 🏗️ Architect / Code Reviewer
1. `FOREIGN_KEYS_IN_DBT_ANALYSIS.md` - Why no FK?
2. `FINAL_ARCHITECTURE_SUMMARY.md` - Design decisions
3. `COUNTER_ARGUMENT_ARCHITECT.md` - Counter-arguments
4. `docs/database-design.md` - Complete schema

### 📊 Data Analyst / BI Developer
1. `docs/POWER_BI_IMPLEMENTATION_PLAN.md` - **Complete Power BI implementation plan** ⭐
2. `docs/POWER_BI_QUICK_REFERENCE.md` - Quick reference guide
3. `docs/CALCULATION_FLOW_5MIN_TO_YTD.md` - **Calculation methodology** ⭐
4. `docs/METER_POLARITY_DETECTION.md` - **Meter polarity detection** ⚠️
5. `docs/database-design.md` - What tables are available?
6. `DBT_TRANSFORMATION_SUMMARY.md` - How data flows
7. `docs/Ideas_Database_PowerBI.md` - BI ideas
