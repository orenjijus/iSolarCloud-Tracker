# MMSR Documentation — Master Index

Indeks utama dokumentasi proyek MMSR (solar data pipeline, dbt, Power BI). Gunakan dokumen ini untuk menemukan panduan, arsitektur, dan hasil analisis.

---

## 🗂 Struktur dokumentasi

| Lokasi | Isi |
|--------|-----|
| **docs/** (folder ini) | Arsitektur sistem, database design, Power BI, weekly log, cleaning log, PRD, Agent workflows |
| **dbt/docs/** | Dokumentasi dbt: arsitektur dbt, fact tables, data quality, dim_assets, troubleshooting FusionSolar, seeds |
| **reports/** | Hasil analisis & validasi: Excel vs DB crosscheck, validasi, investigasi per site, panduan operasional |

---

## 🎯 Mulai dari sini (by role)

### Arsitek / Tech lead
- [Arsitektur sistem (high-level)](./architecture.md)
- [Database design](./database-design.md)
- [Arsitektur dbt (staging, incremental, mart)](../dbt/docs/architecture/README.md)

### Data engineer
- [Cara menjalankan dbt](../dbt/docs/RUN_DBT.md) — [dbt Documentation Index](../dbt/docs/DOCUMENTATION_INDEX.md)
- [Troubleshooting FusionSolar](../dbt/docs/FUSIONSOLAR_TROUBLESHOOTING.md)
- [Fact tables (5min, availability)](../dbt/docs/FACT_TABLES_DOCUMENTATION_INDEX.md)

### BI / Analis
- [Power BI implementation plan](./POWER_BI_IMPLEMENTATION_PLAN.md) — [Quick reference](./POWER_BI_QUICK_REFERENCE.md)
- [Excel vs DB crosscheck — master](../reports/excel-crosscheck/README.md) (panduan + hasil di reports)
- [Weekly log (Power BI)](./weekly-log/README.md)

### Project / Product
- [MMSR Dashboard PRD](./mmsr-dashboard-prd.md)
- [Proposal organisasi dokumentasi](./PROPOSAL_ORGANIZE_DOCUMENTATION.md)

---

## 📁 docs/ — Isi utama

### Arsitektur & database
- [architecture.md](./architecture.md) — PV system data architecture (ingestion → DB → dbt → BI)
- [database-design.md](./database-design.md) — Schema & tabel
- [DATABASE_SIZING_DETAIL.md](./DATABASE_SIZING_DETAIL.md) — Sizing

### Power BI & reporting
- [POWER_BI_IMPLEMENTATION_PLAN.md](./POWER_BI_IMPLEMENTATION_PLAN.md)
- [POWER_BI_QUICK_REFERENCE.md](./POWER_BI_QUICK_REFERENCE.md)
- [mmsr-dashboard-prd.md](./mmsr-dashboard-prd.md)
- [Weekly log](./weekly-log/README.md) — Summary, Power BI, quick start, seed vs mart

### Data & operasional
- [CLEANING_LOG_IMPLEMENTATION_GUIDE_ID.md](./CLEANING_LOG_IMPLEMENTATION_GUIDE_ID.md) — Cleaning log
- [EXCEL_FORMAT_VALIDATION_GUIDE.md](./EXCEL_FORMAT_VALIDATION_GUIDE.md) — Validasi format Excel

### Konteks existing & Agent
- [Existing-context/](./Existing-context/) — Runbook, ERD, database existing, ide Power BI
- [Agent-ideas/](./Agent-ideas/) — Workflow agent (data engineer, BI, PM, dll.)

---

## 📁 dbt/docs/ — Pintu masuk

- [DOCUMENTATION_INDEX.md](../dbt/docs/DOCUMENTATION_INDEX.md) — Indeks lengkap dbt
- [RUN_DBT.md](../dbt/docs/RUN_DBT.md) — Cara run dbt
- [FACT_TABLES_DOCUMENTATION_INDEX.md](../dbt/docs/FACT_TABLES_DOCUMENTATION_INDEX.md) — Fact tables 5min
- [DATA_QUALITY_INDEX.md](../dbt/docs/DATA_QUALITY_INDEX.md) — Data quality framework
- [architecture/](../dbt/docs/architecture/) — Keputusan arsitektur dbt (incremental, staging, mart)

---

## 📁 reports/ — Hasil analisis & panduan

- [reports/README.md](../reports/README.md) — Kategori & navigasi semua report
- **Excel vs DB**: [reports/excel-crosscheck/README.md](../reports/excel-crosscheck/README.md) — **Master Excel crosscheck** (panduan + link hasil)
- Validasi & investigasi per site: lihat [reports/README.md](../reports/README.md)

---

## 🔗 Link cepat

| Butuh | Dokumen |
|-------|---------|
| Cara run dbt | [dbt/docs/RUN_DBT.md](../dbt/docs/RUN_DBT.md) |
| Excel vs DB crosscheck | [reports/excel-crosscheck/README.md](../reports/excel-crosscheck/README.md) |
| Arsitektur sistem | [docs/architecture.md](./architecture.md) |
| Arsitektur dbt | [dbt/docs/architecture/README.md](../dbt/docs/architecture/README.md) |
| Power BI | [docs/POWER_BI_IMPLEMENTATION_PLAN.md](./POWER_BI_IMPLEMENTATION_PLAN.md) |
| Weekly log | [docs/weekly-log/README.md](./weekly-log/README.md) |

---

*Terakhir diperbarui: Jan 2025 — setelah reorganisasi dokumentasi.*
