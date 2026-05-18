# Proposal: Organisasi & Konsolidasi Dokumentasi

**Tujuan**: Merapikan konteks di `dbt/docs/`, `docs/`, dan `reports/` — mengurangi tumpang tindih, menetapkan master file per topik, dan memudahkan navigasi.

---

## 1. Ringkasan Temuan

### 1.1 Isi per direktori

| Lokasi | Jumlah (approx) | Isi utama |
|--------|------------------|-----------|
| **dbt/docs/** | ~60+ file | Arsitektur dbt, fact tables, data quality, Excel crosscheck, dim_assets, troubleshooting FusionSolar, seeds, site performance, indeks |
| **docs/** | ~25+ file | Arsitektur sistem, database design, Power BI, weekly log, cleaning log, Excel validation, PRD, Agent workflows, existing context |
| **reports/** | ~70 file | Hasil analisis (validation, crosscheck, PR, POA, site-specific), beberapa panduan (crosscheck, monthly comparison, investigation) |

### 1.2 Tumpang tindih yang teridentifikasi

| Topik | dbt/docs | docs/ | reports/ | Masalah |
|-------|----------|-------|----------|--------|
| **Excel vs DB / Crosscheck** | EXCEL_CROSSCHECK_GUIDE, EXCEL_CROSSCHECK_*, EXCEL_COMPARISON_* | - | DATABASE_EXCEL_CROSS_CHECK_GUIDE, CROSS_CHECK_QUICK_START, EXCEL_VS_DB_*, CROSSCHECK_*, MONTHLY_* | Dua sumber panduan + banyak hasil serupa; bingung “mulai dari mana” |
| **Arsitektur** | architecture/ (8 file), FINAL_ARCHITECTURE_SUMMARY | architecture.md | - | Keputusan dbt vs gambaran sistem tercerai; banyak file analisis staging/incremental |
| **Validation** | FACT_TABLES_VALIDATION_* | - | VALIDATION_*, VALIDATION_*_SUMMARY, dll. | dbt = cara & ekspektasi; reports = hasil run — belum jelas dipisah dan di-index |
| **TabularEditor** | TabularEditor2_Comprehensive_Measures_Instructions | - | TabularEditor2_Instructions | Dua panduan, kemungkinan overlap |
| **Dim Assets** | BEST_PRACTICE, QUICK_REFERENCE, SOLUTION_*, WHY_* | - | - | 4+ file untuk satu domain; bisa satu master |
| **FusionSolar** | TROUBLESHOOTING_*, QUICK_FIX_*, ISSUE_* | - | - | Beberapa doc troubleshooting terpisah |
| **Weekly Log** | - | WEEKLY_LOG_*, SEED_VS_MART_WEEKLY_LOG | - | Beberapa file; bisa satu folder + satu entry point |

---

## 2. Prinsip organisasi yang diusulkan

1. **Satu master per topik**: Satu “file utama” atau “epic” per tema (Excel crosscheck, arsitektur dbt, dim_assets, dll.) dengan link ke detail/analisis lain.
2. **Pisah “panduan” vs “hasil analisis”**: Panduan/how-to di docs (atau dbt/docs); hasil run, validasi, dan investigasi di reports (dengan README kategorisasi).
3. **Indeks master di root docs**: Satu `docs/README.md` (atau `DOCUMENTATION_MASTER_INDEX.md`) yang mengarahkan ke semua area: docs, dbt/docs, reports.
4. **Kurangi duplikasi teks**: Gabung file yang overlap (mis. beberapa analisis arsitektur dbt jadi 1–2 file dengan section jelas).
5. **Reports tetap banyak file, tapi terkategori**: Tidak wajib merge 70 file; cukup kategorisasi + README + arsip yang jelas redundant.

---

## 3. Struktur yang diusulkan

### 3.1 `docs/` (master project-level)

- **Tetap**: `architecture.md`, `database-design.md`, PRD, Power BI, Agent-ideas, Existing-context, PDF referensi.
- **Baru**:
  - **`docs/README.md`** atau **`DOCUMENTATION_MASTER_INDEX.md`**: indeks utama ke semua dokumentasi (docs, dbt/docs, reports).
  - **`docs/excel-crosscheck/`** (opsional): satu master “Excel vs DB Crosscheck” — gabung panduan dari dbt + reports; link ke reports untuk hasil spesifik.
- **Weekly log**: Satu folder **`docs/weekly-log/`** berisi:
  - `README.md` (entry point: summary + link ke POWER_BI, QUICK_START, SEED_VS_MART).
  - Pindahkan/relink WEEKLY_LOG_* dan SEED_VS_MART_WEEKLY_LOG ke sini.

### 3.2 `dbt/docs/` (dbt-specific)

- **architecture/**  
  - Gabung 8 file analisis jadi **1–2 file**:
    - **`README.md`**: ringkasan + link ke keputusan final.
    - **`ARCHITECTURE_DECISIONS.md`**: merge ANALISIS_*, DATA_FLOW, FINAL_*, RESPONSE_*, COUNTER_*, FINAL_VERDICT (per section).
  - Tetap referensi ke `docs/architecture.md` untuk sistem secara keseluruhan.

- **transformation theory/**  
  - Tetap atau gabung jadi satu **`TRANSFORMATION_AND_FK.md`** (FK + DBT summary + Hyperscale singkat) + quick reference terpisah jika perlu.

- **Konsolidasi per topik**:
  - **Dim Assets**: Satu **`DIM_ASSETS.md`** dengan section: Overview, Quick reference, Best practice, Solution (auto refresh), FAQ (why not run).
  - **FusionSolar**: Satu **`FUSIONSOLAR_TROUBLESHOOTING.md`** (gabung TROUBLESHOOTING_*, QUICK_FIX_*, ISSUE_*).
  - **Fact tables**: Tetap pakai **FACT_TABLES_DOCUMENTATION_INDEX.md**; pastikan COMPLETE_SUMMARY, QUICK_REFERENCE, DESIGN, VALIDATION tidak mengulang paragraf yang sama.

- **Excel**:
  - Opsi A: Panduan crosscheck pindah ke `docs/excel-crosscheck/` (satu master), dbt/docs hanya link + query/technical notes.
  - Opsi B: Tetap satu **EXCEL_CROSSCHECK_MASTER.md** di dbt/docs yang merge EXCEL_CROSSCHECK_GUIDE + COMPARISON_TABLE + link ke reports untuk hasil.

- **Berkas non-doc**:
  - **`debug_*.sql`**, **`*.csv`** (mis. pg_stat_user_tables): pindah ke `dbt/scripts/` atau `dbt/analyses/` agar dbt/docs hanya berisi markdown.

### 3.3 `reports/` (hasil analisis & one-off)

- **Tetap** banyak file; fokus pada **kategorisasi + navigasi**.
- **Baru**:
  - **`reports/README.md`** dengan:
    - Kategori: Excel vs DB / Validation, PR & POA, Site-specific (Shoetown, MMKI, Garuda, dll.), Config & seeds, TabularEditor, Lain-lain.
    - Penjelasan singkat: “reports = hasil analisis/validasi; panduan lengkap ada di docs/ atau dbt/docs”.
    - Link ke master docs (Excel crosscheck, validation, dll.).
- **Opsional** subfolder (jika ingin lebih rapi):
  - `reports/validation/` — VALIDATION_*, SUMMARY_*
  - `reports/site-investigations/` — SHOETOWN_*, MMKI*, GARUDA_*, POA_*, PR_*
  - `reports/guides/` — DATABASE_EXCEL_CROSS_CHECK_GUIDE, CROSS_CHECK_QUICK_START, MONTHLY_COMPARISON_GUIDE, INVESTIGATION_QUERIES_GUIDE (atau tetap di root + kategori di README).
- **TabularEditor**: Satu master (pilih yang lebih lengkap antara dbt vs reports); yang lain jadi link/alias atau hapus jika benar-benar duplikat.
- **Redundant**: File “FINAL_*_SUMMARY” atau “VALIDATION_*_SUMMARY” yang benar-benar duplikat bisa di-archive (mis. `reports/archive/`) atau di-merge ke satu summary per epic.

---

## 4. Master file & epic (checklist)

| Epic / Topik | Master file / lokasi | Action |
|--------------|----------------------|--------|
| Dokumentasi seluruh repo | `docs/README.md` atau `docs/DOCUMENTATION_MASTER_INDEX.md` | Buat baru; link ke dbt/docs, reports, Agent, Existing-context |
| Arsitektur sistem (high-level) | `docs/architecture.md` | Tetap; pastikan link ke dbt/docs/architecture |
| Arsitektur dbt (staging, incremental, mart) | `dbt/docs/architecture/README.md` + `ARCHITECTURE_DECISIONS.md` | Merge 8 file → 2 |
| Excel vs DB crosscheck | `docs/excel-crosscheck/README.md` atau `dbt/docs/EXCEL_CROSSCHECK_MASTER.md` | Satu panduan; hasil di reports + link |
| Fact tables (5min, availability) | `dbt/docs/FACT_TABLES_DOCUMENTATION_INDEX.md` + COMPLETE_SUMMARY, QUICK_REFERENCE | Rapikan; kurangi duplikasi teks |
| Data quality | `dbt/docs/DATA_QUALITY_INDEX.md` | Tetap sebagai entry point |
| Dim Assets | `dbt/docs/DIM_ASSETS.md` | Merge 4 file → 1 |
| FusionSolar troubleshooting | `dbt/docs/FUSIONSOLAR_TROUBLESHOOTING.md` | Merge 3+ file → 1 |
| Weekly log | `docs/weekly-log/README.md` | Satu folder; kumpulkan WEEKLY_LOG_* |
| Validation (cara + hasil) | dbt/docs = cara; reports = hasil | README reports + DOCUMENTATION_INDEX jelas |
| TabularEditor | Satu file (dbt atau docs) | Pilih master; yang lain link |
| Reports (navigasi) | `reports/README.md` | Kategorisasi + link ke master docs |

---

## 5. Urutan eksekusi yang disarankan

1. **Fase 1 – Indeks & navigasi (low risk)**  
   - Buat `docs/README.md` (master index).  
   - Buat `reports/README.md` (kategori + penjelasan).  
   - Update `dbt/docs/DOCUMENTATION_INDEX.md` agar path/link mengikuti struktur baru (setelah ada keputusan final).

2. **Fase 2 – Konsolidasi dalam dbt/docs**  
   - Merge `dbt/docs/architecture/` → README + ARCHITECTURE_DECISIONS.  
   - Merge dim_assets → DIM_ASSETS.md.  
   - Merge FusionSolar troubleshooting → FUSIONSOLAR_TROUBLESHOOTING.md.  
   - Pindahkan `debug_*.sql`, `*.csv` dari dbt/docs ke dbt/scripts atau dbt/analyses.

3. **Fase 3 – Excel crosscheck master**  
   - Pilih opsi A (docs) atau B (dbt/docs); buat satu master, link dari kedua tempat.  
   - Update reports/README agar “Excel vs DB” mengarah ke master tersebut.

4. **Fase 4 – Weekly log & TabularEditor**  
   - Folder docs/weekly-log/ + README.  
   - Satu master TabularEditor; yang lain jadi link.

5. **Fase 5 – Optional**  
   - Subfolder reports (validation, site-investigations, guides).  
   - Archive/merge summary yang redundant di reports.

---

## 6. Keputusan final (Jan 2025)

- **Setuju** dengan struktur proposal; **subfolder reports** dipakai.
- **Excel crosscheck master** → **reports**: `reports/excel-crosscheck/README.md` (panduan + hasil). Semua materi Excel vs DB di **reports/excel-crosscheck/**.
- **Eksekusi** diterapkan: Fase 1–5 selesai (indeks, subfolder, merge architecture/dim_assets/FusionSolar, weekly-log, script pindah, TabularEditor link, DOCUMENTATION_INDEX update).

---

## 7. Ringkasan yang sudah dikerjakan

| Aksi | Status |
|------|--------|
| docs/README.md (master index) | ✅ |
| reports/README.md + subfolder (excel-crosscheck, validation, site-investigations, guides) | ✅ |
| reports/excel-crosscheck/README.md (master Excel crosscheck) | ✅ |
| dbt/docs/architecture/ → README + ARCHITECTURE_DECISIONS; 8 file → archive/ | ✅ |
| dbt/docs/DIM_ASSETS.md (gabung 4 file dim_assets) | ✅ |
| dbt/docs/FUSIONSOLAR_TROUBLESHOOTING.md (gabung 4 file FusionSolar) | ✅ |
| docs/weekly-log/ + README; WEEKLY_LOG_* dipindah | ✅ |
| dbt/docs → debug*.sql, *.csv → dbt/scripts/ | ✅ |
| TabularEditor cross-link | ✅ |
| dbt/docs/DOCUMENTATION_INDEX.md update | ✅ |

File lama (BEST_PRACTICE_DIM_ASSETS, TROUBLESHOOTING_FUSIONSOLAR_*, dll.) diarsipkan di dbt/docs/archive/.
