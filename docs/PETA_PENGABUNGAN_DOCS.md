# Peta Penggabungan & Penghapusan Dokumentasi

Dokumen ini memetakan file-file yang **satu konteks** dan **sudah ter-update**, lalu mengusulkan penggabungan jadi satu doc dan file yang bisa dihapus/diarsipkan. **Konfirmasi setiap blok** sebelum eksekusi.

---

## Legenda

- **GABUNG** = Beberapa file digabung jadi 1 file master (isi digabung per section).
- **HAPUS/ARSIP** = File dihapus atau dipindah ke archive setelah isi masuk ke master.
- **TETAP** = Tidak digabung (tetap 2 file atau sudah optimal).

---

## A. docs/ (project-level)

### A1. Cleaning Log — **GABUNG 4 → 1**

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `CLEANING_LOG_IMPLEMENTATION_GUIDE_ID.md` | Panduan implementasi, file yang dibuat, struktur tabel, DAX | **Digabung** ke master |
| 2 | `CLEANING_LOG_NEXT_STEPS.md` | Langkah setelah dbt seed (verifikasi, perbaiki data, mart) | **Digabung** ke master |
| 3 | `cleaning-log-is-active-explanation.md` | Penjelasan kolom `is_active` (soft delete, koreksi) | **Digabung** ke master |
| 4 | `cleaning-log-table-design.md` | Schema DB, CREATE TABLE, data model, contoh query | **Digabung** ke master |

**Usulan hasil:**
- **Buat**: `docs/CLEANING_LOG.md` (satu dokumen dengan section: 1. Overview & table design, 2. Implementation & file yang dibuat, 3. Next steps setelah seed, 4. Kolom is_active).
- **Hapus/arsip**: Keempat file di atas (atau pindah ke `docs/archive/`).

**Konfirmasi A1:** Setuju 4 file cleaning log digabung jadi `docs/CLEANING_LOG.md` dan file lama dihapus/arsip? (Ya/Tidak)

---

### A2. Excel Validation (format & template) — **GABUNG 2 → 1**

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `EXCEL_FORMAT_VALIDATION_GUIDE.md` | Masalah format/capacity (Shoetown), panduan validasi | **Digabung** ke master |
| 2 | `EXCEL_VALIDATION_TEMPLATE.md` | Template struktur Excel (kolom, sheet) untuk validasi POA | **Digabung** ke master |

**Usulan hasil:**
- **Buat**: `docs/EXCEL_VALIDATION.md` (section: 1. Format validation guide, 2. Template struktur Excel).
- **Hapus/arsip**: Kedua file di atas.

**Konfirmasi A2:** Setuju 2 file Excel validation digabung jadi `docs/EXCEL_VALIDATION.md` dan file lama dihapus/arsip? (Ya/Tidak)

---

### A3. Power BI — **TETAP 2**

| File | Isi | Aksi |
|------|-----|------|
| `POWER_BI_IMPLEMENTATION_PLAN.md` | Plan lengkap (arsitektur, DAX, drill-down, checklist) | **Tetap** |
| `POWER_BI_QUICK_REFERENCE.md` | Quick reference (tabel kunci, flow, essential DAX) | **Tetap** |

Alasan: Fungsinya beda (satu lengkap, satu ringkas); quick ref banyak dirujuk. Tidak digabung.

**Konfirmasi A3:** Setuju Power BI tetap 2 file? (Ya/Tidak)

---

## B. dbt/docs/ (dbt-specific)

### B1. Site Performance — **GABUNG 3 → 1**

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `SITE_PERFORMANCE_ARCHITECTURE.md` | Arsitektur 3-level mart, availability capacity-weighted | **Digabung** ke master |
| 2 | `SITE_PERFORMANCE_DAILY_IMPLEMENTATION.md` | Rencana daily (energy, sensor GHI/POA, availability) | **Digabung** ke master |
| 3 | `SITE_PERFORMANCE_IMPLEMENTATION_PLAN.md` | Site performance calculations, formula, action items | **Digabung** ke master |

**Usulan hasil:**
- **Buat**: `dbt/docs/SITE_PERFORMANCE.md` (section: 1. Architecture, 2. Daily implementation, 3. Implementation plan & calculations).
- **Hapus/arsip**: Ketiga file di atas.

**Konfirmasi B1:** Setuju 3 file site performance digabung jadi `dbt/docs/SITE_PERFORMANCE.md` dan file lama dihapus/arsip? (Ya/Tidak)

---

### B2. Seed — **GABUNG 2 → 1**

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `SEED_MANAGEMENT.md` | Kategori seed (static/dynamic/config), kapan update, cara run | **Jadi dasar** master |
| 2 | `SEED_AUTO_UPDATE_GUIDE.md` | Auto-update seeds saat dbt run (script, workflow) | **Digabung** sebagai section di master |

**Usulan hasil:**
- **Update**: `dbt/docs/SEED_MANAGEMENT.md` — tambah section "Auto-update seeds saat dbt run" (isi dari SEED_AUTO_UPDATE_GUIDE).
- **Hapus/arsip**: `SEED_AUTO_UPDATE_GUIDE.md`.

**Konfirmasi B2:** Setuju SEED_AUTO_UPDATE_GUIDE digabung ke SEED_MANAGEMENT dan file SEED_AUTO_UPDATE_GUIDE dihapus/arsip? (Ya/Tidak)

---

### B3. Referensi Tabel — **GABUNG 2 → 1**

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `REFERENSI_TABEL_DATABASE.md` | Referensi lengkap (mart, dimensions, staging, seeds) | **Digabung** ke master |
| 2 | `REFERENSI_TABEL_SIMPLE.md` | Tabel singkat (nama tabel | deskripsi | schema) | **Digabung** sebagai quick ref di master |

**Usulan hasil:**
- **Buat**: `dbt/docs/REFERENSI_TABEL.md` (section: 1. Quick reference [tabel simple], 2. Detail [isi REFERENSI_TABEL_DATABASE]).
- **Hapus/arsip**: Kedua file di atas.

**Konfirmasi B3:** Setuju 2 file referensi tabel digabung jadi `dbt/docs/REFERENSI_TABEL.md` dan file lama dihapus/arsip? (Ya/Tidak)

---

### B4. Incremental Filter (issue & fix) — **GABUNG 2 → 1**

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `INCREMENTAL_FILTER_ISSUE_EXPLANATION.md` | Kenapa data FusionSolar tidak masuk (root cause filter) | **Digabung** ke master |
| 2 | `LONG_TERM_FIX_INCREMENTAL_FILTER_SCENARIO.md` | Solusi per-system filter, skenario implementasi | **Digabung** ke master |

**Usulan hasil:**
- **Buat**: `dbt/docs/INCREMENTAL_FILTER.md` (section: 1. Penjelasan issue, 2. Solusi long-term & skenario).
- **Hapus/arsip**: Kedua file di atas.

**Konfirmasi B4:** Setuju 2 file incremental filter digabung jadi `dbt/docs/INCREMENTAL_FILTER.md` dan file lama dihapus/arsip? (Ya/Tidak)

---

### B5. Fact Tables (summary/overview) — **GABUNG 3 → 1 (opsional)**

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `FACT_TABLES_COMPLETE_SUMMARY.md` | Executive summary, overview 3 fact tables, build results | **Digabung** ke overview |
| 2 | `FACT_TABLES_IMPLEMENTATION_SUMMARY.md` | Implementation summary (fact tables yang dibuat, dependencies) | **Digabung** ke overview |
| 3 | `FACT_TABLES_VALIDATION_SUMMARY.md` | Validation summary (POA/GHI fallback, inverter tracking) | **Digabung** ke overview |

**Tetap tidak digabung:**  
- `FACT_TABLES_DOCUMENTATION_INDEX.md` (entry point),  
- `FACT_TABLES_QUICK_REFERENCE.md`,  
- `FACT_5MIN_CALCULATED_METRICS_DESIGN.md`,  
- `FACT_TABLES_VALIDATION_RESULTS.md` / `FACT_TABLES_VALIDATION_ANALYSIS.md` (detail).

**Usulan hasil:**
- **Buat**: `dbt/docs/FACT_TABLES_OVERVIEW.md` (gabungan executive + implementation + validation summary).
- **Hapus/arsip**: FACT_TABLES_COMPLETE_SUMMARY, FACT_TABLES_IMPLEMENTATION_SUMMARY, FACT_TABLES_VALIDATION_SUMMARY.
- **Update**: FACT_TABLES_DOCUMENTATION_INDEX agar mengarah ke FACT_TABLES_OVERVIEW.

**Konfirmasi B5:** Setuju 3 file fact tables summary digabung jadi `dbt/docs/FACT_TABLES_OVERVIEW.md` dan 3 file lama dihapus/arsip? (Ya/Tidak)

---

### B6. Excel Crosscheck (dbt) — **RINGKAS + ARSIP**

Master Excel vs DB sudah di **reports/excel-crosscheck/**. Di dbt/docs masih ada:

| No | File saat ini | Isi singkat | Aksi |
|----|----------------|-------------|------|
| 1 | `EXCEL_CROSSCHECK_GUIDE.md` | How-to crosscheck availability dari fact tables (query, export) | **Tetap** (technical how-to) atau digabung ke satu "dbt Excel" doc |
| 2 | `EXCEL_COMPARISON_SUMMARY.md` | Hasil perbandingan (point-in-time) | **Arsip/pindah** ke reports/excel-crosscheck atau archive |
| 3 | `EXCEL_COMPARISON_RESULTS.md` | Hasil perbandingan detail | **Arsip/pindah** |
| 4 | `EXCEL_CROSSCHECK_RESULTS.md` | Data siap banding (point-in-time) | **Arsip/pindah** |
| 5 | `EXCEL_CROSSCHECK_COMPARISON_TABLE.md` | Template tabel perbandingan | **Arsip/pindah** atau link ke reports |

**Usulan hasil:**
- **Tetap**: `EXCEL_CROSSCHECK_GUIDE.md` (tetap sebagai panduan teknis availability crosscheck dari sisi dbt/fact tables), dengan paragraf di atas: "Panduan & hasil crosscheck lengkap: reports/excel-crosscheck/README.md".
- **Pindah ke dbt/docs/archive/**: EXCEL_COMPARISON_SUMMARY, EXCEL_COMPARISON_RESULTS, EXCEL_CROSSCHECK_RESULTS, EXCEL_CROSSCHECK_COMPARISON_TABLE (hasil/template point-in-time; master hasil di reports).

**Konfirmasi B6:** Setuju EXCEL_CROSSCHECK_GUIDE tetap; 4 file hasil/template Excel di dbt dipindah ke archive? (Ya/Tidak)

---

## C. Ringkasan konfirmasi — ✅ Selesai (Jan 2025)

| Id | Usulan | Konfirmasi | Status |
|----|--------|------------|--------|
| A1 | Cleaning log: 4 → 1 (`docs/CLEANING_LOG.md`) | Ya | ✅ Selesai; file lama di docs/archive/ |
| A2 | Excel validation: 2 → 1 (`docs/EXCEL_VALIDATION.md`) | Ya | ✅ Selesai; file lama di docs/archive/ |
| A3 | Power BI: tetap 2 file | Ya | ✅ Tidak ada perubahan |
| B1 | Site performance: 3 → 1 (`dbt/docs/SITE_PERFORMANCE.md`) | Ya | ✅ Selesai; file lama di dbt/docs/archive/ |
| B2 | Seed: SEED_AUTO_UPDATE_GUIDE → section di SEED_MANAGEMENT | Ya | ✅ Selesai; SEED_AUTO_UPDATE_GUIDE di archive |
| B3 | Referensi tabel: 2 → 1 (`dbt/docs/REFERENSI_TABEL.md`) | Ya | ✅ Selesai; file lama di dbt/docs/archive/ |
| B4 | Incremental filter: 2 → 1 (`dbt/docs/INCREMENTAL_FILTER.md`) | Ya | ✅ Selesai; file lama di dbt/docs/archive/ |
| B5 | Fact tables summary: 3 → 1 (`dbt/docs/FACT_TABLES_OVERVIEW.md`) | Ya | ✅ Selesai; FACT_TABLES_DOCUMENTATION_INDEX di-update |
| B6 | Excel dbt: guide tetap; 4 file hasil/template → archive | Ya | ✅ Selesai; 4 file di dbt/docs/archive/, note di EXCEL_CROSSCHECK_GUIDE |

Eksekusi selesai. File master baru: docs/CLEANING_LOG.md, docs/EXCEL_VALIDATION.md; dbt/docs/SITE_PERFORMANCE.md, REFERENSI_TABEL.md, INCREMENTAL_FILTER.md, FACT_TABLES_OVERVIEW.md; SEED_MANAGEMENT.md (ditambah section Auto-Update).
