# Excel vs DB Crosscheck — Master

Panduan dan hasil crosscheck antara database (`mart_site_performance_daily`, fact tables) dengan Excel report. Excel dipakai sebagai **benchmark** untuk validasi.

---

## 🎯 Tujuan

1. **Validasi**: Memastikan data database sesuai dengan Excel  
2. **Discrepancy**: Menemukan perbedaan (tanggal, site, kolom)  
3. **Root cause**: Menentukan penyebab (database vs Excel)  
4. **Audit**: Mendokumentasikan temuan untuk perbaikan  

---

## 🚀 Quick start (5 menit)

1. **Export Excel** → Save As CSV (`excel_daily_performance.csv`).  
2. **Export DB** → Jalankan query export di panduan lengkap (Step 1); simpan sebagai CSV.  
3. **Import Excel ke DB** → Buat temp table + `COPY` dari CSV (lihat Step 2 di panduan lengkap).  
4. **Jalankan comparison** → Query perbandingan (Step 3–5 di panduan lengkap).  
5. **Review discrepancy** → Summary report + root cause analysis jika perlu.

**Checklist**: Export Excel → Temp table → Import → Comparison → Summary → Root cause (jika ada).

---

## 📚 Panduan di folder ini

| Dokumen | Isi |
|---------|-----|
| **[DATABASE_EXCEL_CROSS_CHECK_GUIDE.md](./DATABASE_EXCEL_CROSS_CHECK_GUIDE.md)** | Panduan lengkap: struktur data, workflow step-by-step, query export/import/comparison, special cases (POA override, GHI fallback MMKI) |
| **[CROSS_CHECK_QUICK_START.md](./CROSS_CHECK_QUICK_START.md)** | Ringkasan langkah cepat + checklist + special cases MMKI |

---

## 📊 Hasil & analisis (di folder ini)

- **EXCEL_VS_DB_COMPARISON_*** — Hasil perbandingan (analysis, results, updated)  
- **CROSSCHECK_*** — Analisis crosscheck, toleransi, baseline  
- **MONTHLY_COMPARISON_*** — Panduan & hasil perbandingan bulanan  
- **COMPARISON_RESULTS_SUMMARY**, **SITES_CAUSING_MONTHLY_DIFFERENCES**  
- **LARGE_DIFFERENCES_***, **TOLERANCE_UPDATE_SUMMARY**  
- **KNOWN_EXCEL_ISSUES**, **UNMATCH_RECORDS_FOR_MANUAL_CHECK**  

*(File-file di atas dipindahkan ke subfolder ini agar semua materi Excel vs DB dalam satu tempat.)*

---

## 🔗 Terkait

- **Fact tables & availability**: [dbt/docs/FACT_TABLES_DOCUMENTATION_INDEX.md](../../dbt/docs/FACT_TABLES_DOCUMENTATION_INDEX.md) — validasi availability, MIT, POA  
- **Excel crosscheck dari sisi dbt (availability)**: [dbt/docs/EXCEL_CROSSCHECK_GUIDE.md](../../dbt/docs/EXCEL_CROSSCHECK_GUIDE.md) — crosscheck daily availability dari fact tables  

---

*Master Excel crosscheck — Jan 2025.*
