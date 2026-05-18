# Quick Start: Database vs Excel Cross-Check

## 🚀 Langkah Cepat (5 Menit)

### 1. Export Excel ke CSV
- Buka Excel file
- File → Save As → CSV (Comma delimited)
- Simpan sebagai `excel_daily_performance.csv`

### 2. Import Excel ke Database
```sql
-- Buka file: cross_check_queries.sql
-- Jalankan STEP 2: Buat Temporary Table
-- Edit path CSV di query COPY, lalu jalankan
```

### 3. Jalankan Comparison
```sql
-- Dari file: cross_check_queries.sql
-- Jalankan STEP 3: Comparison Query
-- Review hasil discrepancy
```

### 4. Summary Report
```sql
-- Dari file: cross_check_queries.sql
-- Jalankan STEP 4: Summary Report
-- Lihat overview per site
```

### 5. Root Cause Analysis (jika ada discrepancy)
```sql
-- Dari file: cross_check_queries.sql
-- Jalankan STEP 5: Root Cause Analysis
-- Edit problem_date dan problem_site
```

---

## 📋 Checklist Cepat

- [ ] Excel di-export ke CSV
- [ ] Temporary table dibuat (STEP 2)
- [ ] Data Excel di-import ke temporary table
- [ ] Comparison query dijalankan (STEP 3)
- [ ] Summary report di-review (STEP 4)
- [ ] Root cause analysis untuk discrepancy (STEP 5)

---

## 📚 Dokumentasi Lengkap

Lihat `DATABASE_EXCEL_CROSS_CHECK_GUIDE.md` untuk:
- Penjelasan detail setiap step
- Troubleshooting common issues
- Tips dan best practices

---

## 🔧 File yang Dibutuhkan

1. `cross_check_queries.sql` - Semua query SQL
2. `cross_check_queries_mmki_baseline.sql` - Query khusus untuk MMKI sites (POA override & GHI fallback)
3. `DATABASE_EXCEL_CROSS_CHECK_GUIDE.md` - Dokumentasi lengkap
4. Excel file (export ke CSV)

---

## ⚠️ Special Cases: POA Override & GHI Fallback

### Untuk MMKI Sites (I, II, III)

**POA Override**:
- POA sensors dari MMKI II/III secara fisik di MMKI I, tapi secara logis milik MMKI II/III
- Override mulai berlaku dari tanggal yang ditentukan (lihat `seed_sensor_site_mapping.csv`)
- Gunakan `cross_check_queries_mmki_baseline.sql` untuk baseline check

**GHI Fallback**:
- MMKI II dan III tidak punya GHI sensor, pakai GHI dari MMKI I
- MMKI II GHI harus sama dengan MMKI I GHI (setelah fallback diimplementasi)

**Verification**:
- [ ] Check POA override configuration di `seed_sensor_site_mapping.csv`
- [ ] Check GHI fallback configuration di `seed_sensor_site_mapping.csv`
- [ ] Verify POA override working (MMKI II punya POA values)
- [ ] Verify GHI fallback working (MMKI II GHI = MMKI I GHI)

Lihat `DATABASE_EXCEL_CROSS_CHECK_GUIDE.md` section "Special Cases" untuk detail lebih lanjut.

---

**Tips**: Simpan hasil comparison sebagai CSV untuk dokumentasi!

