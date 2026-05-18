# Hasil Verifikasi Format PR: Excel vs Database

**Tanggal Verifikasi**: 2025-01-XX  
**Status**: ✅ **MASALAH DITEMUKAN DAN DIPASTIKAN**

---

## Temuan Utama

### 1. Format Data Excel

**Excel menyimpan PR sebagai DECIMAL (0-1)**, bukan percentage (0-100)
- Contoh: `0.73` berarti 73%
- Tipe data: `real` (numeric)
- **BUKAN** string dengan '%' seperti yang diasumsikan sebelumnya

### 2. Format Data Database

**Database menyimpan PR sebagai PERCENTAGE (0-100)**
- Contoh: `73.45` berarti 73.45%
- Formula: `((daily_energy_mwh * 1000) / daily_ghi_kwh_m2 / actual_capacity_kw) * 100`

### 3. Masalah di Query Comparison Saat Ini

**Query comparison mengasumsikan Excel bisa punya '%' atau tidak**, padahal:
- Excel sudah disimpan sebagai numeric decimal (0-1)
- Query mencoba cek `LIKE '%'` yang tidak relevan untuk numeric
- Logika konversi salah karena mengasumsikan format string

---

## Hasil Verifikasi

### Test 1: Perbandingan Format

| Method | Total Records | Match Count | Match Rate | Avg Diff | Max Diff |
|--------|---------------|-------------|------------|----------|----------|
| **Current Logic** (Excel as decimal) | 3,318 | 0 | **0.00%** | 76.48 | 1,580.80 |
| **Fixed Logic** (Excel * 100 as percent) | 3,318 | 6 | **0.18%** | 4.76 | 1,504.57 |

**Kesimpulan**: 
- ✅ Excel memang perlu dikalikan 100 untuk jadi percentage
- ⚠️ Tapi masih ada perbedaan sistematis (avg diff = 4.76%)

### Test 2: Match Rate dengan Berbagai Tolerance

| Tolerance | Match Rate | Keterangan |
|-----------|------------|------------|
| 0.001 (0.1%) | 0.18% | Hampir tidak ada yang match |
| 0.01 (1%) | 1.39% | Sangat sedikit yang match |
| 0.1 (10%) | 16.34% | Masih rendah |
| 1.0 (100%) | 83.91% | Sebagian besar dalam range 1% |

**Kesimpulan**: 
- Perbedaan rata-rata sekitar **4.76%**
- Sebagian besar data (83.91%) memiliki perbedaan < 1%
- Masih ada perbedaan sistematis yang perlu diinvestigasi lebih lanjut

### Test 3: Contoh Data yang Match (dengan tolerance 0.1%)

| Date | Site | Excel (decimal) | Excel * 100 | DB | Diff |
|------|------|-----------------|-------------|----|----|
| 2025-02-08 | MMKI 1.75 | 0.79 | 79.00 | 78.9999 | 0.0001 |
| 2025-04-03 | Pusan Manis | 0.75 | 75.00 | 74.9997 | 0.0003 |
| 2025-11-07 | PLTS Sumatera | 0.73 | 73.00 | 73.0006 | 0.0006 |

**Kesimpulan**: 
- Data yang match menunjukkan Excel * 100 sangat dekat dengan DB
- Perbedaan kecil kemungkinan karena rounding atau precision

---

## Root Cause Analysis

### Masalah Utama

1. **Format Conversion Error**:
   - Excel: Decimal (0-1) → perlu dikalikan 100
   - Database: Percentage (0-100) → sudah dalam format benar
   - Query saat ini: Tidak mengalikan Excel dengan 100

2. **Perbedaan Sistematis (4.76% avg diff)**:
   - Kemungkinan karena:
     - Perbedaan formula perhitungan
     - Perbedaan rounding/precision
     - Perbedaan capacity values
     - Perbedaan input data (Energy, GHI)

---

## Solusi yang Diperlukan

### Fix 1: Update Query Comparison (PRIORITY 1)

**Ubah logika konversi Excel PR**:

**Dari**:
```sql
CASE 
    WHEN pr_ghi_actual LIKE '%' THEN 
        REPLACE(pr_ghi_actual, '%', '')::numeric / 100.0
    ELSE 
        pr_ghi_actual::numeric
END as excel_pr_ghi
```

**Ke**:
```sql
-- Excel stores PR as decimal (0-1), multiply by 100 to get percentage (0-100)
pr_ghi_actual * 100.0 as excel_pr_ghi
```

**Atau jika ada handling untuk NULL**:
```sql
CASE 
    WHEN pr_ghi_actual IS NULL THEN NULL
    ELSE pr_ghi_actual * 100.0
END as excel_pr_ghi
```

### Fix 2: Investigate Systematic Difference (PRIORITY 2)

Setelah fix format conversion, masih ada perbedaan sistematis ~4.76%. Perlu investigasi:
- Apakah formula perhitungan sama?
- Apakah capacity values sama?
- Apakah input data (Energy, GHI) sama?

---

## Files yang Perlu Diupdate

1. ✅ `dbt/analyses/compare_excel_vs_db_detail.sql`
2. ✅ `dbt/analyses/compare_excel_vs_db_all_sites.sql`
3. ✅ `dbt/analyses/compare_excel_vs_db_large_differences.sql`
4. ✅ `dbt/analyses/compare_excel_vs_db_large_differences_summary.sql`
5. ✅ `dbt/analyses/compare_excel_vs_db_anomaly_dates_list.sql`

**Pattern yang perlu diubah untuk PR GHI**:
- **Dari**: Logika dengan `LIKE '%'` dan conditional division
- **Ke**: `pr_ghi_actual * 100.0` (untuk Excel)

**Pattern yang perlu diubah untuk PR POA**:
- **Dari**: Logika dengan `LIKE '%'` dan conditional division
- **Ke**: `pr_poa_actual * 100.0` (untuk Excel)

---

## Expected Impact

### Sebelum Fix
- **PR GHI Match Rate**: 0-37% (sangat rendah)
- **Root Cause**: Format conversion error

### Setelah Fix (Expected)
- **PR GHI Match Rate**: Diharapkan >80% (dengan tolerance 1%)
- **Remaining Issues**: Perbedaan sistematis ~4.76% perlu investigasi lebih lanjut

---

## Next Steps

1. ✅ **Verifikasi Format**: Selesai - Excel stores as decimal (0-1)
2. ⏳ **Fix Query Comparison**: Update semua comparison queries
3. ⏳ **Re-run Comparison**: Verifikasi match rate meningkat
4. ⏳ **Investigate Systematic Difference**: Cari tahu penyebab perbedaan 4.76%
5. ⏳ **Update Documentation**: Update dengan format yang benar

---

**Status**: ✅ **VERIFIED** - Siap untuk fix

