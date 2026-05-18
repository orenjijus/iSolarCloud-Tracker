# Summary: PR Format Fix

**Tanggal**: 2025-01-XX  
**Status**: ✅ **FIXED**

---

## Masalah yang Diperbaiki

### Root Cause
- Excel menyimpan PR sebagai **decimal (0-1)**: `0.73` berarti 73%
- Database menyimpan PR sebagai **percentage (0-100)**: `73.45` berarti 73.45%
- Query comparison sebelumnya mengasumsikan Excel bisa punya '%' (string), padahal Excel sudah numeric decimal

### Impact
- **PR GHI Match Rate**: 0-37% (sangat rendah)
- **PR POA Match Rate**: 0-24% (sangat rendah)
- Perbedaan rata-rata: ~76.48 (karena format tidak match)

---

## Solusi yang Diterapkan

### Perubahan di Query Comparison

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
-- Convert PR from decimal (0-1) to percentage (0-100) to match DB format
-- Excel stores PR as decimal (0-1), DB stores as percentage (0-100)
CASE 
    WHEN pr_ghi_actual IS NULL THEN NULL
    ELSE pr_ghi_actual * 100.0
END as excel_pr_ghi
```

---

## Files yang Diupdate

1. ✅ `dbt/analyses/compare_excel_vs_db_detail.sql`
2. ✅ `dbt/analyses/compare_excel_vs_db_all_sites.sql`
3. ✅ `dbt/analyses/compare_excel_vs_db_large_differences.sql`
4. ✅ `dbt/analyses/compare_excel_vs_db_large_differences_summary.sql`
5. ✅ `dbt/analyses/compare_excel_vs_db_anomaly_dates_list.sql`
6. ✅ `EXCEL_VS_DB_COMPARISON_ANALYSIS.md` (dokumentasi)

---

## Expected Impact

### Sebelum Fix
- PR GHI Match Rate: 0-37%
- PR POA Match Rate: 0-24%
- Avg Difference: ~76.48

### Setelah Fix (Expected)
- PR GHI Match Rate: Diharapkan >80% (dengan tolerance 1%)
- PR POA Match Rate: Diharapkan >80% (dengan tolerance 1%)
- Avg Difference: Diharapkan <5% (masih ada perbedaan sistematis yang perlu investigasi)

---

## Next Steps

1. ✅ **Fix Applied**: Semua query comparison sudah diupdate
2. ⏳ **Re-run Comparison**: Jalankan query comparison untuk verifikasi match rate meningkat
3. ⏳ **Investigate Systematic Difference**: Jika masih ada perbedaan ~4.76%, perlu investigasi lebih lanjut:
   - Apakah formula perhitungan sama?
   - Apakah capacity values sama?
   - Apakah input data (Energy, GHI) sama?

---

**Status**: ✅ **COMPLETED** - Siap untuk testing

