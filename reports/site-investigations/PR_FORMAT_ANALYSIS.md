# Analisis Masalah Format PR: Excel vs Database

**Tanggal Analisis**: 2025-01-XX  
**Masalah**: PR GHI dan PR POA menunjukkan perbedaan sistematis antara Excel dan Database  
**Kasus yang Match**: 9 September 2024, MMKI 1, PR GHI = 73.45% (match di Excel dan DB)

---

## Root Cause Analysis

### 1. Format PR di Database

**Formula Database** (dari `mart_site_performance_daily.sql`):
```sql
PR GHI = ((daily_energy_mwh * 1000.0) / daily_ghi_kwh_m2 / actual_capacity_kw) * 100.0
```

**Hasil**: PR disimpan sebagai **percentage (0-100)**, bukan decimal (0-1)
- Contoh: PR = 73.45 berarti 73.45%

---

### 2. Format PR di Excel (dari `site_daily_performance_excel_all`)

Excel menyimpan PR dalam **2 format berbeda**:
1. **Dengan tanda '%'**: `"73.45%"` (string dengan %)
2. **Tanpa tanda '%'**: `"73.45"` atau `73.45` (numeric atau string tanpa %)

---

### 3. Logika Konversi Saat Ini (di query comparison)

**File**: `compare_excel_vs_db_detail.sql` dan `compare_excel_vs_db_all_sites.sql`

```sql
CASE 
    WHEN pr_ghi_actual LIKE '%' THEN 
        REPLACE(pr_ghi_actual, '%', '')::numeric / 100.0  -- Konversi ke decimal (0-1)
    ELSE 
        pr_ghi_actual::numeric  -- Tetap sebagai percentage (0-100)
END as excel_pr_ghi
```

**Masalah**: Logika ini **inconsistent**!
- Jika Excel punya '%' → dikonversi ke **decimal (0-1)**
- Jika Excel tidak punya '%' → tetap sebagai **percentage (0-100)**

---

### 4. Mengapa Ada Perbedaan Sistematis?

**Skenario 1: Excel dengan '%' (misalnya "73.45%")**
- Excel: `"73.45%"` → konversi: `73.45 / 100 = 0.7345` (decimal)
- Database: `73.45` (percentage)
- Perbandingan: `0.7345 vs 73.45` → **SELISIH BESAR!** ❌

**Skenario 2: Excel tanpa '%' (misalnya "73.45")**
- Excel: `"73.45"` → konversi: `73.45` (tetap percentage)
- Database: `73.45` (percentage)
- Perbandingan: `73.45 vs 73.45` → **MATCH!** ✅

**Kesimpulan**: 
- Data Excel yang **punya '%'** akan selalu mismatch karena dikonversi ke decimal
- Data Excel yang **tidak punya '%'** akan match karena tetap sebagai percentage

---

### 5. Bukti: Kasus 9 September 2024, MMKI 1

User menemukan bahwa pada tanggal ini, PR GHI match di 73.45%. Ini kemungkinan karena:
- Excel value untuk tanggal ini **tidak punya '%'** (misalnya `"73.45"` atau `73.45`)
- Jadi dikonversi sebagai percentage (0-100), sama dengan database
- Hasilnya match!

---

## Solusi yang Diperlukan

### Opsi 1: Selalu Konversi Excel ke Percentage (0-100) - **RECOMMENDED**

**Logika Baru**:
```sql
CASE 
    WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
    WHEN pr_ghi_actual LIKE '%' THEN 
        NULLIF(REPLACE(pr_ghi_actual, '%', ''), '')::numeric  -- Hapus %, tetap percentage
    ELSE 
        NULLIF(TRIM(pr_ghi_actual), '')::numeric  -- Tetap percentage
END as excel_pr_ghi
```

**Penjelasan**:
- Jika Excel punya '%' → hapus '%', tetap sebagai percentage (0-100)
- Jika Excel tidak punya '%' → gunakan langsung sebagai percentage (0-100)
- Hasil: Selalu percentage (0-100), sama dengan database

---

### Opsi 2: Konversi Database ke Decimal (0-1)

**Tidak direkomendasikan** karena:
- Perlu mengubah semua query comparison
- Database sudah menggunakan format percentage (0-100) secara konsisten
- Lebih mudah mengubah logika konversi Excel

---

## Query untuk Verifikasi

### 1. Cek Format Excel PR
```sql
-- Lihat distribusi format PR di Excel
SELECT 
    CASE WHEN pr_ghi_actual LIKE '%' THEN 'HAS_%' ELSE 'NO_%' END as format_type,
    COUNT(*) as count,
    MIN(pr_ghi_actual) as min_value,
    MAX(pr_ghi_actual) as max_value
FROM public.site_daily_performance_excel_all
WHERE pr_ghi_actual IS NOT NULL
GROUP BY format_type
```

### 2. Test Konversi Baru
```sql
-- Bandingkan match rate dengan logika lama vs baru
-- (Lihat file: analyze_pr_format_issue.sql)
```

### 3. Cek Kasus Spesifik 9 September
```sql
-- (Lihat file: check_pr_format_sept9_mmki1.sql)
```

---

## Impact Analysis

### Sebelum Fix
- **Match Rate PR GHI**: 0-37% (sangat rendah)
- **Root Cause**: Excel values dengan '%' dikonversi ke decimal, sedangkan database percentage

### Setelah Fix (Expected)
- **Match Rate PR GHI**: Diharapkan >90% untuk data yang valid
- **Remaining Issues**: Hanya perbedaan calculation atau data quality issues

---

## Action Items

1. ✅ **Identifikasi Masalah**: Format conversion inconsistent
2. ⏳ **Verifikasi**: Jalankan query `analyze_pr_format_issue.sql` untuk konfirmasi
3. ⏳ **Fix Query**: Update semua comparison queries dengan logika baru
4. ⏳ **Test**: Verifikasi match rate meningkat setelah fix
5. ⏳ **Document**: Update documentation dengan format yang benar

---

## Files yang Perlu Diupdate

1. `dbt/analyses/compare_excel_vs_db_detail.sql`
2. `dbt/analyses/compare_excel_vs_db_all_sites.sql`
3. `dbt/analyses/compare_excel_vs_db_large_differences.sql`
4. `dbt/analyses/compare_excel_vs_db_large_differences_summary.sql`
5. `dbt/analyses/compare_excel_vs_db_anomaly_dates_list.sql`

**Pattern yang perlu diubah**:
- **Dari**: `REPLACE(pr_ghi_actual, '%', '')::numeric / 100.0` (jika punya %)
- **Ke**: `REPLACE(pr_ghi_actual, '%', '')::numeric` (jika punya %)

---

**Status**: ⏳ **PENDING VERIFICATION** - Perlu jalankan query verifikasi dulu sebelum fix

