# Perbaikan Inconsistency Monthly Target di mart_site_performance_daily

**Tanggal:** 2025-01-XX  
**Status:** ✅ **FIXED**  
**Model Terpengaruh:** `mart_site_performance_daily`  
**Severity:** High - Data Quality Issue

---

## 📋 Executive Summary

Ditemukan masalah inconsistency pada nilai `energy_target_monthly_mwh` di `mart_site_performance_daily` dimana dalam satu bulan yang sama, nilai monthly target berbeda-beda untuk site yang sama. Masalah ini mempengaruhi perhitungan KPI daily dan adjusted KPI yang bergantung pada nilai monthly target.

**Contoh Kasus:**
- Site: PT. MMKI 4.292 MWP - Phase 3
- Bulan: Desember 2025
- Masalah: Beberapa hari memiliki `energy_target_monthly_mwh = 418.394448`, sedangkan hari lainnya memiliki `443.875082`
- Nilai yang benar (dari `mart_simulation_targets_monthly`): `443.8750819`

---

## 🔍 Root Cause Analysis

### Masalah yang Ditemukan

Nilai `energy_target_monthly_mwh` tidak konsisten dalam satu bulan karena **JOIN key mismatch** antara CTE `monthly_targets_for_kpi` dan SELECT statement akhir.

### Detail Teknis

1. **CTE `monthly_targets_for_kpi` (SEBELUM PERBAIKAN):**
   ```sql
   monthly_targets_for_kpi AS (
       SELECT 
           t.site_code,  -- ❌ Hanya menggunakan site_code
           dd.year,
           dd.month,
           SUM(t.energy_target_mwh) as energy_target_monthly_mwh
       FROM mart_simulation_targets_daily t
       ...
       GROUP BY t.site_code, dd.year, dd.month
   )
   ```

2. **JOIN di SELECT Akhir:**
   ```sql
   LEFT JOIN monthly_targets_for_kpi mt_kpi
       ON mt_kpi.site_code = COALESCE(t.site_code, da.site_id::text)  -- ❌ Mismatch!
       AND mt_kpi.year = dd.year
       AND mt_kpi.month = dd.month
   ```

### Mengapa Terjadi Mismatch?

- CTE mengelompokkan berdasarkan `t.site_code` saja
- JOIN akhir menggunakan `COALESCE(t.site_code, da.site_id::text)`
- Ketika `t.site_code` NULL atau tidak match, JOIN gagal dan menghasilkan NULL atau nilai default
- Beberapa baris berhasil match (mendapat nilai benar), beberapa tidak (mendapat nilai salah atau NULL)

### Dampak

1. **Data Quality:** Nilai monthly target tidak konsisten dalam satu bulan
2. **KPI Calculation:** `energy_kpi_daily_mwh` dan `energy_a_kpi_daily_mwh` menjadi salah karena menggunakan monthly target yang tidak konsisten
3. **Reporting:** Laporan bulanan menjadi tidak akurat

---

## ✅ Solusi yang Diterapkan

### Perubahan pada CTE `monthly_targets_for_kpi`

**SEBELUM:**
```sql
monthly_targets_for_kpi AS (
    SELECT 
        t.site_code,
        dd.year,
        dd.month,
        SUM(t.energy_target_mwh) as energy_target_monthly_mwh
    FROM {{ ref('mart_simulation_targets_daily') }} t
    LEFT JOIN {{ ref('dim_date_generated') }} dd
        ON t.date_key = dd.date_key
    WHERE dd.year IS NOT NULL AND dd.month IS NOT NULL
    GROUP BY t.site_code, dd.year, dd.month
)
```

**SESUDAH:**
```sql
monthly_targets_for_kpi AS (
    SELECT 
        COALESCE(t.site_code, da.site_id::text) as join_key,  -- ✅ Konsisten dengan JOIN akhir
        dd.year,
        dd.month,
        SUM(t.energy_target_mwh) as energy_target_monthly_mwh
    FROM {{ ref('mart_simulation_targets_daily') }} t
    LEFT JOIN {{ ref('dim_date_generated') }} dd
        ON t.date_key = dd.date_key
    LEFT JOIN {{ ref('dim_assets') }} da 
        ON (da.site_id::text = t.site_code::text OR da.site_name = t.site_name)
        AND da.asset_level = 'Site'
    WHERE dd.year IS NOT NULL AND dd.month IS NOT NULL
    GROUP BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
)
```

### Perubahan pada CTE `monthly_simulation_for_kpi`

Perubahan yang sama diterapkan untuk konsistensi:
```sql
monthly_simulation_for_kpi AS (
    SELECT 
        COALESCE(t.site_code, da.site_id::text) as join_key,  -- ✅ Konsisten
        ...
    GROUP BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
)
```

### Perubahan pada JOIN Conditions

**SEBELUM:**
```sql
LEFT JOIN monthly_targets_for_kpi mt_kpi
    ON mt_kpi.site_code = COALESCE(t.site_code, da.site_id::text)
    AND mt_kpi.year = dd.year
    AND mt_kpi.month = dd.month
```

**SESUDAH:**
```sql
LEFT JOIN monthly_targets_for_kpi mt_kpi
    ON mt_kpi.join_key = COALESCE(t.site_code, da.site_id::text)  -- ✅ Menggunakan join_key
    AND mt_kpi.year = dd.year
    AND mt_kpi.month = dd.month
```

---

## 🔧 Langkah-Langkah Perbaikan

### 1. Update Model SQL
File: `dbt/models/marts/mart_site_performance_daily.sql`
- Update CTE `monthly_targets_for_kpi` (baris 439-453)
- Update CTE `monthly_simulation_for_kpi` (baris 458-472)
- Update JOIN conditions (baris 703-708)

### 2. Reingest Data
Karena `mart_site_performance_daily` adalah incremental model, perlu reingest untuk memperbarui data yang sudah ada:

```bash
cd "c:\Users\Administrator\Documents\Code\MMSR API - Server MA\dbt"
dbt run --select mart_site_performance_daily --vars "{reingest_start_date: '2025-12-01', reingest_end_date: '2025-12-31'}"
```

**Atau untuk full refresh:**
```bash
dbt run --select mart_site_performance_daily --full-refresh
```

---

## ✅ Verifikasi Hasil

### 1. Konsistensi Monthly Target

**Query Verifikasi:**
```sql
SELECT 
    site_name,
    year,
    month,
    COUNT(DISTINCT energy_target_monthly_mwh) as distinct_targets,
    MIN(energy_target_monthly_mwh) as min_target,
    MAX(energy_target_monthly_mwh) as max_target,
    COUNT(*) as total_days
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND year = 2025
    AND month = 12
GROUP BY site_name, year, month;
```

**Hasil:**
- ✅ `distinct_targets = 1` (hanya satu nilai unik)
- ✅ `min_target = max_target = 443.875082` (konsisten)
- ✅ Nilai sesuai dengan `mart_simulation_targets_monthly` (443.8750819)

### 2. Adjusted KPI Calculation

**Query Verifikasi:**
```sql
SELECT 
    date_key,
    energy_target_mwh,
    energy_kpi_monthly_mwh,
    energy_simulation_monthly_mwh,
    energy_a_kpi_daily_mwh as calculated_value,
    (energy_target_mwh * (energy_kpi_monthly_mwh / energy_simulation_monthly_mwh)) as manual_calc,
    ABS(energy_a_kpi_daily_mwh - (energy_target_mwh * (energy_kpi_monthly_mwh / energy_simulation_monthly_mwh))) as difference
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND year = 2025
    AND month = 12;
```

**Hasil:**
- ✅ Perhitungan `energy_a_kpi_daily_mwh` sudah benar
- ✅ Perbedaan dengan manual calculation < 0.000001 (hanya precision rounding)
- ✅ Formula: `energy_target_mwh * (energy_kpi_monthly_mwh / energy_simulation_monthly_mwh)`

### 3. Cross-Reference dengan mart_simulation_targets_monthly

**Query:**
```sql
SELECT 
    'mart_site_performance_daily' as source,
    site_name,
    year,
    month,
    energy_target_monthly_mwh
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND year = 2025
    AND month = 12
GROUP BY site_name, year, month, energy_target_monthly_mwh

UNION ALL

SELECT 
    'mart_simulation_targets_monthly' as source,
    site_name,
    year,
    month,
    energy_target_mwh as energy_target_monthly_mwh
FROM "MMSR"."mart"."mart_simulation_targets_monthly"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND year = 2025
    AND month = 12;
```

**Hasil:**
- ✅ Nilai dari kedua source konsisten (443.875082 vs 443.8750819)
- ✅ Perbedaan kecil hanya karena rounding/precision

### 4. Site Lain

**Query:**
```sql
SELECT 
    site_name,
    year,
    month,
    COUNT(DISTINCT energy_target_monthly_mwh) as distinct_targets
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE year = 2025
    AND month = 12
GROUP BY site_name, year, month
HAVING COUNT(DISTINCT energy_target_monthly_mwh) > 1;
```

**Hasil:**
- ✅ Tidak ada site lain yang mengalami masalah yang sama
- ✅ Semua site memiliki monthly target yang konsisten

---

## 📊 Impact Analysis

### Before Fix
- ❌ Monthly target tidak konsisten dalam satu bulan
- ❌ KPI daily dan adjusted KPI salah
- ❌ Data quality issue yang mempengaruhi reporting

### After Fix
- ✅ Monthly target konsisten untuk semua hari dalam satu bulan
- ✅ KPI daily dan adjusted KPI terhitung dengan benar
- ✅ Data quality issue teratasi

### Affected Metrics
1. `energy_target_monthly_mwh` - ✅ Fixed
2. `energy_kpi_daily_mwh` - ✅ Fixed (bergantung pada monthly target)
3. `energy_a_kpi_daily_mwh` - ✅ Fixed (bergantung pada monthly target)

---

## 🎯 Rekomendasi

### 1. Monitoring
- Tambahkan data quality check untuk memastikan monthly target konsisten dalam satu bulan
- Query monitoring:
  ```sql
  SELECT 
      site_name,
      year,
      month,
      COUNT(DISTINCT energy_target_monthly_mwh) as distinct_targets
  FROM "MMSR"."mart"."mart_site_performance_daily"
  GROUP BY site_name, year, month
  HAVING COUNT(DISTINCT energy_target_monthly_mwh) > 1;
  ```

### 2. Best Practices
- Pastikan JOIN key konsisten antara CTE dan SELECT statement
- Gunakan `COALESCE` dengan logika yang sama di semua tempat
- Test dengan berbagai skenario (NULL values, missing joins, dll)

### 3. Testing
- Test dengan site yang memiliki `site_code` NULL
- Test dengan site yang memiliki multiple matches di `dim_assets`
- Test dengan date range yang berbeda

---

## 📝 File Changes Summary

### Modified Files
1. `dbt/models/marts/mart_site_performance_daily.sql`
   - Lines 439-453: Updated `monthly_targets_for_kpi` CTE
   - Lines 458-472: Updated `monthly_simulation_for_kpi` CTE
   - Lines 703-708: Updated JOIN conditions

### Key Changes
- Added `LEFT JOIN dim_assets` in both CTEs
- Changed grouping key from `t.site_code` to `COALESCE(t.site_code, da.site_id::text)`
- Changed JOIN condition from `mt_kpi.site_code` to `mt_kpi.join_key`

---

## 🔗 Related Documentation

- [dbt Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt CTEs Best Practices](https://docs.getdbt.com/guides/best-practices/ctes)
- [REINGESTION_WORKFLOW.md](../dbt/docs/REINGESTION_WORKFLOW.md)

---

## ✅ Sign-off

**Fixed By:** AI Assistant  
**Date:** 2025-01-XX  
**Verified By:** [User Name]  
**Status:** ✅ **RESOLVED**

---

**Last Updated:** 2025-01-XX
