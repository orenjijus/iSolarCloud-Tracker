# Perbedaan Target Monthly MMKI Phase 3 - Desember 2025

**Tanggal**: 2025-01-XX  
**Site**: PT. MMKI 4.292 MWP - Phase 3  
**Bulan**: Desember 2025  
**Status**: ⚠️ **PERBEDAAN DITEMUKAN**

---

## 📊 Perbandingan Nilai

| Source | Target Monthly (MWh) | Perbedaan vs Excel |
|--------|---------------------|-------------------|
| **Excel** | **418.3944483** | - (baseline) |
| Database (mart_simulation_targets_monthly) | 443.875082 | +25.48 MWh (+6.1%) |
| Database (SUM daily) | 443.875082 | +25.48 MWh (+6.1%) |
| Seed File (SUM) | Perlu dicek | - |

**Perbedaan**: Database lebih besar **25.48 MWh** (6.1%) dibanding Excel

---

## 🔍 Kemungkinan Penyebab Perbedaan

### 1. **Excel Exclude Issue Dates / Unavailability Dates** ⚠️ (Kemungkinan Tinggi)

Excel mungkin menghitung target monthly dengan **exclude issue dates** atau **unavailability dates**, sedangkan database menghitung **include semua hari**.

**Cara Verifikasi**:
```sql
-- Cek apakah ada issue dates di Desember 2025
SELECT 
    date_key,
    site_name,
    issue_type,
    description
FROM "MMSR"."staging"."seed_issue_dates"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND EXTRACT(YEAR FROM date_key) = 2025
    AND EXTRACT(MONTH FROM date_key) = 12;
```

**Jika Excel exclude issue dates**, maka:
- Database: SUM semua hari (termasuk issue dates)
- Excel: SUM hanya hari normal (exclude issue dates)
- **Perbedaan**: ~25.48 MWh = SUM dari issue dates

---

### 2. **Excel Exclude Weekend / Holiday** (Kemungkinan Sedang)

Excel mungkin hanya menghitung hari kerja (weekday), exclude weekend dan holiday.

**Cara Verifikasi**:
```sql
-- Cek weekend/holiday di Desember 2025
SELECT 
    date_key,
    day_type,
    energy_target_mwh,
    CASE 
        WHEN day_type IN ('Weekend', 'Holiday') THEN 'EXCLUDED'
        ELSE 'INCLUDED'
    END as excel_logic
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND year = 2025
    AND month = 12
ORDER BY date_key;
```

---

### 3. **Excel Punya Data Berbeda di Seed File** (Kemungkinan Rendah)

Excel mungkin menggunakan data yang berbeda atau versi seed file yang berbeda.

**Cara Verifikasi**:
- Bandingkan nilai daily di seed file dengan Excel
- Cek apakah ada missing dates di seed file
- Cek apakah ada data yang di-update setelah Excel di-export

---

### 4. **Excel Menggunakan Logika Perhitungan Berbeda** (Kemungkinan Sedang)

Excel mungkin menggunakan formula atau logika perhitungan yang berbeda, misalnya:
- Weighted average instead of SUM
- Exclude certain conditions
- Different rounding/precision

---

## 🔧 Query untuk Investigasi

Query lengkap tersedia di: `queries/check_target_monthly_mmki3_dec2025.sql`

### Query Utama:

1. **Cek Target Monthly di Database**
2. **Cek Target Monthly dari SUM Daily**
3. **Cek Target Monthly dari Seed File**
4. **Detail Daily Data untuk Desember 2025**
5. **Perbandingan Lengkap dengan Excel**
6. **Cek Issue Dates atau Missing Dates**
7. **Cek Apakah Excel Exclude Certain Days**
8. **Hitung Target dengan Exclude Issue Dates**

---

## 📋 Langkah-Langkah Investigasi

### Step 1: Cek Issue Dates

Jalankan query untuk melihat apakah ada issue dates di Desember 2025:

```sql
SELECT 
    date_key,
    site_name,
    issue_type,
    description
FROM "MMSR"."staging"."seed_issue_dates"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND EXTRACT(YEAR FROM date_key) = 2025
    AND EXTRACT(MONTH FROM date_key) = 12;
```

**Jika ada issue dates**, hitung SUM target untuk hari-hari tersebut:

```sql
SELECT 
    SUM(mstd.energy_target_mwh) as target_exclude_issue_dates
FROM "MMSR"."mart"."mart_simulation_targets_daily" mstd
LEFT JOIN "MMSR"."staging"."seed_issue_dates" id
    ON mstd.date_key = id.date_key
    AND mstd.site_name = id.site_name
WHERE mstd.site_name LIKE '%MMKI%Phase 3%'
    AND mstd.year = 2025
    AND mstd.month = 12
    AND id.date_key IS NULL;  -- Exclude issue dates
```

**Jika hasilnya mendekati 418.3944483**, berarti Excel exclude issue dates.

---

### Step 2: Cek Weekend/Holiday

Cek apakah Excel exclude weekend/holiday:

```sql
SELECT 
    day_type,
    COUNT(*) as days_count,
    SUM(energy_target_mwh) as target_sum
FROM "MMSR"."mart"."mart_simulation_targets_daily"
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND year = 2025
    AND month = 12
GROUP BY day_type;
```

---

### Step 3: Bandingkan dengan Excel Detail

Jika memungkinkan, bandingkan nilai daily di Excel dengan database untuk melihat hari mana yang berbeda.

---

## ✅ Rekomendasi

### Jika Excel Exclude Issue Dates:

1. **Option 1: Update Excel** (Recommended)
   - Gunakan nilai dari database (443.875082) yang include semua hari
   - Atau tambahkan kolom terpisah untuk "Target (Exclude Issue Dates)"

2. **Option 2: Update Database**
   - Buat kolom baru `energy_target_monthly_exclude_issue_dates_mwh`
   - Hitung dengan exclude issue dates untuk matching dengan Excel

### Jika Excel Exclude Weekend/Holiday:

1. **Clarify dengan Business**: Apakah target monthly seharusnya include atau exclude weekend/holiday?
2. **Document**: Dokumentasikan logika perhitungan yang benar
3. **Align**: Pastikan database dan Excel menggunakan logika yang sama

---

## 📝 Catatan

- **Database value (443.875082)** adalah SUM dari semua daily target di Desember 2025
- **Excel value (418.3944483)** kemungkinan exclude issue dates atau kondisi tertentu
- **Perbedaan 25.48 MWh** perlu diinvestigasi lebih lanjut untuk menentukan logika yang benar

---

## 🔗 Related Files

- Query: `queries/check_target_monthly_mmki3_dec2025.sql`
- Report: `reports/MONTHLY_TARGET_INCONSISTENCY_FIX.md`
- Model: `dbt/models/marts/mart_simulation_targets_daily.sql`
- Model: `dbt/models/marts/mart_simulation_targets_monthly.sql`

---

**Last Updated**: 2025-01-XX

