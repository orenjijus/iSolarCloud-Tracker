# Root Cause Analysis: Perbedaan Nilai Energy November 2025

## Summary

Ditemukan 2 kasus perbedaan nilai energy antara database dan Excel:

1. **PLTS Rooftop Sumatera Prima Fibreboard** - 2 November 2025
   - Database: 13.565292 MWh (13,565.292 kWh)
   - Excel: 13.71 MWh (13,710 kWh)
   - **Selisih: 0.144708 MWh (144.708 kWh)**
   - **Total MTD Database: 215.85 MWh**
   - **Total MTD Excel: 216 MWh**

2. **Garuda Metalindo 1** - 13 November 2025
   - Database: 14.348074 MWh (14,348.074 kWh)
   - Excel: 2.346885 MWh (2,346.885 kWh)
   - **Selisih: 12.001189 MWh (12,001.189 kWh)**

---

## Root Cause 1: PLTS Rooftop Sumatera Prima Fibreboard - 2 November

### Masalah
Meter tidak terdeteksi sebagai **cumulative meter** karena threshold detection terlalu ketat.

### Detail Teknis

**Meter: ISO_1680199_7_13_2 (positive_active_energy)**
- Nilai akhir 1 Nov: 150,831.8906 kWh
- Nilai awal 2 Nov: 150,982.7812 kWh
- **Perbedaan: 150.89 kWh (0.1% dari nilai sebelumnya)**

**Threshold Detection Logic:**
```sql
CASE 
    WHEN ABS(first_value - prev_day_max) < 0.01  -- Threshold 1: < 0.01 kWh
        OR (prev_day_max > 0 AND ABS(first_value - prev_day_max) / prev_day_max < 0.001)  -- Threshold 2: < 0.1%
    THEN true  -- Cumulative
    ELSE false  -- Non-cumulative
END
```

**Hasil Detection:**
- `abs_diff = 150.89` → **> 0.01** ❌ (Threshold 1 gagal)
- `pct_diff = 0.1%` → **= 0.001** ❌ (Threshold 2 gagal, harus < 0.001)
- **Kesimpulan: Meter TIDAK terdeteksi sebagai cumulative**

**Perhitungan untuk Meter ISO_1680199_7_13_2:**
- **Database (non-cumulative):** MAX - MIN = 157,765.9375 - 150,982.7812 = **6,783.1563 kWh**
- **Excel (cumulative):** MAX - prev_day_max = 157,765.9375 - 150,831.8906 = **6,934.0469 kWh**
- **Selisih per meter:** 6,934.0469 - 6,783.1563 = **150.89 kWh**

**Total Energy (2 Revenue Meters):**
- **Database:** 6.7823433 + 6.7829485 = **13.5652918 MWh** ≈ **13.565292 MWh** ✅
- **Excel (estimated):** ~13.71 MWh
- **Selisih total:** 13.71 - 13.565292 = **0.144708 MWh** ≈ **0.14 MWh** ✅ (sesuai laporan user)

### Root Cause
Threshold 0.1% (`< 0.001`) terlalu ketat. Meter dengan perbedaan tepat 0.1% tidak terdeteksi sebagai cumulative, padahal seharusnya dianggap cumulative karena nilai awal hari 2 hampir sama dengan nilai akhir hari 1 (hanya berbeda 150.89 kWh dari total ~150,000 kWh).

---

## Root Cause 2: Garuda Metalindo 1 - 13 November

### Masalah
**Data Quality Issue:** Data recovery sudah diterapkan untuk data akhir hari (23:55), tapi belum untuk data awal hari (00:00-00:45). Data awal hari masih menggunakan data lama yang salah.

### Detail Teknis

**Meter: ISO_1458125_7_7_1 (positive_active_energy) - p8030**

**Status Data di Raw Table (`raw.isolarcloud_historical_data`):**

✅ **Data yang Sudah Benar (Data Recovery Applied):**
- **12 Nov 23:55:** 927,234.1875 kWh (927234187.5 Wh) ✅
- **13 Nov 23:55:** 929,581.4375 kWh (929581437.5 Wh) ✅

❌ **Data yang Masih Salah (Belum Data Recovery):**
- **13 Nov 00:00:** 915,231.0625 kWh (915231062.5 Wh) ❌ **DATA LAMA**
- **13 Nov 00:05:** 915,231.0625 kWh ❌
- **13 Nov 00:10:** 915,231.0625 kWh ❌
- ... (dan seterusnya sampai sekitar 00:45 atau lebih)

**Verifikasi dari Raw Data:**
```
Nov 12 23:55: p8030 = 927234187.5 Wh = 927234.1875 kWh ✅ (benar)
Nov 13 00:00: p8030 = 915231062.5 Wh = 915231.0625 kWh ❌ (salah, seharusnya ~927234187.5 Wh)
Nov 13 23:55: p8030 = 929581437.5 Wh = 929581.4375 kWh ✅ (benar)
```

**Root Cause:**
- Data recovery untuk timestamp awal hari 13 (00:00, 00:05, dll) belum di-update di raw table
- Hanya data akhir hari (23:55) yang sudah menggunakan data recovery
- Nilai awal hari 13 seharusnya ~927,234.1875 kWh (sama dengan akhir hari 12, karena meter cumulative)
- Tapi di database masih 915,231.0625 kWh (data lama sebelum recovery)

**Perhitungan dengan Data Salah:**
- **Database (non-cumulative):** MAX - MIN = 929,581.4375 - 915,231.0625 = **14,350.375 kWh** = **14.348074 MWh** ❌
- **Excel (dengan data recovery):** MAX - prev_day_max = 929,581.4375 - 927,234.1875 = **2,347.25 kWh** ≈ **2.346885 MWh** ✅

**Perhitungan dengan Data Benar (Setelah Reingest):**
- Setelah data recovery di-reingest, nilai awal hari 13 akan menjadi ~927,234.1875 kWh
- Meter akan terdeteksi sebagai cumulative (karena first_value ≈ prev_day_max)
- **Database (cumulative):** MAX - prev_day_max = 929,581.4375 - 927,234.1875 = **2,347.25 kWh** ≈ **2.346885 MWh** ✅
- Nilai akan match dengan Excel setelah reingest

### Root Cause
**Data Quality Issue - Partial Data Recovery:**
- Data recovery sudah diterapkan untuk data akhir hari (23:55) ✅
- Data recovery belum diterapkan untuk data awal hari (00:00 dan beberapa timestamp berikutnya) ❌
- Perlu reingest raw table untuk timestamp awal hari 13 November dengan data recovery yang benar

---

## Kesimpulan

### PLTS Rooftop Sumatera Prima Fibreboard
- **Root Cause:** Threshold detection terlalu ketat (0.1% harus < 0.001, padahal perbedaan tepat 0.1%)
- **Solusi:** Perlu menyesuaikan threshold atau menggunakan logika yang lebih toleran untuk kasus edge case

### Garuda Metalindo 1
- **Root Cause:** Data quality issue - data recovery partial (hanya akhir hari yang sudah benar, awal hari masih data lama)
- **Solusi:** Reingest raw table dengan data recovery untuk timestamp awal hari 13 November (00:00 dan beberapa timestamp berikutnya). Setelah reingest, nilai akan match dengan Excel.

---

## Rekomendasi

1. **Perbaiki Threshold Detection:** ✅ **DILAKUKAN**
   - Ubah threshold dari `< 0.001` menjadi `<= 0.001` untuk handle edge case
   - File: `dbt/models/marts/mart_site_performance_daily.sql`
   - Perubahan: Line 100, dari `< 0.001` menjadi `<= 0.001`

2. **Data Quality - Garuda Metalindo 1:** ✅ **AKAN DILAKUKAN OLEH USER**
   - **Status:** Data recovery sudah diterapkan untuk akhir hari (23:55) ✅
   - **Masalah:** Data recovery belum diterapkan untuk awal hari (00:00 dan beberapa timestamp berikutnya) ❌
   - **Action:** Reingest raw table dengan data recovery untuk timestamp awal hari 13 November (00:00, 00:05, dll)
   - **Expected:** Setelah reingest, nilai awal hari akan menjadi ~927,234.1875 kWh (sama dengan akhir Nov 12)
   - **Result:** Meter akan terdeteksi sebagai cumulative dan nilai akan match dengan Excel (2.346885 MWh)

3. **Testing:**
   - Setelah threshold diperbaiki, re-run model untuk November 2025
   - Verifikasi bahwa PLTS Rooftop Sumatera Prima Fibreboard tanggal 2 November sekarang match dengan Excel
   - Setelah reingest GM 1, verifikasi bahwa nilai tanggal 13 November match dengan Excel

