# Analisis: Power Available Hours untuk MMKI III - 2025-12-13

## Status: ✅ FIXED

**Tanggal Fix**: 2025-12-17  
**Status**: Masalah sudah teratasi dengan perubahan LEFT JOIN di `fact_inverter_calculations_5min`

---

## Masalah
Power Available Hours untuk MMKI III pada tanggal 13 Desember 2025 hanya menunjukkan 8.67 jam, padahal seharusnya sekitar 12 jam.

## Root Cause (Ditemukan)
**Masalah**: Data hilang di `mart.fact_site_calculations_5min` untuk periode **14:00-16:00** karena **INNER JOIN** antara `fact_inverter_calculations_5min` dan `fact_sensor_calculations_5min` (untuk MIT).

**Detail**:
- Data mentah dan staging **ADA** untuk periode 14:00-16:00
- Data inverter di `mart_inverter_performance_5min` **ADA** (288 records per jam)
- Data sensor/MIT di `fact_sensor_calculations_5min` **TIDAK ADA** untuk periode tersebut
- **INNER JOIN** menghilangkan semua data inverter ketika MIT data tidak ada
- Akibatnya: 36 interval (3 jam) hilang dari calculation

## Logika yang Benar
Berdasarkan klarifikasi:
- **Power Available Hours** = menghitung **SEMUA** interval dimana inverter menyala (power > 0), **TIDAK peduli MIT = 0 atau 1**
- **Unavailability Hours** = hanya dihitung ketika **MIT = 1** (ada sinar matahari) **DAN** inverter tidak menyala (power = 0)

## Solusi yang Diterapkan ✅

### Perubahan di `fact_inverter_calculations_5min.sql`

**Sebelum** (INNER JOIN):
```sql
FROM inverter_power ip
INNER JOIN site_mit sm  -- INNER JOIN: only count when BOTH inverter and MIT data exist
    ON ip.timestamp = sm.timestamp
    AND ip.site_id = sm.site_id
```

**Sesudah** (LEFT JOIN):
```sql
FROM inverter_power ip
LEFT JOIN site_mit sm  -- LEFT JOIN: include inverter data even when MIT data is missing
    ON ip.timestamp = sm.timestamp
    AND ip.site_id = sm.site_id
```

**MIT Default**: `COALESCE(sm.mit, 0)` - Default ke 0 jika data sensor tidak ada (no sun = MIT 0)

### Hasil Setelah Fix
- **Power Available Hours**: Meningkat dari **8.67 jam** menjadi **12.50 jam** ✅
- **Data untuk periode 14:00-16:00**: Sekarang muncul lengkap (36 interval) ✅
- **Total intervals**: Meningkat dari 119 menjadi 165 intervals ✅

## Kode yang Benar
Kode di `mart_site_performance_daily.sql` sudah benar:
```sql
-- Power Available Hours: menghitung semua interval dimana inverter menyala
SUM(fsc.power_available_ratio) * 5.0 / 60.0 as power_available_hours

-- Unavailability Hours: hanya ketika MIT=1 dan inverter tidak menyala
SUM(fsc.unavailability_ratio) * 5.0 / 60.0 as unavailability_hours
```

Dan di `fact_site_calculations_5min.sql`:
```sql
-- power_available_ratio = available_inverters / total_inverters (selalu dihitung)
power_available_ratio = available_inverters::DECIMAL / total_inverters::DECIMAL

-- unavailability_ratio = hanya ketika MIT=1
unavailability_ratio = CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END
```

## Lesson Learned
- **LEFT JOIN lebih tepat** untuk join dengan data sensor/MIT karena:
  - Data inverter tidak boleh hilang hanya karena sensor data tidak ada
  - MIT = 0 adalah default yang masuk akal (no sensor data = assume no sun)
  - Power Available Hours harus menghitung semua interval dimana inverter menyala, tidak peduli MIT

## Catatan
- ✅ Masalah sudah teratasi
- ✅ Power Available Hours sekarang akurat (~12.50 jam)
- ✅ Data tidak hilang meskipun sensor data tidak tersedia

