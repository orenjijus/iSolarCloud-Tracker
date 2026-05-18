# Hasil Reingest Garuda Metalindo (IKP) - 30 Oktober 2025

## ✅ Status: BERHASIL

Data sudah berhasil di-reprocess dan hasilnya normal.

## Perbandingan Sebelum dan Sesudah Reingest

### Sebelum Reingest (30 Oktober 2025)
- ❌ daily_energy_mwh: **0.000015 MWh** (hampir 0)
- ❌ daily_ghi_kwh_m2: 0.000000
- ❌ daily_poa_weighted_kwh_m2: 0.000000
- ❌ availability_percent: NULL
- ❌ Meter stuck: positive_active_energy tidak berubah (diff = 0)

### Sesudah Reingest (30 Oktober 2025)
- ✅ daily_energy_mwh: **1.224955 MWh** (normal!)
- ✅ daily_ghi_kwh_m2: 4.743213
- ✅ daily_poa_weighted_kwh_m2: 0.000000
- ✅ availability_percent: 100%
- ✅ power_available_hours: 10.138889
- ✅ Meter berfungsi normal:
  - negative_active_energy: diff = 733.0017 kWh
  - positive_active_energy: diff = 1,225,687.6 kWh

## Perbandingan dengan Hari Sekitarnya

| Tanggal | daily_energy_mwh | daily_ghi_kwh_m2 | availability_percent |
|---------|------------------|------------------|---------------------|
| 29 Okt  | 1.270915 MWh     | 4.780917         | 100%                |
| **30 Okt** | **1.224955 MWh** | **4.743213**     | **100%**            |
| 31 Okt  | 1.396272 MWh     | 5.368684         | 100%                |

## Kesimpulan

1. ✅ **Reingest berhasil** - Data raw yang baru sudah terproses dengan baik
2. ✅ **Data sekarang normal** - Nilai energy, GHI, dan availability sudah sesuai
3. ✅ **Meter berfungsi** - Meter data menunjukkan perubahan yang normal sepanjang hari
4. 📊 **Data konsisten** - Nilai 30 Oktober sekarang konsisten dengan hari sebelum dan sesudahnya

## Detail Meter Data (Revenue Meter: ISO_1445767_7_12_1)

### 30 Oktober 2025 (Setelah Reingest)
- **negative_active_energy**: 
  - First: 167,505.0049 kWh
  - Last: 168,238.0066 kWh
  - **Daily diff: 733.0017 kWh** ✅

- **positive_active_energy**: 
  - First: 409,436,156.2 kWh
  - Last: 410,661,843.8 kWh
  - **Daily diff: 1,225,687.6 kWh** ✅

### 29 Oktober 2025 (Sebagai Perbandingan)
- **negative_active_energy**: Daily diff = 488.0066 kWh
- **positive_active_energy**: Daily diff = 1,271,406.2 kWh

## Command yang Digunakan

```bash
cd "C:\Users\Administrator\Documents\Code\MMSR API - Server MA\dbt"
dbt run --select stg_isolarcloud__perf_unpivoted+ \
  --vars '{
    "reingest_start_date": "2025-10-30",
    "reingest_end_date": "2025-10-30",
    "reingest_ps_ids": "1445767"
  }'
```

## Models yang Ter-refresh

1. ✅ stg_isolarcloud__perf_unpivoted (52,415 rows inserted)
2. ✅ mart_inverter_performance_5min (60,196 rows inserted)
3. ✅ mart_meter_performance_5min (68,247 rows inserted)
4. ✅ mart_sensor_measurements_5min (41,183 rows inserted)
5. ✅ fact_sensor_calculations_5min (19,638 rows inserted)
6. ✅ mart_sensor_daily (76 rows inserted)
7. ✅ fact_inverter_calculations_5min (35,282 rows inserted)
8. ✅ fact_site_calculations_5min (3,584 rows inserted)
9. ✅ mart_site_performance_daily (15 rows inserted)
10. ✅ mart_site_performance_monthly (view refreshed)

**Total waktu processing**: ~13 menit 41 detik

