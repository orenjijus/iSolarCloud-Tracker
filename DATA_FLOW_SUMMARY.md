# Data Flow Summary - Sensor & Meter

## Status: SUCCESS ✅

Data sensor dan meter sudah berhasil masuk dari staging → intermediate → mart tables.

---

## Data Flow Analysis

### 1. Staging Layer
- **stg_fusionsolar__perf_unpivoted**: 328.98M rows
- **stg_isolarcloud__perf_unpivoted**: 177.87M rows

### 2. Intermediate Layer
- **int_sensors_unified**: 21.75M rows
  - FusionSolar: 13.36M rows (44.04M before filtering)
  - iSolarCloud: 8.38M rows
  
- **int_meters_unified**: 55.15M rows
  - FusionSolar: 46.27M rows (44.04M before filtering)
  - iSolarCloud: 11.11M rows

### 3. Mart Layer
- **mart_sensor_measurements_5min**: 21.75M rows
  - FusionSolar: 13.36M rows
    - 45 unique assets
    - 6 unique metrics
    - Period: 2024-04-30 23:00 → 2025-09-18 06:55
  
  - iSolarCloud: 8.38M rows
    - 48 unique assets
    - 7 unique metrics
    - Period: 2024-10-13 12:10 → 2025-09-17 23:55

- **mart_meter_performance_5min**: 57.38M rows
  - FusionSolar: 46.27M rows
    - 30 unique assets
    - 20 unique metrics
    - Period: 2024-04-30 23:00 → 2025-09-18 06:55
  
  - iSolarCloud: 11.11M rows
    - 54 unique assets
    - 6 unique metrics
    - Period: 2024-10-10 16:15 → 2025-09-17 23:55

---

## Issues Fixed

### 1. Device Type Mismatch
**Problem**: Device type codes salah di intermediate models
- **Sensors**: `device_type = 2` (wrong) → `device_type = 5` (correct for iSolarCloud)
- **Sensors**: `dev_type_id = 2` (wrong) → `dev_type_id = 10` (correct for FusionSolar)  
- **Meters**: `dev_type_id = 7` (wrong) → `dev_type_id = 17` (correct for FusionSolar)

### 2. Invalid Values (NaN)
**Problem**: Data dengan nilai -nan, nan, infinity menyebabkan error casting
**Solution**: Added filter di staging models untuk skip invalid values sebelum casting ke numeric

### 3. Asset ID Mismatch
**Problem**: JOIN ke dim_assets gagal karena asset_id format berbeda
- **dim_assets** menggunakan: `FS_` untuk FusionSolar dan `ISO_` untuk iSolarCloud
- **mart** sebelumnya menggunakan: `FUS_` untuk FusionSolar
**Solution**: Update JOIN logic di mart models untuk gunakan prefix yang benar

---

## Database Location

Semua data tersimpan di database PostgreSQL dengan schema `public`:

### Tables
- `stg_fusionsolar__perf_unpivoted`
- `stg_isolarcloud__perf_unpivoted`
- `int_sensors_unified`
- `int_meters_unified`
- `mart_sensor_measurements_5min`
- `mart_meter_performance_5min`
- `dim_assets`
- `dim_date_generated`

### Data Volume
- **Total Staging**: ~506M rows
- **Total Intermediate**: ~77M rows  
- **Total Mart**: ~79M rows

---

## Next Steps

1. Data sudah siap untuk reporting/visualization
2. Monitor incremental load performance
3. Consider partitioning jika volume data terus bertambah

---

## Summary

✅ **Staging**: 506M rows  
✅ **Intermediate**: 77M rows (77% data ter-filter)  
✅ **Mart**: 79M rows  
✅ **System field**: Berfungsi dengan benar  
✅ **All joins**: Berhasil  

**Total Execution Time**: ~52 minutes untuk full rebuild

