# Fix untuk Issue: Sensor dan Meter Tidak Ada Data Masuk

## Problem
Data untuk sensor dan meter tidak muncul di mart tables (`mart_sensor_measurements_5min` dan `mart_meter_performance_5min`).

## Root Cause
Device type filter di intermediate models menggunakan kode yang salah:

### Sensor (int_sensors_unified.sql)
- ❌ Sebelum: `device_type = 2` (iSolarCloud) - Tidak ada device dengan type ini
- ✅ Sekarang: `device_type = 5` (iSolarCloud) - Meteo Station
- ❌ Sebelum: `dev_type_id = 2` (FusionSolar) - Tidak ada device dengan type ini  
- ✅ Sekarang: `dev_type_id = 10` (FusionSolar) - Meteo Station

### Meter (int_meters_unified.sql)
- ✅ iSolarCloud: `device_type = 7` - Sudah benar
- ❌ Sebelum: `dev_type_id = 7` (FusionSolar) - Salah
- ✅ Sekarang: `dev_type_id = 17` (FusionSolar) - Meter

## Device Type Codes Reference

### iSolarCloud (dari stg_isolarcloud__devices.sql)
- `device_type = 1`: Inverter
- `device_type = 5`: Meteo Station (Sensor/Weather Station)
- `device_type = 7`: Meter

### FusionSolar (dari stg_fusionsolar__devices.sql)
- `dev_type_id = 1`: Inverter
- `dev_type_id = 10`: Meteo Station (Sensor/Weather Station)
- `dev_type_id = 17`: Meter

## Files Modified
1. `models/intermediate/int_sensors_unified.sql`
   - Line 19: Changed `device_type = 2` → `device_type = 5` for iSolarCloud
   - Line 35: Changed `dev_type_id = 2` → `dev_type_id = 10` for FusionSolar

2. `models/intermediate/int_meters_unified.sql`
   - Line 35: Changed `dev_type_id = 7` → `dev_type_id = 17` for FusionSolar

## Next Steps
1. Rebuild intermediate models:
   ```bash
   dbt run --select int_sensors_unified int_meters_unified
   ```

2. Rebuild mart models:
   ```bash
   dbt run --select mart_sensor_measurements_5min mart_meter_performance_5min
   ```

3. Verify data in mart tables:
   ```sql
   SELECT COUNT(*) FROM mmsr_solar_data.mart_sensor_measurements_5min;
   SELECT COUNT(*) FROM mmsr_solar_data.mart_meter_performance_5min;
   ```

## Expected Result
Data sensor dan meter sekarang akan muncul di mart tables karena filter device type sudah benar.

