# Shoetown Sensor Mapping Validation & POA Daily Validation

**Date**: 2025-01-XX  
**Site**: Shoetown Ligung Indonesia (1479456)  
**Status**: In Progress

---

## 📋 Sensor Repositioning Timeline

### POA Sensors

#### 1. SLI-IRR-1-Aold → SLI-IRR-1-A
- **Old Sensor**: `SLI-IRR-1-Aold` (device_id: `ISO_1479456_5_16_2`)
  - Capacity: 452.4 kWp
  - Deactivated: **October 1, 2025**
- **New Sensor**: `SLI-IRR-1-A` (device_id: `ISO_1479456_5_24_1`)
  - Capacity: 452.4 kWp (sensor fisik sama, hanya reposisi)
  - Activated: **October 3, 2025**
  - **Note**: Sensor fisik sama dengan old sensor, hanya reposisi

#### 2. SLI-IRR-2-Aold → SLI-IRR-2-A
- **Old Sensor**: `SLI-IRR-2-Aold` (device_id: `ISO_1479456_5_15_2`)
  - Capacity: 452.4 kWp
  - Deactivated: **October 1, 2025**
- **New Sensor**: `SLI-IRR-2-A` (device_id: `ISO_1479456_5_25_1`)
  - Capacity: 452.4 kWp (sensor fisik sama, hanya reposisi)
  - Activated: **October 3, 2025**
  - **Note**: Sensor fisik sama dengan old sensor, hanya reposisi

#### 3. SLI-IRR-3-F → Meteo Station16 ✅ **FOUND AND ADDED**
- **Old Sensor**: `SLI-IRR-3-F` (device_id: `ISO_1479456_5_17_1`)
  - Capacity: 928 kWp (sensor fisik sama, hanya reposisi)
  - Replacement date: **October 3, 2025**
- **New Sensor**: `Meteo Station16` (device_id: `ISO_1479456_5_27_1`)
  - **Status**: ✅ **ADDED TO CONFIG**
  - Capacity: 928 kWp (sensor fisik sama dengan SLI-IRR-3-F, hanya reposisi)
  - Grid connection date: November 12, 2025 (tapi reposisi terjadi Oct 3)
  - **Note**: Sensor fisik sama, hanya reposisi, jadi capacity tetap 928 kWp

### GHI Sensors (Pyrano)

#### 4. SLI-PYR-01-F → SLI-PYR
- **Old Sensor**: `SLI-PYR-01-F` (device_id: `ISO_1479456_5_21_1`)
  - Type: GHI
  - Deactivated: **October 3, 2025** (dicabut)
- **New Sensor**: `SLI-PYR` (device_id: `ISO_1479456_5_26_1`)
  - Type: GHI
  - Activated: **October 12, 2025** (baru nyala lagi setelah dicabut)

---

## 🔍 Validation Queries

### 1. Check Grid Connection Dates
Run this query to verify grid connection dates for all sensors:
```sql
-- File: queries/check_shoetown_sensor_grid_connection_dates.sql
```

### 2. Export Daily POA for Excel Comparison
Run this query to export data matching Excel format:
```sql
-- File: queries/export_shoetown_poa_daily_for_excel_validation.sql
```

### 3. Comprehensive Validation
Run this query for detailed sensor mapping and POA validation:
```sql
-- File: queries/validate_shoetown_sensor_mapping_and_poa.sql
```

---

## 📊 Expected Sensor Configuration After Repositioning

### Active POA Sensors (After Oct 3, 2025):
1. ✅ `SLI-IRR-1-A` (ISO_1479456_5_24_1) - 928 kWp
2. ✅ `SLI-IRR-2-A` (ISO_1479456_5_25_1) - 763.28 kWp
3. ⚠️ `SLI-IRR-4-F` (ISO_1479456_5_18_1) - 763.28 kWp
4. ⚠️ Replacement for `SLI-IRR-3-F` - **NEEDS VERIFICATION**

### Inactive POA Sensors (After Oct 1, 2025):
1. ❌ `SLI-IRR-1-Aold` (ISO_1479456_5_16_2) - Should be excluded
2. ❌ `SLI-IRR-2-Aold` (ISO_1479456_5_15_2) - Should be excluded
3. ❌ `SLI-IRR-3-F` (ISO_1479456_5_17_1) - Should be excluded after replacement

---

## ✅ Action Items

### Immediate Actions:
1. [x] **Query database** to check all devices in raw database
   - Run: `queries/check_shoetown_all_devices_raw.sql`
   - This will show:
     - All devices for Shoetown (1479456)
     - Which devices are already in sensor_config (✅ IN_CONFIG)
     - Which devices are NOT in sensor_config (❌ NOT_IN_CONFIG - NEEDS TO BE ADDED)
     - Focus on Meteo Stations and devices with "16" in name
2. [ ] **Identify Meteo Station16 or replacement device**:
   - Check query results section 2 (Meteo Stations) and section 3 (devices with "16")
   - Look for devices with grid_connection_date >= 2025-10-03 (replacement date)
   - Note the device_ps_key and device_name
3. [ ] **Add device to sensor_config** if found:
   - Open: `dbt/seeds/seed_sensor_config.csv`
   - Add new row with format:
     ```
     iSolarCloud;1479456;<device_name>;ISO_<device_ps_key>;POA;<capacity>
     ```
   - Example if device_ps_key is `1479456_5_XX_X`:
     ```
     iSolarCloud;1479456;Meteo Station16;ISO_1479456_5_XX_X;POA;928
     ```
   - **Note**: Capacity value needs to be verified (check with field team or device specs)
4. [ ] **Run validation queries**:
   - Export daily POA: `queries/export_shoetown_poa_daily_for_excel_validation.sql`
   - Compare with Excel file: `shoetown_poa_daily.xlsx`

### Validation Steps:
1. [ ] Compare grid connection dates with expected timeline
2. [ ] Verify sensor capacities match Excel
3. [ ] Compare daily POA per sensor (SLI_IRR_1_A, SLI_IRR_2_A, SLI_IRR_3_F, SLI_IRR_4_F)
4. [ ] Compare weighted average POA with Excel
5. [ ] Check for date ranges where sensors should be excluded/included

---

## 📝 Notes

- **Sensor Replacement Logic**: The current system uses `grid_connection_date` to filter data at ingestion time. If old sensors stop sending data after deactivation, they won't appear in calculations. However, if both old and new sensors have data for overlapping periods, the validation query filters them based on dates.

- **Excel Comparison**: The export query (`export_shoetown_poa_daily_for_excel_validation.sql`) filters sensors based on replacement timeline to match expected Excel behavior.

- **Meteo Station16**: This sensor needs verification. Check:
  - Database for device existence
  - Grid connection date
  - POA capacity value
  - Whether it's actually Meteo Station4 or a different device

---

## 🔗 Related Files

- Sensor Config: `dbt/seeds/seed_sensor_config.csv`
- Sensor Site Mapping: `dbt/seeds/seed_sensor_site_mapping.csv`
- Validation Queries: `queries/validate_shoetown_sensor_mapping_and_poa.sql`
- Export Query: `queries/export_shoetown_poa_daily_for_excel_validation.sql`
- Grid Connection Dates: `queries/check_shoetown_sensor_grid_connection_dates.sql`

---

**Last Updated**: 2025-01-XX  
**Status**: Waiting for database query results and Meteo Station16 verification

