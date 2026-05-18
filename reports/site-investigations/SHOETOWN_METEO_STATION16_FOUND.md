# Meteo Station16 Found - Sensor Config Update

**Date**: 2025-01-XX  
**Status**: ✅ Device Found and Added to Config

---

## 🔍 Discovery

**Meteo Station16** ditemukan di `raw.isolarcloud_devices`:

- **device_ps_key**: `1479456_5_27_1`
- **device_name**: `Meteo Station16`
- **device_type**: 5 (环境监测仪 - Meteo Station)
- **grid_connection_date**: `2025-11-12 13:53:07` (12 November 2025)
- **dev_status**: 1 (active)
- **Status**: ❌ **NOT_IN_CONFIG** → ✅ **NOW ADDED**

---

## 📊 Comparison with SLI-IRR-3-F

| Sensor | device_ps_key | grid_connection_date | dev_status | Notes |
|--------|---------------|---------------------|------------|-------|
| **SLI-IRR-3-F** (OLD) | `1479456_5_17_1` | 2024-11-26 | 0 (inactive) | Old sensor, to be replaced |
| **Meteo Station16** (NEW) | `1479456_5_27_1` | 2025-11-12 | 1 (active) | Replacement sensor |

**Timeline Analysis**:
- SLI-IRR-3-F: Activated Nov 26, 2024
- Meteo Station16: Activated Nov 12, 2025
- **Gap**: ~11.5 months between sensors
- **Note**: User mentioned replacement date is Oct 3, 2025, but Meteo Station16 grid_connection_date is Nov 12, 2025. This might indicate:
  - Sensor was repositioned on Oct 3 but grid connection date updated later
  - Or there was a delay in activation
  - Need to verify with field team

---

## ✅ Action Taken

### Added to `seed_sensor_config.csv`:

```csv
iSolarCloud;1479456;Meteo Station16;1479456_5_27_1;POA;928
```

**Configuration Details**:
- **Source**: `iSolarCloud`
- **site_id**: `1479456` (Shoetown)
- **dev_name**: `Meteo Station16`
- **device_id**: `1479456_5_27_1`
- **sensor_type**: `POA`
- **sensor_capacity**: `928` kWp (sensor fisik sama dengan SLI-IRR-3-F, hanya reposisi)

---

## ⚠️ Important Notes

### 1. Capacity Value
- **Confirmed**: 928 kWp (sensor fisik sama dengan SLI-IRR-3-F yang juga 928 kWp, hanya reposisi)
- **Status**: ✅ **UPDATED** - Capacity sudah disesuaikan di config

### 2. Grid Connection Date Discrepancy
- **User mentioned**: Replacement date Oct 3, 2025
- **Database shows**: grid_connection_date = Nov 12, 2025
- **Possible explanations**:
  - Sensor repositioned Oct 3, but grid connection date updated later
  - Delay in activation
  - Different date meaning (reposition vs grid connection)
- **Action**: Verify with field team which date is correct for sensor replacement logic

### 3. Sensor Replacement Logic
- **Old sensor (SLI-IRR-3-F)**: Should be excluded after replacement date
- **New sensor (Meteo Station16)**: Should be included after activation date
- **Current query logic**: Uses grid_connection_date for filtering
- **If Oct 3 is correct**: May need to adjust query logic or add manual override

---

## 📋 Next Steps

1. [x] ✅ Add Meteo Station16 to sensor_config
2. [x] ✅ **CAPACITY CONFIRMED** (452.4 kWp) - sensor fisik sama, hanya reposisi
3. [x] ✅ **REPLACEMENT DATE CONFIRMED** (Oct 3 untuk reposisi, Nov 12 untuk grid connection date)
4. [ ] Reload seed: `dbt seed --select seed_sensor_config`
5. [ ] Re-run models: `dbt run --select mart_sensor_daily mart_site_performance_daily`
6. [ ] Validate POA calculation with Excel

---

## 🔗 Related Files

- Sensor Config: `dbt/seeds/seed_sensor_config.csv` (updated)
- Validation Query: `queries/export_shoetown_poa_daily_for_excel_validation.sql`
- Documentation: `reports/SHOETOWN_SENSOR_MAPPING_VALIDATION.md`

---

**Last Updated**: 2025-01-XX  
**Status**: Device added to config, capacity and date need verification

