# How to Add Device to Sensor Config

**Purpose**: When devices are synced from isolarcloud harvester, they go to `raw.isolarcloud_devices` but need to be manually added to `seed_sensor_config.csv`.

---

## Step-by-Step Guide

### Step 1: Check Raw Database for New Devices

Run query to find devices not yet in sensor_config:
```sql
-- File: queries/check_shoetown_all_devices_raw.sql
-- Section 4: DEVICES NOT IN CONFIG (PRIORITY - NEEDS ACTION)
```

This will show:
- `device_ps_key`: The key from raw database
- `suggested_device_id`: Format to use in sensor_config (ISO_<device_ps_key>)
- `device_name`: Name of the device
- `grid_connection_date`: When device was activated
- `suggested_sensor_type`: Suggested sensor type (POA/GHI/Weather)

### Step 2: Identify Device Details

From query results, note:
- **device_ps_key**: e.g., `1479456_5_XX_X`
- **device_name**: e.g., `Meteo Station16`
- **sensor_type**: POA, GHI, or Weather
- **capacity**: Need to verify (check device specs or field team)

### Step 3: Add to seed_sensor_config.csv

Open file: `dbt/seeds/seed_sensor_config.csv`

Add new row with format:
```
Source;site_id;dev_name;device_id;sensor_type;sensor_capacity
```

**Format Details**:
- `Source`: `iSolarCloud` (for isolarcloud devices) or `FusionSolar` (for fusionsolar devices)
- `site_id`: Power station ID, e.g., `1479456`
- `dev_name`: Device name from raw database, e.g., `Meteo Station16`
- `device_id`: Format `ISO_<device_ps_key>`, e.g., `ISO_1479456_5_XX_X`
- `sensor_type`: `POA`, `GHI`, or `Weather` (leave empty if not a sensor)
- `sensor_capacity`: Capacity in kWp (for POA sensors), leave empty for GHI/Weather

**Example**:
```csv
iSolarCloud;1479456;Meteo Station16;ISO_1479456_5_XX_X;POA;928
```

### Step 4: Verify Capacity Value

For POA sensors, capacity is critical for weighted average calculation. Verify:
- Check device specifications
- Check with field team
- Compare with similar sensors at same site
- If unsure, can start with estimated value and adjust based on validation results

### Step 5: Reload Seed and Re-run Models

After updating sensor_config:
```bash
# Reload seed
dbt seed --select seed_sensor_config

# Re-run models that use sensor config
dbt run --select mart_sensor_daily mart_site_performance_daily
```

### Step 6: Validate

Run validation queries to verify:
```sql
-- Check if device now appears in sensor config
SELECT * FROM staging.seed_sensor_config 
WHERE device_id = 'ISO_<device_ps_key>';

-- Check if device data appears in mart_sensor_daily
SELECT * FROM mart.mart_sensor_daily
WHERE device_id = 'ISO_<device_ps_key>'
ORDER BY date_key DESC
LIMIT 10;
```

---

## Common Scenarios

### Scenario 1: New POA Sensor (e.g., Meteo Station16)

```csv
iSolarCloud;1479456;Meteo Station16;ISO_1479456_5_XX_X;POA;928
```

**Notes**:
- Must have `sensor_type = POA`
- Must have `sensor_capacity` value (in kWp)
- Capacity is used for weighted average calculation

### Scenario 2: New GHI Sensor (Pyrano)

```csv
iSolarCloud;1479456;SLI-PYR-New;ISO_1479456_5_XX_X;GHI;
```

**Notes**:
- `sensor_type = GHI`
- `sensor_capacity` is empty (GHI sensors don't have capacity)

### Scenario 3: Weather Station (No Sensor Type)

```csv
iSolarCloud;1479456;Weather Station1;ISO_1479456_5_XX_X;;
```

**Notes**:
- `sensor_type` is empty
- `sensor_capacity` is empty
- Used for weather data (temperature, wind, etc.) but not for POA/GHI calculation

---

## Troubleshooting

### Device Not Appearing After Adding to Config

1. **Check device_id format**: Must be `ISO_<device_ps_key>` (exact match)
2. **Reload seed**: Run `dbt seed --select seed_sensor_config`
3. **Check for typos**: Verify device_ps_key matches exactly
4. **Check site_id**: Must match power station ID

### Capacity Value Unknown

1. Check similar sensors at same site
2. Check device specifications/documentation
3. Contact field team
4. Can estimate and adjust based on validation results

### Device Type Confusion

- **POA**: Plane of Array irradiance sensor (has capacity)
- **GHI**: Global Horizontal Irradiance sensor (no capacity)
- **Weather**: Weather station (temperature, wind, etc.) - no capacity

---

## Related Files

- Sensor Config: `dbt/seeds/seed_sensor_config.csv`
- Raw Devices: `raw.isolarcloud_devices` (database table)
- Check Devices Query: `queries/check_shoetown_all_devices_raw.sql`
- Validation: `queries/validate_shoetown_sensor_mapping_and_poa.sql`

---

**Last Updated**: 2025-01-XX

