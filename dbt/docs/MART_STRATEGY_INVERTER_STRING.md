# Mart Strategy: Inverter & String Performance

## Current Status

✅ **Completed:**
- `mart_meter_performance_5min` - 75.4M rows in `mart` schema
- `mart_sensor_measurements_5min` - 25.6M rows in `mart` schema
- `mart_simulation_targets_daily` - 3,286 rows in `mart` schema

🚧 **Pending:**
- `mart_inverter_performance_5min` - Needs new seed for inverter-string-POA mapping
- `mart_string_performance_5min` - Needs new seed for inverter-string-POA mapping
- `mart_inverter_performance_daily` - Depends on 5min mart
- `mart_string_performance_daily` - Depends on 5min mart

---

## Problem Statement

To properly analyze inverter and string performance, we need to:

1. **Map Strings to Inverters**: Know which strings belong to which inverter
2. **Map POA Sensors**: Associate the correct Plane of Array (POA) irradiance sensor with each inverter/string
3. **Calculate Performance Ratios**: Compare actual performance vs. expected based on irradiance

Currently, the inverter and string marts only have basic metrics but lack:
- POA irradiance association
- String-to-inverter relationships
- Performance ratio calculations

---

## Proposed Solution: New Seed File

### `seed_inverter_string_poa_mapping.csv`

**Purpose**: Maps inverters to their strings and associated POA sensors

**Proposed Structure:**

```csv
Source,site_id,inverter_id,inverter_name,string_number,string_id,poa_sensor_id,poa_sensor_name,azimuth_degrees,tilt_degrees,panel_count,panel_capacity_w,notes
```

**Columns:**
- `Source` - 'FusionSolar' or 'iSolarCloud'
- `site_id` - Site/plant identifier
- `inverter_id` - Inverter device ID (e.g., `dev_id` for FusionSolar, `device_ps_key` for iSolarCloud)
- `inverter_name` - Human-readable inverter name
- `string_number` - String number (1, 2, 3, ...)
- `string_id` - Unique string identifier (e.g., `INV001_STR01`)
- `poa_sensor_id` - Device ID of the POA sensor (from `seed_sensor_config`)
- `poa_sensor_name` - Name of the POA sensor
- `azimuth_degrees` - String orientation (0-360)
- `tilt_degrees` - String tilt angle
- `panel_count` - Number of panels in string
- `panel_capacity_w` - Total capacity of string in watts
- `notes` - Additional information

**Example Data:**

```csv
Source,site_id,inverter_id,inverter_name,string_number,string_id,poa_sensor_id,poa_sensor_name,azimuth_degrees,tilt_degrees,panel_count,panel_capacity_w,notes
FusionSolar,NE=50488260,DEV001,Inverter-01,1,INV001_STR01,EM01102287046729,EMI-1 [NORTH],0,15,20,6000,North-facing string
FusionSolar,NE=50488260,DEV001,Inverter-01,2,INV001_STR02,EM02102287046729,EMI-2 [SOUTH],180,15,20,6000,South-facing string
iSolarCloud,1445767,1445767_1_1_1,Inverter1,1,INV1_STR01,1445767_5_9_1,IrradianceSensor_PVTemp(Azimuth 152.6),152.6,15,22,6600,POA sensor
iSolarCloud,1445767,1445767_1_1_1,Inverter1,2,INV1_STR02,1445767_5_9_1,IrradianceSensor_PVTemp(Azimuth 152.6),152.6,15,22,6600,Same POA as STR01
```

---

## Updated Mart Models

### 1. `mart_inverter_performance_5min` (Enhanced)

**New Features:**
- Join with `seed_inverter_string_poa_mapping` to get POA sensor for each inverter
- Join with `mart_sensor_measurements_5min` to get POA irradiance
- Calculate performance metrics:
  - `performance_ratio` = (actual_power / expected_power) * 100
  - `expected_power` = (irradiance * capacity * efficiency_factor)
  - `specific_yield` = energy / capacity

**Enhanced Columns:**
```sql
SELECT 
    -- Existing columns
    i.timestamp_5min,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    i.metric_id,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    i.metric_value,
    
    -- New columns from mapping
    ism.poa_sensor_id,
    ism.poa_sensor_name,
    ism.string_count,  -- Count of strings per inverter
    
    -- New calculated columns
    poa.irradiance_w_m2,
    poa.ambient_temp_c,
    poa.pv_temp_c,
    CASE 
        WHEN m.unified_name = 'inv_active_power' AND poa.irradiance_w_m2 > 0
        THEN (i.metric_value / NULLIF(poa.irradiance_w_m2 * da.capacity_kw * 0.001, 0)) * 100
        ELSE NULL
    END as performance_ratio
FROM {{ ref('int_inverters_unified_5min') }} i
LEFT JOIN {{ ref('dim_assets') }} da ON ...
LEFT JOIN {{ ref('seed_inverter_string_poa_mapping') }} ism 
    ON da.asset_id = CONCAT(
        CASE WHEN i.system = 'fusionsolar' THEN 'FS' ELSE 'ISO' END, 
        '_', i.device_ps_key
    )
    AND ism.string_number = 1  -- Use first string's POA for inverter-level
LEFT JOIN {{ ref('mart_sensor_measurements_5min') }} poa
    ON poa.asset_id = ism.poa_sensor_id
    AND poa.timestamp = i.timestamp_5min
    AND poa.metric_name = 'irradiance'
```

### 2. `mart_string_performance_5min` (New/Enhanced)

**Features:**
- One row per string per timestamp
- Direct POA sensor association
- String-level performance metrics
- Comparison with other strings on same inverter

**Structure:**
```sql
SELECT 
    s.timestamp_5min,
    ism.string_id,
    ism.inverter_id,
    ism.inverter_name,
    da.site_name,
    da.system,
    dd.date_key,
    s.metric_id,
    m.unified_name as metric_name,
    s.metric_value,
    
    -- String configuration
    ism.string_number,
    ism.azimuth_degrees,
    ism.tilt_degrees,
    ism.panel_count,
    ism.panel_capacity_w,
    
    -- POA sensor data
    ism.poa_sensor_id,
    ism.poa_sensor_name,
    poa.irradiance_w_m2,
    poa.ambient_temp_c,
    poa.pv_temp_c,
    
    -- Calculated metrics
    CASE 
        WHEN m.unified_name LIKE 'string_%_voltage' 
        THEN s.metric_value * NULLIF(
            (SELECT metric_value FROM s WHERE metric_id = corresponding_current_id), 0
        )
        ELSE NULL
    END as string_power_w,
    
    CASE 
        WHEN string_power_w IS NOT NULL AND poa.irradiance_w_m2 > 0
        THEN (string_power_w / NULLIF(poa.irradiance_w_m2 * ism.panel_capacity_w * 0.001, 0)) * 100
        ELSE NULL
    END as string_performance_ratio
FROM {{ ref('int_strings_unified_5min') }} s
LEFT JOIN {{ ref('seed_inverter_string_poa_mapping') }} ism 
    ON s.device_ps_key = ism.inverter_id
    AND s.string_number = ism.string_number  -- Match string number from metric_id
LEFT JOIN {{ ref('mart_sensor_measurements_5min') }} poa
    ON poa.asset_id = ism.poa_sensor_id
    AND poa.timestamp = s.timestamp_5min
```

---

## Implementation Steps

### Step 1: Create Seed File
1. Create `seeds/seed_inverter_string_poa_mapping.csv`
2. Populate with inverter-string-POA mappings
3. Add to `dbt_project.yml` seed configuration
4. Run `dbt seed` to load

### Step 2: Update Intermediate Models (if needed)
- Verify `int_inverters_unified_5min` has all needed fields
- Verify `int_strings_unified_5min` exists and has string-level data
- If `int_strings_unified_5min` doesn't exist, create it from `int_inverters_unified_5min` by filtering string metrics

### Step 3: Update Mart Models
1. Update `mart_inverter_performance_5min` with POA joins and calculations
2. Update `mart_string_performance_5min` (enable it) with full string analysis
3. Test with sample data

### Step 4: Build Daily Aggregations
1. Update `mart_inverter_performance_daily` to use enhanced 5min mart
2. Update `mart_string_performance_daily` to use enhanced 5min mart
3. Add daily performance ratio averages

---

## Data Requirements

### For Seed File Population:

1. **Inverter Information:**
   - From `stg_fusionsolar__devices` and `stg_isolarcloud__devices`
   - Device IDs, names, site associations

2. **String Information:**
   - From inverter data - string voltage/current metrics indicate which strings exist
   - String numbers: 1-24 for FusionSolar, 1-40 for iSolarCloud
   - May need manual mapping for physical string-to-inverter relationships

3. **POA Sensor Mapping:**
   - From `seed_sensor_config` - filter by `sensor_type = 'POA'`
   - Map sensors to inverters/strings based on:
     - Physical proximity
     - Orientation match (azimuth)
     - Site location

4. **String Configuration:**
   - Panel count per string
   - Panel capacity
   - Azimuth and tilt (if available from site documentation)

---

## Questions to Resolve

1. **String Identification:**
   - How do we identify which string number corresponds to which physical string?
   - Do we have documentation mapping physical strings to inverter inputs?

2. **POA Sensor Selection:**
   - If multiple POA sensors exist at a site, which one maps to which inverter?
   - Should we use nearest sensor, or sensor with matching orientation?

3. **String Metrics Extraction:**
   - Does `int_inverters_unified_5min` contain string-level metrics (pv1_u, pv1_i, etc.)?
   - Or do we need to create `int_strings_unified_5min` separately?

4. **Performance Calculations:**
   - What efficiency factors should we use for expected power calculations?
   - Should we account for temperature derating?

---

## Next Actions

1. ✅ **Review this strategy** - Confirm approach and requirements
2. ⏳ **Gather mapping data** - Collect inverter-string-POA relationships
3. ⏳ **Create seed file** - Build `seed_inverter_string_poa_mapping.csv`
4. ⏳ **Update mart models** - Enhance with POA joins and calculations
5. ⏳ **Test and validate** - Verify performance ratio calculations

---

## Benefits

Once implemented, the enhanced marts will provide:

1. **Performance Analysis**: Compare actual vs. expected performance based on irradiance
2. **String-Level Insights**: Identify underperforming strings
3. **Inverter Efficiency**: Track inverter performance ratios over time
4. **POA Correlation**: Understand relationship between irradiance and power output
5. **Anomaly Detection**: Flag strings/inverters with unusual performance ratios

