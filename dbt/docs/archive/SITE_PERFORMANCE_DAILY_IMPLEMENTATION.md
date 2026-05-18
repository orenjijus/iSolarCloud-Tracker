# Site Performance Daily - Implementation Plan

## Requirements Summary

### 1. Energy Calculation (Daily Yield)
- **Source**: `mart_meter_performance_5min`
- **Filter**: `meter_type = 'Revenue'` from `seed_meter_config`
- **Method**: 
  - Check if `ABS(negative_active_energy)` > `ABS(positive_active_energy)` → use negative (generation)
  - Otherwise → use positive (consumption)
- **Daily Calculation**: 
  - `LAST_VALUE(metric_value) - FIRST_VALUE(metric_value)` for the day
  - OR: `MAX(metric_value) - MIN(metric_value)` excluding 0 and NULL
- **Multiple Meters**: If 2+ revenue meters per site, SUM their daily energy

### 2. Sensor Calculations (GHI and POA)

#### GHI
- **Source**: `mart_sensor_measurements_5min`
- **Filter**: `sensor_type = 'GHI'` from `seed_sensor_config`
- **Calculation**: `MAX(daily_irradiance)` per GHI sensor per day
- **Site GHI**: Use the MAX value (or average if multiple GHI sensors)

#### POA Weighted Average
- **Source**: `mart_sensor_measurements_5min`
- **Filter**: `sensor_type = 'POA'` from `seed_sensor_config`
- **Calculation**: 
  - Step 1: Get `MAX(daily_irradiance)` per POA sensor per day
  - Step 2: Weight by capacity from `seed_sensor_config` (new column needed)
  - Formula: `SUM(MAX_daily_irradiance * poa_capacity) / SUM(poa_capacity)` per site per day

**Action Required**: Add `capacity` column to `seed_sensor_config.csv`

### 3. Availability Calculation
- **Source**: `mart_inverter_performance_5min`
- **Filter**: `metric_name = 'inv_active_power'`
- **5-minute Logic**: 
  - Inverter available if `active_power > 0`
  - Site availability = `COUNT(available_inverters) / COUNT(total_inverters)` (count-based for now)
- **Daily Aggregation**:
  - `power_available_hours` = COUNT(5-min intervals where site available) * 5/60
  - `unavailibility_hours` = COUNT(5-min intervals where site unavailable) * 5/60
  - `availability_percent` = `power_available_hours / (power_available_hours + unavailibility_hours) * 100`

### 4. Performance Ratio (PR)

#### PR GHI
```
PR_GHI = daily_energy_mwh / (daily_ghi_kwh_m2 / 1000) / site_capacity_mw
```

#### PR POA
```
PR_POA = daily_energy_mwh / (daily_poa_weighted_kwh_m2 / 1000) / site_capacity_mw
```

**Note**: Need site capacity from `dim_site` (to be created)

### 5. Target Comparisons
- **Source**: `mart_simulation_targets_daily`
- **Join**: By `date_key` and `site_id` (or `site_code`)
- **Comparisons**:
  - `energy_actual_vs_target_pct` = `(daily_energy_mwh / energy_target_mwh) * 100`
  - `ghi_actual_vs_target_pct` = `(daily_ghi_kwh_m2 / ghi_target) * 100`
  - `energy_vs_ghi_variance` = `energy_actual_vs_target_pct - ghi_actual_vs_target_pct`
  - `pr_ghi_actual` = calculated PR GHI
  - `pr_poa_actual` = calculated PR POA
  - `pr_ghi_variance_pct` = `pr_ghi_actual - daily_pr_ghi_target`
  - `pr_poa_variance_pct` = `pr_poa_actual - daily_pr_poa_target`

---

## Implementation Steps

### Step 1: Update `seed_sensor_config.csv`
- Add `capacity` column (kW or MW) for POA sensors
- This will be used for weighted average calculation

### Step 2: Create `dim_site` Dimension
- **Purpose**: Site metadata including actual capacity and tariff
- **Columns**:
  - `site_id` (PK)
  - `site_name`
  - `site_code`
  - `system` (FusionSolar/iSolarCloud)
  - `actual_capacity_mw` (may differ from platform)
  - `tariff` (for severity ranking in PowerBI)
  - `latitude`, `longitude`
  - Other site metadata

### Step 3: Build `mart_site_performance_daily`
- Implement all calculations above
- Join with `mart_simulation_targets_daily` for target comparisons
- Include MTD/YTD aggregations (window functions)

---

## Questions to Resolve

1. **Energy Calculation**: Prefer `LAST_VALUE - FIRST_VALUE` or `MAX - MIN` (excluding 0/NULL)?
   - **Answer**: Either is fine, but `MAX - MIN` (excluding 0/NULL) is more robust

2. **POA Capacity**: What unit should capacity be in `seed_sensor_config`? (kW, MW, or W?)
   - **Recommendation**: Use kW for consistency

3. **Site Capacity**: Should `dim_site.actual_capacity_mw` be in MW or kW?
   - **Recommendation**: MW (as name suggests)

4. **Multiple GHI Sensors**: If site has multiple GHI sensors, use MAX or average?
   - **Recommendation**: Use MAX (as per your requirement)

5. **Target Join**: Should we join by `site_id` or `site_code`?
   - **Answer**: Use both (OR condition) as in `mart_simulation_targets_daily`

---

## Next Actions

1. ✅ Confirm energy calculation method
2. ✅ Add `capacity` column to `seed_sensor_config.csv`
3. ✅ Create `dim_site` dimension
4. ✅ Build `mart_site_performance_daily` model

