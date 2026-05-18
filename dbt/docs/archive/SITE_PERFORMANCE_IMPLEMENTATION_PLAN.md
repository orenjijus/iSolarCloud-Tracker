# Site Performance Implementation Plan

## Architecture Decision

### Three-Tier Structure:

1. **Device-Level Marts** (Granular)
   - `mart_inverter_performance_5min` - Inverter metrics only
   - `mart_string_performance_5min` - String metrics
   - `mart_meter_performance_5min` - Meter metrics  
   - `mart_sensor_measurements_5min` - Sensor metrics

2. **Site-Level Marts** (Aggregated)
   - `mart_site_performance_5min` - 5-minute site aggregations
   - `mart_site_performance_daily` - Daily site aggregations

3. **Analysis Layer** (PowerBI/Views)
   - Cross-device analysis
   - Drill-down capabilities

---

## Site Performance Calculations

### 1. Site Availability (Capacity-Weighted)

**Formula:**
```
site_availability = SUM(available_inverter_capacity) / SUM(total_inverter_capacity)

Where:
- available_inverter_capacity = inverter_capacity IF active_power > 0 ELSE 0
- inverter_capacity = SUM(string_capacities) for that inverter
```

**Current (without seed - temporary):**
```sql
-- Count-based (temporary until we have capacity)
site_availability = 
    COUNT(DISTINCT CASE WHEN inv.active_power > 0 THEN inv.asset_id END)
    / COUNT(DISTINCT inv.asset_id)
```

**Future (with seed - capacity-weighted):**
```sql
-- Capacity-weighted
WITH inverter_capacities AS (
    SELECT 
        inv.site_name,
        inv.timestamp,
        inv.asset_id as inverter_id,
        CASE WHEN inv.metric_value > 0 THEN ism.total_string_capacity_w ELSE 0 END as available_capacity,
        ism.total_string_capacity_w as total_capacity
    FROM mart_inverter_performance_5min inv
    JOIN (
        SELECT 
            inverter_id,
            SUM(panel_capacity_w) as total_string_capacity_w
        FROM seed_inverter_string_poa_mapping
        GROUP BY inverter_id
    ) ism ON inv.asset_id = ism.inverter_id
    WHERE inv.metric_name = 'inv_active_power'
)
SELECT 
    site_name,
    timestamp,
    SUM(available_capacity) / NULLIF(SUM(total_capacity), 0) as site_availability_ratio
FROM inverter_capacities
GROUP BY site_name, timestamp
```

### 2. POA Weighted Average

**Formula:**
```
weighted_poa = SUM(poa_irradiance * capacity_for_that_poa) / SUM(capacity_for_that_poa)

Where capacity = sum of string capacities that use that POA sensor
```

**Current (without seed - simple average):**
```sql
-- Simple average of all POA sensors at site
SELECT 
    site_name,
    timestamp,
    AVG(metric_value) as poa_irradiance_w_m2
FROM mart_sensor_measurements_5min
WHERE metric_name = 'irradiance'
AND asset_id IN (
    SELECT device_id FROM seed_sensor_config WHERE sensor_type = 'POA'
)
GROUP BY site_name, timestamp
```

**Future (with seed - capacity-weighted):**
```sql
-- Weight by inverter capacity that uses each POA
WITH poa_with_capacity AS (
    SELECT 
        s.site_name,
        s.timestamp,
        s.metric_value as poa_irradiance,
        SUM(ism.panel_capacity_w) as total_capacity_for_poa
    FROM mart_sensor_measurements_5min s
    JOIN seed_inverter_string_poa_mapping ism 
        ON s.asset_id = ism.poa_sensor_id
    WHERE s.metric_name = 'irradiance'
    GROUP BY s.site_name, s.timestamp, s.metric_value
)
SELECT 
    site_name,
    timestamp,
    SUM(poa_irradiance * total_capacity_for_poa) 
    / NULLIF(SUM(total_capacity_for_poa), 0) as weighted_poa_irradiance_w_m2
FROM poa_with_capacity
GROUP BY site_name, timestamp
```

### 3. Performance Ratio

**POA-based:**
```
PR_POA = site_energy_kwh / (weighted_poa_irradiance_kwh_m2 * total_capacity_kw)
```

**GHI-based:**
```
PR_GHI = site_energy_kwh / (ghi_irradiance_kwh_m2 * total_capacity_kw)
```

---

## Proposed Site Performance Table

### `mart_site_performance_5min`

```sql
{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['timestamp_5min', 'site_id'],
    indexes=[
        {'columns': ['timestamp_5min', 'site_id'], 'type': 'btree'},
        {'columns': ['site_id'], 'type': 'btree'},
        {'columns': ['timestamp_5min'], 'type': 'btree'}
    ],
    meta={'hyperscale': true}
) }}

-- 5-minute site-level performance aggregations
WITH inverter_aggregations AS (
    SELECT 
        site_name,
        timestamp,
        COUNT(DISTINCT asset_id) as inverter_count,
        COUNT(DISTINCT CASE WHEN metric_value > 0 THEN asset_id END) as available_inverter_count,
        SUM(CASE WHEN metric_name = 'inv_active_power' THEN metric_value ELSE 0 END) as site_active_power_kw,
        SUM(CASE WHEN metric_name = 'inv_yield' THEN metric_value ELSE 0 END) as site_energy_kwh
    FROM {{ ref('mart_inverter_performance_5min') }}
    WHERE metric_name IN ('inv_active_power', 'inv_yield')
    GROUP BY site_name, timestamp
),

sensor_aggregations AS (
    SELECT 
        site_name,
        timestamp,
        AVG(CASE WHEN metric_name = 'irradiance' AND sc.sensor_type = 'POA' 
            THEN metric_value END) as poa_irradiance_w_m2,
        AVG(CASE WHEN metric_name = 'irradiance' AND sc.sensor_type = 'GHI' 
            THEN metric_value END) as ghi_irradiance_w_m2,
        AVG(CASE WHEN metric_name = 'ambient_temp' THEN metric_value END) as ambient_temp_c,
        AVG(CASE WHEN metric_name = 'pv_temp' THEN metric_value END) as pv_temp_c
    FROM {{ ref('mart_sensor_measurements_5min') }} s
    LEFT JOIN {{ ref('seed_sensor_config') }} sc 
        ON s.asset_id = sc.device_id
    GROUP BY site_name, timestamp
),

meter_aggregations AS (
    SELECT 
        site_name,
        timestamp,
        SUM(CASE WHEN metric_name = 'positive_active_energy' THEN metric_value ELSE 0 END) as meter_energy_kwh
    FROM {{ ref('mart_meter_performance_5min') }}
    WHERE metric_name = 'positive_active_energy'
    GROUP BY site_name, timestamp
)

SELECT 
    i.timestamp as timestamp_5min,
    da.asset_id as site_id,
    da.site_name,
    da.system,
    dd.date_key,
    
    -- Inverter Metrics
    i.inverter_count,
    i.available_inverter_count,
    CASE 
        WHEN i.inverter_count > 0 
        THEN i.available_inverter_count::DECIMAL / i.inverter_count::DECIMAL 
        ELSE NULL 
    END as site_availability_ratio,  -- Temporary: count-based
    i.site_active_power_kw,
    COALESCE(m.meter_energy_kwh, i.site_energy_kwh) as site_energy_kwh,  -- Prefer meter if available
    
    -- Irradiance Metrics
    s.poa_irradiance_w_m2,
    s.ghi_irradiance_w_m2,
    s.ambient_temp_c,
    s.pv_temp_c,
    
    -- Performance Ratios (will need capacity from seed later)
    -- For now, NULL until we have capacity
    NULL as performance_ratio_poa,
    NULL as performance_ratio_ghi,
    NULL as total_site_capacity_kw  -- Will come from seed
    
FROM inverter_aggregations i
LEFT JOIN sensor_aggregations s 
    ON i.site_name = s.site_name 
    AND i.timestamp = s.timestamp
LEFT JOIN meter_aggregations m 
    ON i.site_name = m.site_name 
    AND i.timestamp = m.timestamp
LEFT JOIN {{ ref('dim_assets') }} da 
    ON da.site_name = i.site_name 
    AND da.asset_level = 'Site'
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(i.timestamp)
WHERE i.timestamp IS NOT NULL
{% if is_incremental() %}
    AND i.timestamp > (SELECT MAX(timestamp_5min) FROM {{ this }})
{% endif %}
```

---

## Implementation Steps

### Step 1: Build Inverter Mart (Now)
- ✅ Remove availability (it's site-level)
- ✅ Build basic inverter metrics
- Command: `dbt run --select mart_inverter_performance_5min --full-refresh`

### Step 2: Create Site Performance 5min (Now)
- Create `mart_site_performance_5min` with:
  - Count-based availability (temporary)
  - Site energy (from inverters/meters)
  - POA/GHI (simple average)
  - Basic structure ready for capacity-weighting later

### Step 3: Create Site Performance Daily (Now)
- Aggregate from 5min table
- Add MTD/YTD calculations
- Daily availability, energy, PR

### Step 4: Enhance with Seed (Later)
- Update availability to capacity-weighted
- Update POA to capacity-weighted
- Add accurate performance ratios

---

## Questions to Answer

1. **Site Energy Source**: 
   - Use meter data (if available)?
   - Or sum of inverter yields?
   - Or both (prefer meter, fallback to inverter)?

2. **POA Calculation (Now)**:
   - Simple average of all POA sensors?
   - Or use first available POA sensor?
   - Or median?

3. **GHI Calculation**:
   - One GHI sensor per site?
   - Or average if multiple GHI sensors?

4. **Capacity (Temporary)**:
   - Can we get inverter capacity from device metadata?
   - Or wait for seed file?

---

## Recommendation

**Build site performance table now with:**
- Count-based availability (temporary, will enhance later)
- Site energy from meters (prefer) or inverters (fallback)
- Simple POA/GHI averages (will enhance to weighted later)
- Structure ready for capacity-weighting when seed is ready

This gives you:
- ✅ Site-level metrics immediately
- ✅ Can enhance later without breaking changes
- ✅ Clear separation: device marts vs. site marts

