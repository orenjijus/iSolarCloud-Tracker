# Site Performance Architecture

## Understanding

**Availability is Site-Level, Capacity-Weighted:**
- Site has 10 inverters
- Each inverter has capacity = sum of string capacities
- Availability = SUM(available_inverter_capacity) / SUM(total_inverter_capacity)
- Example: If 1 inverter (10% of capacity) is down → availability = 0.9
- If all inverters available → availability = 1.0

## Recommended Architecture

### Three-Level Mart Structure:

1. **Device-Level Marts** (Current):
   - `mart_inverter_performance_5min` - Inverter metrics (NO availability)
   - `mart_string_performance_5min` - String metrics
   - `mart_meter_performance_5min` - Meter metrics
   - `mart_sensor_measurements_5min` - Sensor metrics

2. **Site-Level Marts** (New/Enhanced):
   - `mart_site_performance_5min` - Site-level aggregations at 5-minute
   - `mart_site_performance_daily` - Site-level daily aggregations

3. **Analysis Layer** (Optional):
   - Views or PowerBI measures for cross-device analysis

---

## Site Performance Table Structure

### `mart_site_performance_5min`

**Purpose**: Site-level metrics aggregated from devices

**Key Metrics:**
- **Availability** (capacity-weighted from inverters)
- **POA Weighted Average** (from sensors, weighted by inverter capacity)
- **GHI** (from sensors)
- **Site Energy** (from meters or sum of inverters)
- **Performance Ratio (POA)** = site_energy / (weighted_poa * total_capacity)
- **Performance Ratio (GHI)** = site_energy / (ghi * total_capacity)

**Structure:**
```sql
CREATE TABLE mart.mart_site_performance_5min (
    timestamp_5min TIMESTAMPTZ,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    date_key DATE,
    
    -- Inverter Aggregations
    total_inverter_capacity_kw DECIMAL,  -- Sum of all inverter capacities
    available_inverter_capacity_kw DECIMAL,  -- Sum of inverters with active_power > 0
    site_availability_ratio DECIMAL,  -- available_capacity / total_capacity
    inverter_count INTEGER,
    available_inverter_count INTEGER,
    
    -- Energy Metrics
    site_energy_kwh DECIMAL,  -- From meters or sum of inverters
    site_active_power_kw DECIMAL,  -- Sum of inverter active_power
    
    -- Irradiance Metrics
    poa_irradiance_w_m2 DECIMAL,  -- Weighted average from sensors
    ghi_irradiance_w_m2 DECIMAL,  -- From GHI sensors
    ambient_temp_c DECIMAL,
    pv_temp_c DECIMAL,
    
    -- Performance Ratios
    performance_ratio_poa DECIMAL,  -- site_energy / (poa * total_capacity)
    performance_ratio_ghi DECIMAL,  -- site_energy / (ghi * total_capacity)
    
    -- Metadata
    data_quality_score DECIMAL  -- How complete is the data (0-1)
);
```

### `mart_site_performance_daily`

**Purpose**: Daily aggregated site metrics

**Key Metrics:**
- Daily energy (MWh)
- Daily availability (%)
- Daily POA/GHI (kWh/m²)
- Daily performance ratios
- MTD/YTD aggregations

**Structure:**
```sql
CREATE TABLE mart.mart_site_performance_daily (
    date_key DATE,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    
    -- Energy
    daily_energy_mwh DECIMAL,
    energy_mtd_mwh DECIMAL,  -- Month-to-date
    energy_ytd_mwh DECIMAL,  -- Year-to-date
    
    -- Availability
    daily_availability_percent DECIMAL,
    availability_mtd_percent DECIMAL,
    availability_ytd_percent DECIMAL,
    
    -- Irradiance
    daily_poa_kwh_m2 DECIMAL,
    daily_ghi_kwh_m2 DECIMAL,
    poa_mtd_kwh_m2 DECIMAL,
    ghi_mtd_kwh_m2 DECIMAL,
    
    -- Performance Ratios
    daily_pr_poa DECIMAL,
    daily_pr_ghi DECIMAL,
    pr_poa_mtd DECIMAL,
    pr_ghi_mtd DECIMAL,
    
    -- Inverter Summary
    total_inverter_capacity_kw DECIMAL,
    average_inverter_count INTEGER
);
```

---

## Calculation Logic

### 1. Site Availability (5-minute)

**Current (without seed):**
```sql
-- Simple count-based (temporary until we have inverter capacity)
site_availability_ratio = 
    COUNT(DISTINCT CASE WHEN inv.active_power > 0 THEN inv.asset_id END) 
    / COUNT(DISTINCT inv.asset_id)
```

**Future (with seed - capacity-weighted):**
```sql
-- Capacity-weighted availability
WITH inverter_status AS (
    SELECT 
        site_name,
        timestamp_5min,
        asset_id,
        CASE WHEN active_power > 0 THEN inverter_capacity_kw ELSE 0 END as available_capacity,
        inverter_capacity_kw as total_capacity
    FROM mart_inverter_performance_5min inv
    JOIN seed_inverter_string_poa_mapping ism 
        ON inv.asset_id = ism.inverter_id
    WHERE metric_name = 'inv_active_power'
    GROUP BY site_name, timestamp_5min, asset_id, active_power, inverter_capacity_kw
)
SELECT 
    site_name,
    timestamp_5min,
    SUM(available_capacity) / NULLIF(SUM(total_capacity), 0) as site_availability_ratio
FROM inverter_status
GROUP BY site_name, timestamp_5min
```

### 2. POA Weighted Average

**Current (without seed):**
```sql
-- Simple average of all POA sensors at site
poa_irradiance_w_m2 = AVG(poa.irradiance)
FROM mart_sensor_measurements_5min poa
WHERE metric_name = 'irradiance' 
AND sensor_type = 'POA'
GROUP BY site_name, timestamp
```

**Future (with seed - capacity-weighted):**
```sql
-- Weight POA by inverter capacity that uses that sensor
WITH poa_with_weights AS (
    SELECT 
        s.site_name,
        s.timestamp,
        s.metric_value as poa_irradiance,
        SUM(ism.panel_capacity_w) as total_capacity_for_this_poa
    FROM mart_sensor_measurements_5min s
    JOIN seed_inverter_string_poa_mapping ism 
        ON s.asset_id = ism.poa_sensor_id
    WHERE s.metric_name = 'irradiance'
    GROUP BY s.site_name, s.timestamp, s.metric_value
)
SELECT 
    site_name,
    timestamp,
    SUM(poa_irradiance * total_capacity_for_this_poa) 
    / NULLIF(SUM(total_capacity_for_this_poa), 0) as weighted_poa_irradiance
FROM poa_with_weights
GROUP BY site_name, timestamp
```

### 3. Performance Ratio

```sql
-- POA-based PR
performance_ratio_poa = 
    site_energy_kwh 
    / NULLIF(weighted_poa_irradiance_w_m2 * total_capacity_kw * 0.001, 0)

-- GHI-based PR  
performance_ratio_ghi = 
    site_energy_kwh 
    / NULLIF(ghi_irradiance_w_m2 * total_capacity_kw * 0.001, 0)
```

---

## Implementation Strategy

### Phase 1: Build Basic Site Performance (Now)

**Can build now without seed:**
- Site availability (count-based, temporary)
- Site energy (sum from inverters or meters)
- POA average (simple average of POA sensors)
- GHI (from GHI sensors)
- Basic performance ratios

**Limitations:**
- Availability not capacity-weighted (uses count instead)
- POA not weighted by capacity (uses simple average)

### Phase 2: Enhance with Seed (Later)

**After `seed_inverter_string_poa_mapping` is ready:**
- Update availability to be capacity-weighted
- Update POA to be capacity-weighted
- Add accurate performance ratios

---

## Recommendation

### Option A: Build Site Performance Table Now (Recommended)

**Pros:**
- Get site-level metrics immediately
- Can enhance later with seed file
- Clear separation: device marts vs. site marts

**Structure:**
- `mart_site_performance_5min` - 5-minute site aggregations
- `mart_site_performance_daily` - Daily aggregations with MTD/YTD

### Option B: Calculate in PowerBI/DAX

**Pros:**
- No new tables
- Flexible calculations

**Cons:**
- Slower queries
- Complex DAX
- Harder to maintain

---

## Proposed Implementation

1. **Remove availability from inverter mart** (it's site-level)
2. **Create `mart_site_performance_5min`** with:
   - Count-based availability (temporary)
   - Site energy aggregations
   - POA/GHI from sensors
   - Basic performance ratios
3. **Create `mart_site_performance_daily`** with:
   - Daily aggregations
   - MTD/YTD calculations
4. **Enhance later** with seed file for capacity-weighting

---

## Questions

1. **For site energy**: Should we use meter data or sum of inverter yields?
2. **For POA now**: Simple average of all POA sensors, or use first available?
3. **For GHI**: One GHI sensor per site, or average if multiple?

