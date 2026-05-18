# 5-Minute Calculated Metrics Fact Table Design

## Status: ✅ IMPLEMENTED

**Implementation Date**: 2025-01-XX  
**Status**: All fact tables created and built successfully

- ✅ `fact_sensor_calculations_5min`: 5,840,445 rows
- ✅ `fact_inverter_calculations_5min`: 8,614,969 rows
- ✅ `fact_site_calculations_5min`: 963,838 rows

## Overview

This document describes the design for 5-minute calculated metrics fact tables. The design uses **separate fact tables by entity type** (Option B) to maintain clear separation of concerns, optimal performance, and consistency with existing architecture.

**See Also**:
- [README.md](../models/facts/README.md) - Detailed documentation
- [FACT_TABLES_QUICK_REFERENCE.md](./FACT_TABLES_QUICK_REFERENCE.md) - Quick reference guide
- [FACT_TABLES_IMPLEMENTATION_SUMMARY.md](./FACT_TABLES_IMPLEMENTATION_SUMMARY.md) - Implementation summary

---

## Calculated Metrics Requirements

### 1. String-Level Calculations
- **String Energy**: `voltage * current` (calculated from raw metrics)
- **String Power**: `voltage * current` (instantaneous)
- **String Performance Ratio**: `string_energy / (poa_irradiance * string_capacity)`
- **Grain**: `timestamp_5min × string_id`

### 2. Inverter-Level Calculations
- **Inverter Availability**: `CASE WHEN active_power > 0 THEN 1 ELSE 0 END` (INTEGER: 0 or 1)
- **Inverter Availability with MIT**: `CASE WHEN active_power > 0 AND mit = 1 THEN 1 ELSE 0 END` (INTEGER: 0 or 1)
- **MIT**: `CASE WHEN irradiance > 40 THEN 1 ELSE 0 END` (INTEGER: 0 or 1, from sensor fact table)
- **Inverter Performance Ratio**: `inverter_energy / (weighted_poa_irradiance * total_capacity)`
- **Weighted POA Irradiance**: `SUM(poa_irradiance * string_capacity) / SUM(string_capacity)`
- **Grain**: `timestamp_5min × inverter_id`

### 3. Sensor-Level Calculations
- **MIT (Minimum Irradiance Threshold)**: `CASE WHEN irradiance > 40 THEN 1 ELSE 0 END` (INTEGER: 0 or 1)
- **Daily Irradiance Accumulation**: Cumulative calculation for daily totals
- **Stuck Sensor Detection**: Uses window functions to detect and filter out malfunctioning sensors
  - A sensor is considered STUCK if it reports the same value for 6+ consecutive intervals (30+ minutes)
  - Only high values (> 100 W/m²) are flagged as stuck to avoid false positives
  - Stuck sensors are marked as NULL and trigger fallback to next priority
- **Sensor Validation Rules**:
  - Deep nighttime validation: Values > 50 W/m² during 00:00-05:59 are suspicious
  - Unrealistic high values: > 2000 W/m² are filtered
  - Stuck values: Constant readings for 30+ minutes with value > 100 W/m² are filtered
- **GHI Fallback Logic**: Sites without GHI sensors use GHI from another site (configured in `seed_sensor_site_mapping`)
- **POA Fallback for MIT**: If GHI (pyranometer) is not available or stuck, use POA from same site as fallback for MIT calculation
- **MIT Fallback Priority**:
  1. **Priority 1**: GHI (pyranometer) from same site - if available and validated
  2. **Priority 2**: POA from same site - if GHI is NULL/mati/stuck and POA is validated
  3. **Priority 3**: GHI from other site - configured fallback (last resort)
- **Grain**: `timestamp_5min × sensor_id`

### 4. Site-Level Calculations
- **Power Available Ratio**: `available_inverters / total_inverters` (DECIMAL: 0-1)
- **Unavailability Ratio**: `CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END` (DECIMAL: 0-1)
- **MIT**: Site-level MIT from sensors (INTEGER: 0 or 1, already has GHI fallback applied)
- **Grain**: `timestamp_5min × site_id`

### 5. Daily Aggregation (in `mart_site_performance_daily_raw`)
- **Power Available Hours**: `SUM(power_available_ratio) / 12.0` (5 minutes = 1/12 hour)
- **Unavailability Hours**: `SUM(unavailability_ratio) / 12.0`
- **Total Hours**: `power_available_hours + unavailability_hours` (only counts time when MIT = 1)
- **Availability Percent**: `power_available_hours / total_hours * 100`

---

## Override Logic

### Overview
Some sites have special requirements where sensor data needs to be mapped differently than physical location:
1. **POA Override**: POA sensors physically located at one site but logically belong to another site
2. **GHI Fallback**: Sites without GHI sensors use GHI from another site (e.g., MMKI II and III use GHI from MMKI I)

### Configuration: `seed_sensor_site_mapping.csv`

```csv
mapping_type;device_id;logical_site_id;effective_date_start;effective_date_end;notes
POA_OVERRIDE;EM06102287046729;NE=51758766;2025-09-16;;POA sensor MMKI II mulai 16 Sep 2025
GHI_FALLBACK;PT. MMKI 2.XX MWp - Site Name;PT. MMKI 1.75 MWp - Painting Building;;;MMKI II pakai GHI dari MMKI I
```

**Columns:**
- `mapping_type`: `POA_OVERRIDE` or `GHI_FALLBACK`
- `device_id`: Device ID (for POA) or site_name (for GHI fallback)
- `logical_site_id`: Target site ID/name for override
- `effective_date_start`: Start date for override (NULL = always applies)
- `effective_date_end`: End date for override (NULL = no end date)
- `notes`: Documentation

### POA Override Logic

**Problem**: POA sensors from MMKI II and III are physically stored at MMKI I, but logically belong to MMKI II/III.

**Solution**: 
1. In `fact_sensor_calculations_5min`, POA sensors use physical site_name from source
2. In `mart_site_performance_daily_raw`, POA site assignment is overridden using `seed_sensor_site_mapping`
3. Override is date-aware (can start/end at specific dates)

**Implementation Flow:**
```
mart_sensor_measurements_5min (physical site_name = MMKI I)
    ↓
fact_sensor_calculations_5min (still physical site_name)
    ↓
mart_site_performance_daily_raw (apply POA override → logical site_name = MMKI II/III)
```

### GHI Fallback Logic

**Problem**: MMKI II and III don't have GHI sensors, but need GHI values for MIT calculation and PR calculation.

**Solution**:
1. In `fact_sensor_calculations_5min`, GHI fallback is applied at 5-minute level
2. Sites without GHI sensors get GHI values from source site (e.g., MMKI I)
3. MIT calculation uses the fallback GHI, ensuring correct availability calculation

**Implementation Flow:**
```
fact_sensor_calculations_5min:
  - MMKI I: Has GHI sensor → use actual GHI
  - MMKI II: No GHI sensor → use GHI from MMKI I (fallback)
  - MMKI III: No GHI sensor → use GHI from MMKI I (fallback)
    ↓
MIT calculation uses correct GHI (with fallback applied)
    ↓
fact_inverter_calculations_5min: Uses MIT from sensor fact (already correct)
    ↓
fact_site_calculations_5min: Uses MIT from inverter fact (already correct)
```

**Why at 5-minute level?**
- MIT must be calculated correctly from the start
- If GHI fallback is only applied at daily level, MIT calculation would be wrong
- All downstream calculations (availability, PR) depend on correct MIT

---

## Fact Table Structures

### 1. `fact_sensor_calculations_5min`

**Purpose**: Sensor-level calculated metrics with GHI fallback logic applied.

**Structure:**
```sql
CREATE TABLE mart.fact_sensor_calculations_5min (
    timestamp TIMESTAMPTZ,
    date_key DATE,
    sensor_id VARCHAR,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    
    -- Sensor Configuration
    sensor_type VARCHAR,  -- 'GHI', 'POA', etc.
    is_fallback BOOLEAN,  -- true if GHI is from fallback
    
    -- Raw Inputs
    irradiance_w_m2 DECIMAL,
    
    -- Calculated Metrics
    mit INTEGER,  -- CASE WHEN irradiance > 40 THEN 1 ELSE 0 END (0 or 1 only)
    
    -- Metadata
    calculation_timestamp TIMESTAMPTZ
);
```

**Key Features:**
- GHI fallback logic applied at 5-minute level
- MIT calculated using correct GHI (with fallback)
- `is_fallback` flag to track which GHI values are from fallback

### 2. `fact_inverter_calculations_5min`

**Purpose**: Inverter-level calculated metrics including availability.

**Structure:**
```sql
CREATE TABLE mart.fact_inverter_calculations_5min (
    timestamp TIMESTAMPTZ,
    date_key DATE,
    inverter_id VARCHAR,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    
    -- Raw Inputs
    active_power_kw DECIMAL,
    mit INTEGER,  -- from fact_sensor_calculations_5min (already has GHI fallback)
    
    -- Calculated Metrics
    inverter_availability INTEGER,  -- CASE WHEN active_power > 0 THEN 1 ELSE 0 END (0 or 1)
    inverter_availability_with_mit INTEGER,  -- CASE WHEN active_power > 0 AND mit = 1 THEN 1 ELSE 0 END (0 or 1)
    
    -- Metadata
    calculation_timestamp TIMESTAMPTZ
);
```

**Key Features:**
- MIT taken from `fact_sensor_calculations_5min` (already has GHI fallback applied)
- Availability metrics are INTEGER (0 or 1), not DECIMAL

### 3. `fact_site_calculations_5min`

**Purpose**: Site-level calculated metrics aggregated from inverters.

**Structure:**
```sql
CREATE TABLE mart.fact_site_calculations_5min (
    timestamp TIMESTAMPTZ,
    date_key DATE,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    
    -- Aggregated Inputs
    total_inverters INTEGER,
    available_inverters INTEGER,
    mit INTEGER,  -- site-level MIT (0 or 1, same for all inverters at same site)
    
    -- Calculated Metrics
    power_available_ratio DECIMAL,  -- available_inverters / total_inverters (0-1)
    unavailability_ratio DECIMAL,   -- CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END (0-1)
    
    -- Metadata
    calculation_timestamp TIMESTAMPTZ
);
```

**Calculation Logic:**
```sql
-- IMPORTANT: total_inverters is FIXED from seed_site_config, not dynamic count
-- This ensures missing inverters are counted as unavailable

-- Step 1: Get fixed total_inverters from dim_assets
total_inverters = MAX(dim_assets.total_inverters)  -- From seed_site_config

-- Step 2: Count available inverters (power > 0)
available_inverters = COUNT(DISTINCT CASE WHEN inverter_availability = 1 THEN inverter_id END)

-- Step 3: Calculate power_available_ratio
-- Example: 9 out of 18 inverters → 9/18 = 0.5 (50%)
power_available_ratio = CASE 
    WHEN total_inverters > 0
    THEN available_inverters::DECIMAL / total_inverters::DECIMAL
    ELSE 0
END

-- Step 4: Calculate unavailability_ratio (only when MIT = 1)
-- Example: MIT=1, 9/18 available → unavailability = 1 - 0.5 = 0.5 (50%)
unavailability_ratio = CASE 
    WHEN mit = 1 AND total_inverters > 0
    THEN 1 - (available_inverters::DECIMAL / total_inverters::DECIMAL)
    ELSE 0
END
```

**Key Features:**
- Aggregated from `fact_inverter_calculations_5min`
- **Fixed Total Inverters**: Uses configured total from `seed_site_config`, not dynamic count
- **Missing Inverters = Unavailable**: Inverters that don't report data are counted as unavailable
- MIT is INTEGER (0 or 1)
- Ratios are DECIMAL (0-1)
- No `availability_percent` at 5-minute level (calculated at daily level)
- **Matches Excel Calculation**: Accurate availability even when some inverters fail to report

### 4. `fact_string_calculations_5min` (Future)

**Purpose**: String-level calculated metrics.

**Structure:**
```sql
CREATE TABLE mart.fact_string_calculations_5min (
    timestamp TIMESTAMPTZ,
    date_key DATE,
    string_id VARCHAR,
    inverter_id VARCHAR,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    
    -- String Configuration (from seed)
    string_number INTEGER,
    string_capacity_w DECIMAL,
    poa_sensor_id VARCHAR,
    
    -- Raw Inputs (for traceability)
    voltage_v DECIMAL,
    current_a DECIMAL,
    poa_irradiance_w_m2 DECIMAL,
    
    -- Calculated Metrics
    string_power_w DECIMAL,  -- voltage * current
    string_energy_wh DECIMAL,  -- cumulative or delta
    string_performance_ratio DECIMAL,  -- string_energy / (poa_irradiance * capacity)
    
    -- Metadata
    calculation_timestamp TIMESTAMPTZ
);
```

---

## Daily Aggregation: `mart_site_performance_daily`

**Purpose**: Daily aggregated metrics ready for PowerBI. This is the existing `mart_site_performance_daily` model, updated to:
- Use availability from `fact_site_calculations_5min`
- Apply POA override logic
- Apply GHI fallback logic
- **Keep PR and percentage calculations in database** (can be used in PowerBI or recalculated for crosscheck)

**Note**: PR and percentage calculations remain in the database. In PowerBI, you can:
- Use the calculated values from database (faster, consistent)
- Recalculate using DAX measures (for flexibility or crosscheck)
- Compare both to validate data quality

**Availability Calculation:**
```sql
-- Aggregate from fact_site_calculations_5min
daily_availability AS (
    SELECT 
        date_key,
        site_name,
        -- power_available_hours = SUM(power_available_ratio) / 12.0
        -- (5 minutes = 1/12 hour)
        SUM(power_available_ratio) / 12.0 as power_available_hours,
        
        -- unavailability_hours = SUM(unavailability_ratio) / 12.0
        SUM(unavailability_ratio) / 12.0 as unavailability_hours,
        
        -- total_hours = power_available_hours + unavailability_hours
        -- (only counts time when MIT = 1, i.e., when there's sunlight)
        (SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0) as total_hours,
        
        -- availability_percent = power_available_hours / total_hours * 100
        CASE 
            WHEN (SUM(power_available_ratio) + SUM(unavailability_ratio)) > 0
            THEN (SUM(power_available_ratio) / 12.0) / NULLIF(
                (SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0),
                0
            ) * 100
            ELSE NULL
        END as availability_percent
    FROM fact_site_calculations_5min
    GROUP BY date_key, site_name
)
```

**Key Points:**
- `total_hours` = `power_available_hours` + `unavailability_hours` (NOT count of intervals)
- Only counts time when MIT = 1 (when there's sunlight)
- `availability_percent` calculated at daily level, not 5-minute level

---

## Implementation Order

The fact tables must be built in this order due to dependencies:

1. **`fact_sensor_calculations_5min`** (First)
   - Applies GHI fallback logic
   - Calculates MIT with correct GHI
   - No dependencies

2. **`fact_inverter_calculations_5min`** (Second)
   - Depends on: `fact_sensor_calculations_5min` (for MIT)
   - Calculates inverter availability

3. **`fact_site_calculations_5min`** (Third)
   - Depends on: `fact_inverter_calculations_5min`
   - Aggregates inverter metrics to site level

4. **`fact_string_calculations_5min`** (Future)
   - Can be built independently
   - May depend on POA data from `fact_sensor_calculations_5min`

5. **`mart_site_performance_daily`** (Final)
   - Depends on: `fact_site_calculations_5min` (for availability)
   - Applies POA override logic
   - Applies GHI fallback logic (already applied at sensor level, but documented here)
   - Aggregates to daily level
   - **Calculates PR and percentages** (available in database, can be used in PowerBI or recalculated for crosscheck)

---

## Benefits of Separate Fact Tables

1. **Focused Design**
   - Each table optimized for its entity type
   - No NULL columns (only relevant metrics)
   - Efficient storage and indexing

2. **Clear Grain**
   - Each table has a single, clear grain
   - Easy to understand what each row represents
   - No confusion about entity types

3. **Better Performance**
   - Smaller tables = faster queries
   - Targeted indexes per entity type
   - Can optimize each table independently

4. **Flexible Schema**
   - Easy to add entity-specific calculations
   - No impact on other entity types
   - Can evolve independently

5. **Maintainable ETL**
   - Separate transformation pipelines
   - Clear separation of concerns
   - Easier to debug and test

6. **Query Simplicity**
   - No need to filter by entity_type
   - Natural queries for entity-specific analysis
   - Easy aggregations (single grain)

7. **Scalability**
   - Can add new calculation tables without affecting existing ones
   - Each table can grow independently
   - No schema bloat

8. **Consistency with Existing Architecture**
   - Matches current pattern (`mart_inverter_performance_5min`, `mart_sensor_measurements_5min`, etc.)
   - Same design philosophy
   - Easy for team to understand

---

## TODO List

### Phase 1: Core Fact Tables
- [x] Create `seed_sensor_site_mapping.csv` with POA override and GHI fallback configurations (already exists)
- [x] Update `dbt_project.yml` to include new seed file (already configured)
- [x] Create `fact_sensor_calculations_5min.sql` with GHI fallback logic and POA fallback for MIT
- [x] Create `fact_inverter_calculations_5min.sql` (depends on sensor fact)
- [x] Create `fact_site_calculations_5min.sql` (depends on inverter fact)
- [x] Create validation queries (`analyses/validate_fact_tables.sql`)
- [x] Test fact tables compilation (successful)
- [ ] Run dbt to build fact tables and test with real data
- [ ] Test GHI fallback logic for MMKI II and III
- [ ] Test MIT calculation with POA fallback

### Phase 2: Daily Aggregation
- [ ] Update `mart_site_performance_daily.sql` to use `fact_site_calculations_5min` for availability
- [ ] Keep PR and percentage calculations in `mart_site_performance_daily.sql` (for PowerBI use or crosscheck)
- [ ] Implement POA override logic in `mart_site_performance_daily.sql`
- [ ] Test POA override for MMKI II and III (starting from 2025-09-16)
- [ ] Verify daily availability calculation (power_available_hours, unavailability_hours, total_hours, availability_percent)
- [ ] Document PowerBI options: use database PR vs DAX calculation for crosscheck

### Phase 3: Testing & Validation
- [ ] Compare availability calculations with Excel
- [ ] Validate GHI fallback values match source site
- [ ] Validate POA override assigns sensors to correct logical sites
- [ ] Test date-aware POA override (effective_date_start/end)
- [ ] Performance testing for incremental loads

### Phase 4: Documentation & PowerBI
- [ ] Document PowerBI options: use database PR vs DAX calculation
- [ ] Create PowerBI DAX measures for PR calculations (for crosscheck)
- [ ] Document crosscheck methodology (compare database PR vs DAX PR)
- [ ] Create PowerBI data model relationships
- [ ] Test PowerBI reports with new fact tables
- [ ] Update data dictionary with new tables and columns

### Phase 5: Future Enhancements
- [ ] Create `fact_string_calculations_5min.sql` (if needed)
- [ ] Add capacity-weighted availability (if seed data available)
- [ ] Add data quality scores to fact tables
- [ ] Create materialized views for PowerBI (if needed)
