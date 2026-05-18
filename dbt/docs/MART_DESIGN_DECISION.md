# Mart Design Decision: Single Inverter Table vs. Separate Inverter & String Tables

## Context

Based on your requirements:
- ✅ String metrics (voltage/current) exist in `int_inverters_unified_5min`
- ✅ Manual mapping required: inverter → strings → POA sensors (engineering work)
- ✅ Each site/inverter has varying number of strings (e.g., 23-24, up to 28)
- ✅ Multiple POA sensors per site, mapped to specific strings
- ✅ Performance calculations:
  - **String**: `string_energy / (poa_irradiance * string_capacity)`
  - **Inverter**: `inverter_energy / (weighted_poa_irradiance * sum_of_string_capacities)`

---

## Option 1: Single Inverter Table (Wide Format)

### Structure
One table `mart_inverter_performance_5min` containing:
- Inverter-level metrics (one row per inverter per timestamp)
- String-level metrics as separate columns (string_1_voltage, string_1_current, string_1_power, string_1_poa_irradiance, string_1_performance_ratio, etc.)
- Up to 28 string columns (or max needed)

### Example Schema
```sql
CREATE TABLE mart.mart_inverter_performance_5min (
    timestamp_5min TIMESTAMPTZ,
    asset_id VARCHAR,
    inverter_name VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    date_key DATE,
    
    -- Inverter-level metrics (long format - multiple rows per timestamp)
    metric_id VARCHAR,
    metric_name VARCHAR,  -- inv_active_power, inv_yield, etc.
    metric_value DECIMAL,
    
    -- String 1 metrics
    string_1_voltage DECIMAL,
    string_1_current DECIMAL,
    string_1_power_w DECIMAL,  -- calculated: voltage * current
    string_1_poa_sensor_id VARCHAR,
    string_1_poa_irradiance DECIMAL,
    string_1_capacity_w DECIMAL,
    string_1_performance_ratio DECIMAL,  -- calculated
    
    -- String 2 metrics
    string_2_voltage DECIMAL,
    string_2_current DECIMAL,
    string_2_power_w DECIMAL,
    string_2_poa_sensor_id VARCHAR,
    string_2_poa_irradiance DECIMAL,
    string_2_capacity_w DECIMAL,
    string_2_performance_ratio DECIMAL,
    
    -- ... up to string_28
    
    -- Inverter-level aggregated
    weighted_poa_irradiance DECIMAL,  -- weighted by string capacity
    total_string_capacity_w DECIMAL,
    inverter_performance_ratio DECIMAL,
    string_count INTEGER
);
```

**OR Long Format (Better for varying string counts):**
```sql
-- One row per inverter per timestamp per metric
-- String data as JSONB or separate columns for first N strings
CREATE TABLE mart.mart_inverter_performance_5min (
    timestamp_5min TIMESTAMPTZ,
    asset_id VARCHAR,
    inverter_name VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    date_key DATE,
    
    -- Inverter metric
    metric_id VARCHAR,
    metric_name VARCHAR,
    metric_value DECIMAL,
    
    -- String data as JSONB array
    strings_data JSONB,  -- [{"string_num": 1, "voltage": 450, "current": 8.5, "poa_irradiance": 800, ...}, ...]
    
    -- Or as separate columns (up to 28)
    string_1_voltage DECIMAL,
    string_1_current DECIMAL,
    string_1_poa_irradiance DECIMAL,
    string_1_performance_ratio DECIMAL,
    -- ... string_2 through string_28
);
```

### ✅ Pros

1. **Single Source of Truth**
   - All inverter and string data in one place
   - No joins needed for inverter + string analysis
   - Easier for PowerBI - one table to connect

2. **Query Simplicity for Inverter Analysis**
   - Simple SELECT for inverter with all strings
   - No complex joins between inverter and string tables
   - Faster for dashboard queries (single table scan)

3. **Data Consistency**
   - Inverter and string metrics always aligned by timestamp
   - No risk of mismatched data between tables
   - Atomic updates (all data updated together)

4. **PowerBI Friendly**
   - Wide format is natural for pivot tables
   - Easy to create string comparison visuals
   - Single relationship to dimensions

5. **Easier Aggregations**
   - Daily aggregations straightforward (one table)
   - Inverter totals = sum of string columns
   - No need to join and aggregate

### ❌ Cons

1. **Schema Rigidity**
   - Fixed number of string columns (e.g., 28 max)
   - Wastes space for inverters with fewer strings (e.g., 10 strings but 28 columns)
   - Schema change required if max strings increases beyond 28

2. **Sparse Data**
   - Many NULL values for unused string columns
   - Storage waste for sites with 10 strings but 28 columns
   - Can be significant with 500M+ rows (your case: ~520M inverter rows)

3. **Query Complexity for String Analysis**
   - Need to UNPIVOT to analyze strings across inverters
   - Harder to filter "all strings with performance_ratio < 80%"
   - Complex WHERE clauses for string-specific queries
   - Cannot easily query "all strings" across all inverters

4. **Maintenance Burden**
   - Seed file must map all strings to columns
   - Column naming becomes complex (string_1_poa_sensor_id, string_1_capacity_w, etc.)
   - Hard to add new string-level metrics
   - Manual mapping becomes more complex (which column = which string)

5. **Scalability Issues**
   - If max strings grows to 40+, table becomes unwieldy
   - Column limit concerns (PostgreSQL has 1600 column limit, but still)
   - Wide tables are harder to optimize

6. **String-Level Filtering**
   - Cannot easily query "all strings" across all inverters
   - Need to check each string_* column separately
   - Poor performance for string-centric analysis

7. **Manual Mapping Complexity**
   - Your manual mapping (engineering work) becomes harder
   - Need to decide which string goes in which column
   - Risk of mapping errors (string 5 in column string_3)

---

## Option 2: Separate Inverter & String Tables (Long Format)

### Structure
Two tables:
- `mart_inverter_performance_5min` - Inverter-level metrics (one row per metric per timestamp)
- `mart_string_performance_5min` - String-level metrics (one row per string per timestamp)

### Example Schema

**mart_inverter_performance_5min:**
```sql
CREATE TABLE mart.mart_inverter_performance_5min (
    timestamp_5min TIMESTAMPTZ,
    asset_id VARCHAR,
    inverter_name VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    date_key DATE,
    
    -- Inverter metrics (long format)
    metric_id VARCHAR,
    metric_name VARCHAR,  -- inv_active_power, inv_yield, inv_dc_power, etc.
    metric_value DECIMAL,
    
    -- Aggregated from strings (calculated)
    weighted_poa_irradiance DECIMAL,  -- weighted by string capacity
    total_string_capacity_w DECIMAL,
    inverter_performance_ratio DECIMAL,
    string_count INTEGER
);
```

**mart_string_performance_5min:**
```sql
CREATE TABLE mart.mart_string_performance_5min (
    timestamp_5min TIMESTAMPTZ,
    string_id VARCHAR,  -- e.g., "INV001_STR01" or "FS_DEV001_STR01"
    inverter_id VARCHAR,  -- links to asset_id in inverter table
    inverter_name VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    date_key DATE,
    
    string_number INTEGER,  -- 1, 2, 3, ... (from seed mapping)
    
    -- String metrics (long format)
    metric_id VARCHAR,
    metric_name VARCHAR,  -- string_1_voltage, string_1_current, etc.
    metric_value DECIMAL,
    
    -- String configuration (from seed)
    poa_sensor_id VARCHAR,
    poa_sensor_name VARCHAR,
    string_capacity_w DECIMAL,
    azimuth_degrees INTEGER,
    tilt_degrees INTEGER,
    panel_count INTEGER,
    
    -- POA data (from sensor mart)
    poa_irradiance_w_m2 DECIMAL,
    ambient_temp_c DECIMAL,
    pv_temp_c DECIMAL,
    
    -- Calculated metrics
    string_power_w DECIMAL,  -- voltage * current (when both available)
    string_energy_wh DECIMAL,  -- cumulative (if calculated)
    string_performance_ratio DECIMAL  -- string_energy / (poa_irradiance * string_capacity)
);
```

### ✅ Pros

1. **Flexible Schema**
   - No fixed limit on number of strings
   - Easy to add new string-level metrics
   - Scales naturally with varying string counts (23, 24, 28, etc.)
   - No schema changes needed when adding sites with more strings

2. **Storage Efficiency**
   - No NULL columns for unused strings
   - Only stores data that exists
   - Better compression (repeated values in columns)
   - With 520M inverter rows, saving NULL columns is significant

3. **String-Centric Analysis**
   - Easy to query "all strings with performance_ratio < 80%"
   - Simple aggregations across all strings
   - Natural filtering and grouping by string
   - Find worst-performing strings site-wide easily

4. **Normalized Design**
   - Follows database normalization principles
   - Easier to maintain and understand
   - Clear separation of concerns

5. **Easier Maintenance**
   - Seed file maps strings naturally (one row per string)
   - Adding new string attributes is straightforward
   - No column proliferation
   - Manual mapping is simpler: just map string_number to POA

6. **Better for String Analytics**
   - Compare strings across inverters easily
   - Find worst-performing strings site-wide
   - String-level trending and analysis
   - Natural for "show me all strings for inverter X"

7. **Incremental Updates**
   - Can update string table independently if needed
   - Inverter table can be derived/aggregated from strings
   - More granular control

8. **Manual Mapping Simplicity**
   - Your engineering mapping work is easier
   - Just map: inverter_id → string_number → poa_sensor_id
   - No need to decide which column = which string
   - Less error-prone

### ❌ Cons

1. **Join Complexity**
   - Need to join two tables for inverter + string analysis
   - More complex PowerBI relationships
   - Potential performance impact with large joins (520M rows)

2. **Data Consistency Risk**
   - Inverter and string data could get out of sync
   - Need careful ETL to ensure alignment
   - More complex update logic

3. **Query Complexity for Combined Analysis**
   - More complex SQL for "inverter with all strings"
   - Need aggregations to get inverter totals
   - Multiple table scans for some queries

4. **PowerBI Complexity**
   - Two tables to connect
   - Need to create relationships
   - More complex DAX for combined analysis
   - May need summary table/view for dashboards

5. **Storage Overhead**
   - Duplicate timestamp/site/inverter metadata in both tables
   - More indexes to maintain
   - Potentially larger total storage (but offset by no NULLs)

6. **ETL Complexity**
   - Need to split data into two tables
   - More complex transformation logic
   - Two tables to build and maintain
   - Need to calculate weighted POA for inverter from strings

---

## Recommendation Matrix

| Use Case | Single Table | Separate Tables |
|----------|-------------|-----------------|
| **PowerBI Dashboards** | ✅ Better (one table) | ⚠️ More complex (but can use view) |
| **String Analysis** | ❌ Difficult (need UNPIVOT) | ✅ Natural |
| **Inverter Analysis** | ✅ Simple | ✅ Simple |
| **Storage Efficiency** | ❌ Wastes space (NULLs) | ✅ Efficient |
| **Schema Flexibility** | ❌ Rigid (28 max) | ✅ Flexible (unlimited) |
| **Query Performance** | ✅ Fast (single table) | ⚠️ Depends on joins |
| **Maintenance** | ⚠️ Complex columns | ✅ Easier |
| **Manual Mapping** | ❌ Complex (which column?) | ✅ Simple (just string_number) |
| **Scalability** | ❌ Limited to 28 strings | ✅ Unlimited |
| **String Filtering** | ❌ Hard | ✅ Easy |

---

## Hybrid Approach (Option 3)

### Structure
- `mart_inverter_performance_5min` - Inverter-level only (no string columns)
- `mart_string_performance_5min` - String-level (long format)
- **View**: `mart_inverter_with_strings_5min` - Pre-joined view for PowerBI

### Benefits
- Best of both worlds
- Inverter table stays clean
- String table for detailed analysis
- View for easy PowerBI consumption

### Trade-off
- Three objects to maintain (2 tables + 1 view)
- More ETL complexity

---

## Final Recommendation

### For Your Use Case: **Option 2 (Separate Tables) + View**

**Reasons:**

1. **Varying String Counts**: Your sites have 23-28 strings per inverter, varying by site. Separate tables handle this naturally without wasted columns.

2. **String-Centric Requirements**: You need string-level performance ratios (`string_energy / (poa_irradiance * string_capacity)`). Separate tables make this much easier.

3. **Manual Mapping**: Your seed file will map strings manually (engineering work). Long format (one row per string) is more natural and less error-prone than wide format (columns per string).

4. **Storage**: With 520M+ rows, avoiding NULL columns saves significant space. Even with duplicate metadata, the savings from no NULLs outweigh the cost.

5. **Scalability**: If you add sites with 40+ strings, separate tables scale without schema changes.

6. **Maintenance**: Easier to add new string attributes or metrics without altering table schema.

7. **PowerBI Solution**: Create a materialized view that joins both tables for dashboard needs. This gives you the best of both worlds.

---

## Implementation Approach (Option 2 + View)

### Step 1: Create Seed File
```csv
Source,site_id,inverter_id,inverter_name,string_number,string_id,poa_sensor_id,poa_sensor_name,azimuth_degrees,tilt_degrees,panel_count,panel_capacity_w
FusionSolar,NE=50488260,DEV001,Inverter-01,1,FS_DEV001_STR01,EM01102287046729,EMI-1 [NORTH],0,15,20,6000
FusionSolar,NE=50488260,DEV001,Inverter-01,2,FS_DEV001_STR02,EM02102287046729,EMI-2 [SOUTH],180,15,20,6000
```

### Step 2: Build String Mart
- Extract string metrics from `int_inverters_unified_5min` (filter by metric_id like 'pv1_u', 'pv1_i', 'p96', 'p70', etc.)
- Join with seed mapping to get POA sensor and string config
- Join with `mart_sensor_measurements_5min` to get POA irradiance
- Calculate string power (voltage * current), energy, performance ratio

### Step 3: Build Inverter Mart
- Use inverter-level metrics from `int_inverters_unified_5min` (exclude string metrics)
- Aggregate from string mart to get:
  - Weighted POA irradiance: `SUM(string_poa_irradiance * string_capacity) / SUM(string_capacity)`
  - Total string capacity: `SUM(string_capacity)`
  - String count
- Calculate inverter performance ratio: `inverter_energy / (weighted_poa_irradiance * total_string_capacity)`

### Step 4: Create PowerBI View (Optional)
```sql
CREATE MATERIALIZED VIEW mart.mart_inverter_with_strings_5min AS
SELECT 
    i.timestamp_5min,
    i.asset_id as inverter_id,
    i.inverter_name,
    i.site_name,
    i.system,
    i.date_key,
    i.metric_name,
    i.metric_value,
    i.weighted_poa_irradiance,
    i.total_string_capacity_w,
    i.inverter_performance_ratio,
    s.string_id,
    s.string_number,
    s.string_power_w,
    s.string_performance_ratio,
    s.poa_irradiance_w_m2
FROM mart.mart_inverter_performance_5min i
LEFT JOIN mart.mart_string_performance_5min s
    ON i.asset_id = s.inverter_id
    AND i.timestamp_5min = s.timestamp_5min;
```

---

## Decision Checklist

- [ ] Review this analysis
- [ ] Confirm PowerBI requirements (can we use a view?)
- [ ] Estimate storage impact for both options
- [ ] Test query performance for both approaches (if possible)
- [ ] Finalize decision
- [ ] Proceed with implementation

---

## Summary

**Recommended: Option 2 (Separate Tables) + Materialized View**

This gives you:
- ✅ Flexible schema for varying string counts
- ✅ Efficient storage (no NULL columns)
- ✅ Easy string analysis
- ✅ Simple manual mapping
- ✅ PowerBI-friendly view when needed
- ✅ Scalable for future growth

The only trade-off is slightly more complex PowerBI setup, but a materialized view solves this elegantly.
