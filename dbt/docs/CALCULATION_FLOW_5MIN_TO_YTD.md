# Calculation Flow: 5-Minute to YTD

## ⚠️ Critical: Energy Calculation Method

**IMPORTANT**: Use **MAX - FIRST_VALUE** method for daily energy calculation (not MAX - MIN, not SUM of deltas).

### Why This Method is Better:

**Problems with MAX - MIN:**
1. **Meter Reset**: If meter resets during day → MIN = 0, MAX = new value → **Wrong calculation**
2. **Starts at 0**: If meter starts at 0 → MIN = 0, MAX = end value → **Missing carryover**

**Problems with SUM of Deltas:**
1. **NULL Values**: Missing readings create NULL deltas → Excluded from sum → **Underestimated**
2. **Zero Values**: No generation intervals = 0 delta → Included but not representative
3. **First Reading**: LAG = NULL for first reading → Excluded → **Missing first interval**

### Correct Method (Simplified):
- **All Days**: `MAX(dayN) - FIRST_VALUE(dayN)`
- **Energy Formula**: `energy = (positive_max - positive_first) - (negative_max - negative_first)`

**Why This Works for All Days**:
- **Normal Continuation**: FIRST_VALUE(day2) = MAX(day1) → Same result as MAX(day2) - MAX(day1)
- **Meter Reset**: FIRST_VALUE(day2) = reset value → Correctly calculates from reset point
- **Simpler**: One rule for all days, no need to check if first day of month

**Benefits**:
- ✅ Handles meter resets (uses first value, not MIN)
- ✅ Handles missing data (uses MAX, ignores gaps)
- ✅ More representative (actual daily generation)
- ✅ Simpler implementation (one rule for all days)
- ✅ Consistent with Excel calculation

---

## Overview

This document defines the complete calculation flow from 5-minute raw data to Year-to-Date (YTD) aggregations, ensuring traceability and accuracy of all business metrics.

## Key Metrics from seed_metric_mapper (used='yes')

### Meter Metrics
- `positive_active_energy` - Cumulative positive active energy (kWh/Wh)
- `negative_active_energy` - Cumulative negative active energy (kWh/Wh)
- `active_power` - Instantaneous active power (W/kW)
- `power_factor` - Power factor (dimensionless)

### Sensor Metrics
- `daily_irradiance` - Daily Global Horizontal Irradiance (MJ/m² or W/m²)
- `irradiance` - Instantaneous GHI (W/m²)

### Inverter Metrics
- `active_power` - Instantaneous active power (kW/W)

---

## Calculation Flow Architecture

```
5-Minute Raw Data (mart_*_performance_5min)
    ↓
Daily Aggregations (fact_site_performance_metrics_daily)
    ↓
MTD Aggregations (Window Functions)
    ↓
YTD Aggregations (Window Functions)
```

---

## 1. Energy Calculation Flow

### 1.1 Source: Cumulative Meter Readings

**Table**: `mart_meter_performance_5min`
**Metric**: `metric_name = 'positive_active_energy'`
**Unit**: kWh (FusionSolar) or Wh (iSolarCloud)
**Type**: Cumulative (increasing value)

**Example Data**:
```
timestamp          | site_id | metric_name            | metric_value
2025-09-01 00:00   | FS_123  | positive_active_energy | 3662.5
2025-09-01 00:05   | FS_123  | positive_active_energy | 3662.6
2025-09-01 00:10   | FS_123  | positive_active_energy | 3662.7
...
2025-09-01 23:55   | FS_123  | positive_active_energy | 3752.7
```

### 1.2 Step 1: 5-Minute Energy Delta

**Calculation**: Difference between consecutive readings

**Important**: Energy = Positive Active Energy - Negative Active Energy

```sql
-- For fact_site_performance_metrics_5min
-- Calculate delta for both positive and negative
WITH positive_delta AS (
    SELECT 
        timestamp,
        site_id,
        metric_value - LAG(metric_value) OVER (
            PARTITION BY site_id 
            ORDER BY timestamp
        ) as positive_delta
    FROM mart_meter_performance_5min
    WHERE metric_name = 'positive_active_energy'
),
negative_delta AS (
    SELECT 
        timestamp,
        site_id,
        metric_value - LAG(metric_value) OVER (
            PARTITION BY site_id 
            ORDER BY timestamp
        ) as negative_delta
    FROM mart_meter_performance_5min
    WHERE metric_name = 'negative_active_energy'
)
SELECT 
    p.timestamp,
    p.site_id,
    COALESCE(p.positive_delta, 0) - COALESCE(n.negative_delta, 0) as energy_delta_mwh
FROM positive_delta p
LEFT JOIN negative_delta n ON p.timestamp = n.timestamp AND p.site_id = n.site_id
```

**Unit Conversion**:
- If unit = 'Wh' → Divide by 1000 to get kWh
- If unit = 'kWh' → Use as-is
- Final unit: MWh (divide by 1000)

**Example**:
```
timestamp          | positive_value | negative_value | positive_delta | negative_delta | energy_delta_mwh
2025-09-01 00:00   | 3662.5        | 5488306.21    | NULL           | NULL           | NULL (first reading)
2025-09-01 00:05   | 3662.6        | 5488306.22    | 0.1 kWh        | 0.01 kWh       | 0.09 kWh (0.1 - 0.01)
2025-09-01 00:10   | 3662.7        | 5488306.23    | 0.1 kWh        | 0.01 kWh       | 0.09 kWh (0.1 - 0.01)
```

**Note**: Energy = Positive Active Energy - Negative Active Energy
- Positive Active Energy = Energy generated/exported
- Negative Active Energy = Energy consumed/imported
- Net Energy = Generated - Consumed

**⚠️ Critical: Meter Polarity Issue**

Sometimes meter installations are incorrect:
- `positive_active_energy` may actually measure **import/consumption** (should be negative)
- `negative_active_energy` may actually measure **generation/export** (should be positive)

**Solution**: Use automatic detection + manual override. See **[METER_POLARITY_DETECTION.md](METER_POLARITY_DETECTION.md)** for:
- Automatic detection query (compares MAX(positive) vs MAX(negative))
- Manual override configuration (`seed_meter_config.csv` `polarity_swapped` column)
- Updated calculation logic with polarity correction

**Edge Cases**:
- **Meter Reset**: If `energy_delta < 0` (for positive) or `energy_delta > 0` (for negative), meter was reset → Set to NULL or handle separately
- **Missing Reading**: If LAG is NULL → Set delta to NULL (first reading of day)
- **Unit Mismatch**: Ensure consistent units (normalize to MWh)
- **Negative Active Energy**: Subtract negative delta from positive delta to get net energy
- **Polarity Swapped**: If meter polarity is incorrect, swap the calculation: `negative - positive` instead of `positive - negative` (see METER_POLARITY_DETECTION.md)

### 1.3 Step 2: Daily Energy Aggregation

**Calculation**: MAX - FIRST_VALUE method (Simplified - One Rule for All Days)

**Method**: 
- **All Days**: `MAX(dayN) - FIRST_VALUE(dayN)`
- **Why This Works**: FIRST_VALUE of day N equals MAX of day N-1 (if no reset), so same result as MAX(dayN) - MAX(dayN-1)

```sql
-- For fact_site_performance_metrics_daily
-- CORRECT METHOD: MAX - FIRST_VALUE (one rule for all days)
-- Handles resets and missing data correctly

WITH daily_positive AS (
    SELECT 
        date_key,
        site_id,
        -- MAX value of the day
        MAX(metric_value) as positive_max,
        -- FIRST_VALUE of the day (not MIN, which could be 0)
        FIRST_VALUE(metric_value) OVER (
            PARTITION BY site_id, date_key 
            ORDER BY timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as positive_first
    FROM mart_meter_performance_5min
    WHERE metric_name = 'positive_active_energy'
    GROUP BY date_key, site_id, timestamp, metric_value
),
daily_negative AS (
    SELECT 
        date_key,
        site_id,
        MAX(metric_value) as negative_max,
        FIRST_VALUE(metric_value) OVER (
            PARTITION BY site_id, date_key 
            ORDER BY timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as negative_first
    FROM mart_meter_performance_5min
    WHERE metric_name = 'negative_active_energy'
    GROUP BY date_key, site_id, timestamp, metric_value
),
daily_positive_agg AS (
    SELECT 
        date_key,
        site_id,
        MAX(positive_max) - MIN(positive_first) as positive_energy_kwh
    FROM daily_positive
    GROUP BY date_key, site_id
),
daily_negative_agg AS (
    SELECT 
        date_key,
        site_id,
        MAX(negative_max) - MIN(negative_first) as negative_energy_kwh
    FROM daily_negative
    GROUP BY date_key, site_id
)
SELECT 
    p.date_key,
    p.site_id,
    -- Net energy = Positive - Negative, convert to MWh
    (p.positive_energy_kwh - COALESCE(n.negative_energy_kwh, 0)) / 1000.0 as energy_actual_mwh
FROM daily_positive_agg p
LEFT JOIN daily_negative_agg n 
    ON p.date_key = n.date_key 
    AND p.site_id = n.site_id
```

**Why This Method Works for All Days:**
- ✅ Uses FIRST_VALUE (not MIN) → Handles meter resets correctly
- ✅ Uses MAX → Ignores missing data (NULL values)
- ✅ More representative → Actual daily generation
- ✅ Simpler → One rule for all days (no need to check first day of month)
- ✅ Handles zero values correctly (if MAX = FIRST_VALUE, energy = 0)
- ✅ **Key Insight**: FIRST_VALUE(dayN) = MAX(dayN-1) if no reset, so same result as MAX(dayN) - MAX(dayN-1)

**Example**:

**Day 1 (Sep 1)**:
```
timestamp          | positive_value | first_value | max_value
2025-09-01 00:00   | 3662.5        | 3662.5      | 3662.5
2025-09-01 00:05   | 3662.6        | 3662.5      | 3662.6
...
2025-09-01 23:55   | 3752.7        | 3662.5      | 3752.7

Daily Energy = MAX(3752.7) - FIRST_VALUE(3662.5) = 90.2 kWh = 0.0902 MWh
```

**Day 2 (Sep 2)** - No Reset:
```
timestamp          | positive_value | first_value | max_value
2025-09-02 00:00   | 3752.7        | 3752.7      | 3752.7  (continues from Sep 1)
2025-09-02 00:05   | 3752.8        | 3752.7      | 3752.8
...
2025-09-02 23:55   | 3838.2        | 3752.7      | 3838.2

Daily Energy = MAX(3838.2) - FIRST_VALUE(3752.7) = 85.5 kWh = 0.0855 MWh
Same as: MAX(Sep 2) - MAX(Sep 1) = 3838.2 - 3752.7 = 85.5 kWh ✅
```

**Day 3 (Sep 3)** - With Reset:
```
timestamp          | positive_value | first_value | max_value
2025-09-03 00:00   | 0.0           | 0.0         | 0.0      (meter reset)
2025-09-03 00:05   | 0.1           | 0.0         | 0.1
...
2025-09-03 23:55   | 88.3          | 0.0         | 88.3

Daily Energy = MAX(88.3) - FIRST_VALUE(0.0) = 88.3 kWh = 0.0883 MWh ✅
Correctly calculates from reset point!
```

**Why This Works**:
- **Normal Continuation**: FIRST_VALUE(dayN) = MAX(dayN-1) → Same as MAX(dayN) - MAX(dayN-1)
- **Meter Reset**: FIRST_VALUE(dayN) = reset value → Correctly calculates from reset point
- **Simpler**: One rule for all days, no need to check if first day of month
- **Handles NULL/0 values**: Uses MAX (ignores gaps)
- **More representative**: Actual daily generation, not sum of intervals

**Validation**:
- Daily energy can be positive (net generation) or negative (net consumption)
- Typical range: 0-200 MWh per day for generation (depends on site capacity)
- Compare with target: `energy_actual_mwh / energy_target_mwh * 100`
- **Critical**: Use MAX - FIRST_VALUE method for ALL days (not MAX - MIN, not SUM of deltas)
- **One Rule**: `MAX(dayN) - FIRST_VALUE(dayN)` works for all days (handles resets automatically)

### 1.4 Step 3: MTD Aggregation (Month-to-Date)

**Calculation**: Running sum from start of month

```sql
-- Window function in fact_site_performance_metrics_daily
energy_mtd_actual_mwh = 
    SUM(energy_actual_mwh) OVER (
        PARTITION BY site_id, year, month 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
```

**Example**:
```
date_key     | site_id | energy_actual_mwh | energy_mtd_actual_mwh
2025-09-01   | FS_123  | 90.2              | 90.2
2025-09-02   | FS_123  | 85.5              | 175.7 (90.2 + 85.5)
2025-09-03   | FS_123  | 88.3              | 264.0 (90.2 + 85.5 + 88.3)
...
2025-09-30   | FS_123  | 92.1              | 2700.5 (sum of all days)
```

**Key Points**:
- Resets at start of each month
- Partition by `(site_id, year, month)` ensures proper reset
- Includes current day in calculation

### 1.5 Step 4: YTD Aggregation (Year-to-Date)

**Calculation**: Running sum from start of year

```sql
-- Window function in fact_site_performance_metrics_daily
energy_ytd_actual_mwh = 
    SUM(energy_actual_mwh) OVER (
        PARTITION BY site_id, year 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
```

**Example**:
```
date_key     | site_id | energy_actual_mwh | energy_ytd_actual_mwh
2025-01-01   | FS_123  | 95.0              | 95.0
2025-01-02   | FS_123  | 92.5              | 187.5
...
2025-09-01   | FS_123  | 90.2              | 24500.0 (sum Jan-Sep)
2025-09-30   | FS_123  | 92.1              | 27200.5 (sum Jan-Sep)
```

**Key Points**:
- Resets at start of each year (January 1st)
- Partition by `(site_id, year)` ensures proper reset
- Includes all days from Jan 1 to current date

---

## 2. GHI Calculation Flow

### 2.1 Source: Sensor Readings

**Table**: `mart_sensor_measurements_5min`
**Metric**: `metric_name = 'daily_ghi'`
**Unit**: MJ/m² (FusionSolar) or W/m² (iSolarCloud)
**Type**: Daily cumulative (resets at midnight)

**Example Data**:
```
timestamp          | site_id | metric_name | metric_value
2025-09-01 00:00   | FS_123  | daily_ghi  | 0.0
2025-09-01 00:05   | FS_123  | daily_ghi  | 0.1
2025-09-01 00:10   | FS_123  | daily_ghi  | 0.2
...
2025-09-01 23:55   | FS_123  | daily_ghi  | 5.2
```

### 2.2 Step 1: 5-Minute GHI Value

**Calculation**: Direct value (no delta needed for daily cumulative)

```sql
-- For fact_site_performance_metrics_5min
ghi_kwh_m2 = metric_value
```

**Unit Conversion**:
- If unit = 'MJ/m²' → Convert to kWh/m²: `MJ/m² * 0.277778 = kWh/m²`
- If unit = 'W/m²' → Convert to kWh/m²: `W/m² * 0.005 = kWh/m²` (for 5-min interval)
- Final unit: kWh/m²

**Note**: For `daily_ghi`, the value at end of day represents total daily GHI

### 2.3 Step 2: Daily GHI Aggregation

**Calculation**: Maximum value for the day (since it's cumulative)

```sql
-- For fact_site_performance_metrics_daily
ghi_actual_kwh_m2 = 
    MAX(metric_value) * conversion_factor  -- Convert to kWh/m²
    WHERE metric_name = 'daily_ghi'
    AND date_key = [selected_date]
    GROUP BY site_id
```

**Alternative (if using instantaneous GHI)**:
```sql
-- Sum of instantaneous GHI values
ghi_actual_kwh_m2 = 
    SUM(metric_value) * 5 / 60 / 1000  -- Convert W/m² to kWh/m²
    WHERE metric_name = 'ghi'
    AND date_key = [selected_date]
    GROUP BY site_id
```

**Example**:
```
date_key     | site_id | ghi_actual_kwh_m2
2025-09-01   | FS_123  | 5.2
2025-09-02   | FS_123  | 4.8
```

### 2.4 Step 3: MTD GHI Aggregation

**Calculation**: Sum of daily GHI values

```sql
-- Window function
ghi_mtd_actual_kwh_m2 = 
    SUM(ghi_actual_kwh_m2) OVER (
        PARTITION BY site_id, year, month 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
```

**Example**:
```
date_key     | site_id | ghi_actual_kwh_m2 | ghi_mtd_actual_kwh_m2
2025-09-01   | FS_123  | 5.2               | 5.2
2025-09-02   | FS_123  | 4.8               | 10.0
2025-09-30   | FS_123  | 5.5               | 150.0 (sum of all days)
```

### 2.5 Step 4: YTD GHI Aggregation

**Calculation**: Sum of daily GHI values from start of year

```sql
-- Window function
ghi_ytd_actual_kwh_m2 = 
    SUM(ghi_actual_kwh_m2) OVER (
        PARTITION BY site_id, year 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
```

---

## 3. Availability Calculation Flow

### 3.1 Source: Inverter Power Readings

**Table**: `mart_inverter_performance_5min`
**Metric**: `metric_name = 'active_power'`
**Unit**: kW or W
**Type**: Instantaneous (power at that moment)

**Example Data**:
```
timestamp_5min      | site_id | metric_name | metric_value
2025-09-01 00:00    | FS_123  | active_power| 0.0
2025-09-01 00:05    | FS_123  | active_power| 0.0
2025-09-01 06:00    | FS_123  | active_power| 50.5
2025-09-01 06:05    | FS_123  | active_power| 52.3
...
2025-09-01 18:00    | FS_123  | active_power| 0.0
```

### 3.2 Step 1: 5-Minute Availability Status

**Calculation**: Binary indicator (available = 1, unavailable = 0)

```sql
-- For fact_site_performance_metrics_5min
is_available = 
    CASE 
        WHEN metric_value > 0 THEN 1 
        ELSE 0 
    END
```

**Threshold**: Power > 0 indicates system is available

**Example**:
```
timestamp_5min      | active_power | is_available
2025-09-01 00:00    | 0.0          | 0
2025-09-01 06:00    | 50.5         | 1
2025-09-01 18:00    | 0.0          | 0
```

### 3.3 Step 2: Daily Availability Calculation

**Calculation**: Count available intervals, convert to hours

```sql
-- For fact_site_performance_metrics_daily
power_available_hours = 
    COUNT(CASE WHEN metric_value > 0 THEN 1 END) * 5.0 / 60.0
    WHERE metric_name = 'active_power'
    AND date_key = [selected_date]
    GROUP BY site_id

total_hours = 
    COUNT(*) * 5.0 / 60.0
    WHERE metric_name = 'active_power'
    AND date_key = [selected_date]
    GROUP BY site_id

availability_percent = 
    (power_available_hours / total_hours) * 100
```

**Example**:
```
date_key     | site_id | power_available_hours | total_hours | availability_percent
2025-09-01   | FS_123  | 12.0                 | 12.0        | 100.0%
2025-09-08   | FS_123  | 7.4                  | 12.0        | 61.7%  (downtime)
```

**Key Points**:
- Each 5-minute interval = 5/60 = 0.0833 hours
- Typical day: 288 intervals = 24 hours
- Availability = (available intervals / total intervals) * 100

### 3.4 Step 3: MTD Availability Aggregation

**Calculation**: Cumulative hours and percentage

```sql
-- Window functions
power_available_mtd_hours = 
    SUM(power_available_hours) OVER (
        PARTITION BY site_id, year, month 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )

total_mtd_hours = 
    SUM(total_hours) OVER (
        PARTITION BY site_id, year, month 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )

availability_mtd_percent = 
    (power_available_mtd_hours / total_mtd_hours) * 100
```

**Example**:
```
date_key     | power_available_hours | total_hours | power_available_mtd_hours | availability_mtd_percent
2025-09-01   | 12.0                 | 12.0        | 12.0                      | 100.0%
2025-09-02   | 12.0                 | 12.0        | 24.0                      | 100.0%
2025-09-08   | 7.4                  | 12.0        | 91.4                      | 99.3%
2025-09-30   | 12.0                 | 12.0        | 360.0                     | 99.8%
```

### 3.5 Step 4: YTD Availability Aggregation

**Calculation**: Cumulative hours from start of year

```sql
-- Window functions
power_available_ytd_hours = 
    SUM(power_available_hours) OVER (
        PARTITION BY site_id, year 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )

total_ytd_hours = 
    SUM(total_hours) OVER (
        PARTITION BY site_id, year 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )

availability_ytd_percent = 
    (power_available_ytd_hours / total_ytd_hours) * 100
```

---

## 4. Performance Ratio (PR) Calculation Flow

### 4.1 Source: Energy and GHI

**Inputs**:
- `energy_actual_mwh` (from Section 1)
- `ghi_actual_kwh_m2` (from Section 2)
- `energy_target_mwh` (from `mart_simulation_targets_daily`)
- `ghi_target_kwh_m2` (from `mart_simulation_targets_daily`)

### 4.2 Step 1: Daily PR Calculation

**Formula**: PR = (Actual Energy / Expected Energy) × 100

**Expected Energy** = Actual GHI × (Target Energy / Target GHI)

```sql
-- For fact_site_performance_metrics_daily
pr_ghi_percent = 
    CASE 
        WHEN ghi_target_kwh_m2 > 0 
        THEN (energy_actual_mwh / NULLIF(ghi_target_kwh_m2, 0)) * 
             (ghi_actual_kwh_m2 / NULLIF(ghi_target_kwh_m2, 0)) * 100
        ELSE NULL
    END
```

**Simplified Formula**:
```sql
pr_ghi_percent = 
    CASE 
        WHEN ghi_target_kwh_m2 > 0 
        THEN (energy_actual_mwh / energy_target_mwh) * 
             (ghi_target_kwh_m2 / ghi_actual_kwh_m2) * 100
        ELSE NULL
    END
```

**Example**:
```
date_key     | energy_actual_mwh | energy_target_mwh | ghi_actual_kwh_m2 | ghi_target_kwh_m2 | pr_ghi_percent
2025-09-01   | 90.2              | 104.15            | 5.2               | 6.0               | 72.3%
```

**Calculation**:
- PR = (90.2 / 104.15) × (6.0 / 5.2) × 100 = 72.3%

### 4.3 Step 2: MTD PR Calculation

**Calculation**: Weighted average of daily PR values

```sql
-- Option 1: Average of daily PR
pr_mtd_ghi_percent = 
    AVG(pr_ghi_percent) OVER (
        PARTITION BY site_id, year, month 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
```

**Alternative (more accurate)**:
```sql
-- Option 2: Calculate from MTD aggregates
pr_mtd_ghi_percent = 
    CASE 
        WHEN ghi_mtd_target_kwh_m2 > 0 
        THEN (energy_mtd_actual_mwh / energy_mtd_target_mwh) * 
             (ghi_mtd_target_kwh_m2 / ghi_mtd_actual_kwh_m2) * 100
        ELSE NULL
    END
```

### 4.4 Step 3: YTD PR Calculation

**Calculation**: Weighted average from start of year

```sql
-- Calculate from YTD aggregates
pr_ytd_ghi_percent = 
    CASE 
        WHEN ghi_ytd_target_kwh_m2 > 0 
        THEN (energy_ytd_actual_mwh / energy_ytd_target_mwh) * 
             (ghi_ytd_target_kwh_m2 / ghi_ytd_actual_kwh_m2) * 100
        ELSE NULL
    END
```

---

## 5. Unit Conversion Reference

### Energy Units
| Source Unit | Target Unit | Conversion Factor |
|------------|-------------|-------------------|
| Wh         | kWh         | ÷ 1000            |
| kWh        | MWh         | ÷ 1000            |
| Wh         | MWh         | ÷ 1,000,000       |

### GHI Units
| Source Unit | Target Unit | Conversion Factor |
|------------|-------------|-------------------|
| MJ/m²      | kWh/m²      | × 0.277778        |
| W/m²       | kWh/m² (5-min) | × 0.005        |
| W/m²       | kWh/m² (hourly) | × 0.001        |

### Time Units
| Source Unit | Target Unit | Conversion Factor |
|------------|-------------|-------------------|
| 5 minutes  | hours       | ÷ 12              |
| intervals  | hours (5-min) | × 0.0833        |

---

## 6. Data Quality Checks

### 6.1 Energy Validation

**Checks**:
1. **Negative Delta (Positive)**: If `positive_delta < 0` → Meter reset detected
2. **Positive Delta (Negative)**: If `negative_delta > 0` → Meter reset detected (negative should decrease)
3. **Unusually High**: If `energy_delta > threshold` → Data quality issue
4. **Missing Readings**: If `LAG() IS NULL` → First reading or gap (exclude from daily sum)
5. **Unit Consistency**: Verify all values in same unit (MWh)
6. **Net Energy Calculation**: Verify `energy_delta = positive_delta - negative_delta`

**Thresholds**:
- Maximum daily energy: Site capacity × 24 hours × 1.2 (20% buffer)
- Minimum daily energy: Can be negative (net consumption) or 0 (no generation)
- Maximum 5-minute delta: Site capacity × (5/60) × 1.2

**Critical Validation**:
- **Use MAX - FIRST_VALUE** method for ALL days (handles resets and missing data)
- **Never use MAX - MIN** (fails when MIN = 0 due to reset)
- **Never use SUM of deltas** (fails with NULL/0 values, missing first reading)
- **One Rule**: `MAX(dayN) - FIRST_VALUE(dayN)` works for all days
- **Why It Works**: FIRST_VALUE(dayN) = MAX(dayN-1) if no reset, so same result as MAX(dayN) - MAX(dayN-1)

### 6.2 GHI Validation

**Checks**:
1. **Negative Values**: GHI should never be negative
2. **Unusually High**: Maximum ~8-10 kWh/m² per day (depends on location)
3. **Missing Values**: If no sensor reading → Set to NULL
4. **Unit Consistency**: Verify conversion applied correctly

**Thresholds**:
- Maximum daily GHI: ~10 kWh/m² (tropical regions)
- Minimum daily GHI: 0 (nighttime)

### 6.3 Availability Validation

**Checks**:
1. **Total Hours**: Should be ~24 hours per day (288 intervals × 5 min)
2. **Available Hours**: Should be ≤ total hours
3. **Percentage**: Should be between 0% and 100%

**Thresholds**:
- Normal availability: > 95%
- Warning: 90-95%
- Critical: < 90%

---

## 7. Complete Calculation Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 5-MINUTE RAW DATA (mart_*_performance_5min)                 │
│                                                              │
│ Meter:  positive_active_energy (cumulative)                  │
│ Sensor: daily_ghi (cumulative)                              │
│ Inverter: active_power (instantaneous)                       │
└──────────────────┬──────────────────────────────────────────┘
                    │
                    │ STEP 1: Calculate Deltas/Aggregates
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ 5-MINUTE FACT TABLE (fact_site_performance_metrics_5min)    │
│                                                              │
│ energy_delta_mwh = metric_value - LAG(metric_value)         │
│ ghi_kwh_m2 = metric_value (converted)                      │
│ is_available = CASE WHEN active_power > 0 THEN 1 ELSE 0     │
└──────────────────┬──────────────────────────────────────────┘
                    │
                    │ STEP 2: Daily Aggregation
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ DAILY FACT TABLE (fact_site_performance_metrics_daily)      │
│                                                              │
│ energy_actual_mwh = SUM(energy_delta_mwh)                   │
│ ghi_actual_kwh_m2 = MAX(daily_ghi)                         │
│ availability_percent = (available_hours / total_hours) * 100│
│ pr_ghi_percent = (energy/ghi) calculation                   │
└──────────────────┬──────────────────────────────────────────┘
                    │
                    │ STEP 3: MTD Window Functions
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ MTD COLUMNS (in daily fact table)                           │
│                                                              │
│ energy_mtd_actual_mwh = SUM() OVER (PARTITION BY month)     │
│ ghi_mtd_actual_kwh_m2 = SUM() OVER (PARTITION BY month)    │
│ availability_mtd_percent = (mtd_hours / mtd_total) * 100    │
└──────────────────┬──────────────────────────────────────────┘
                    │
                    │ STEP 4: YTD Window Functions
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ YTD COLUMNS (in daily fact table)                           │
│                                                              │
│ energy_ytd_actual_mwh = SUM() OVER (PARTITION BY year)      │
│ ghi_ytd_actual_kwh_m2 = SUM() OVER (PARTITION BY year)      │
│ availability_ytd_percent = (ytd_hours / ytd_total) * 100    │
└─────────────────────────────────────────────────────────────┘
```

---

## 8. SQL Implementation Reference

### 8.1 Daily Energy Calculation

```sql
-- CORRECT METHOD: MAX - FIRST_VALUE (One rule for all days)
-- Simpler and more efficient than checking first day of month

WITH daily_positive AS (
    SELECT 
        date_key,
        site_id,
        -- MAX value of the day
        MAX(metric_value) as positive_max,
        -- FIRST_VALUE of the day (not MIN, which could be 0)
        FIRST_VALUE(metric_value) OVER (
            PARTITION BY site_id, date_key 
            ORDER BY timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as positive_first
    FROM mart_meter_performance_5min
    WHERE metric_name = 'positive_active_energy'
    GROUP BY date_key, site_id, timestamp, metric_value
),
daily_negative AS (
    SELECT 
        date_key,
        site_id,
        MAX(metric_value) as negative_max,
        FIRST_VALUE(metric_value) OVER (
            PARTITION BY site_id, date_key 
            ORDER BY timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as negative_first
    FROM mart_meter_performance_5min
    WHERE metric_name = 'negative_active_energy'
    GROUP BY date_key, site_id, timestamp, metric_value
),
daily_positive_agg AS (
    SELECT 
        date_key,
        site_id,
        MAX(positive_max) - MIN(positive_first) as positive_energy_kwh
    FROM daily_positive
    GROUP BY date_key, site_id
),
daily_negative_agg AS (
    SELECT 
        date_key,
        site_id,
        MAX(negative_max) - MIN(negative_first) as negative_energy_kwh
    FROM daily_negative
    GROUP BY date_key, site_id
)
SELECT 
    p.date_key,
    p.site_id,
    -- Net energy = Positive - Negative, convert to MWh
    (p.positive_energy_kwh - COALESCE(n.negative_energy_kwh, 0)) / 1000.0 as energy_actual_mwh
FROM daily_positive_agg p
LEFT JOIN daily_negative_agg n 
    ON p.date_key = n.date_key 
    AND p.site_id = n.site_id

-- Why This Works:
-- - Normal continuation: FIRST_VALUE(dayN) = MAX(dayN-1) → Same as MAX(dayN) - MAX(dayN-1)
-- - Meter reset: FIRST_VALUE(dayN) = reset value → Correctly calculates from reset point
-- - Simpler: One rule for all days, no need to check first day of month

-- WRONG METHODS (DO NOT USE):
-- 1. MAX(metric_value) - MIN(metric_value) 
--    Fails when meter resets (MIN = 0)
-- 2. SUM(energy_delta_mwh)
--    Fails with NULL/0 values, missing first reading
```

### 8.2 Daily GHI Calculation

```sql
WITH daily_ghi AS (
    SELECT 
        date_key,
        site_id,
        site_name,
        -- Get maximum (cumulative value at end of day)
        MAX(CASE 
            WHEN metric_name = 'daily_ghi' AND metric_unit = 'MJ/m²' 
            THEN metric_value * 0.277778  -- Convert MJ/m² to kWh/m²
            WHEN metric_name = 'daily_ghi' AND metric_unit = 'W/m²' 
            THEN metric_value / 1000.0  -- Convert W/m² to kWh/m²
            ELSE metric_value 
        END) as ghi_actual_kwh_m2
    FROM mart_sensor_measurements_5min
    WHERE metric_name = 'daily_ghi'
    GROUP BY date_key, site_id, site_name
)
SELECT * FROM daily_ghi
```

### 8.3 Daily Availability Calculation

```sql
WITH daily_availability AS (
    SELECT 
        date_key,
        site_id,
        site_name,
        COUNT(CASE WHEN metric_value > 0 THEN 1 END) * 5.0 / 60.0 as power_available_hours,
        COUNT(*) * 5.0 / 60.0 as total_hours
    FROM mart_inverter_performance_5min
    WHERE metric_name = 'active_power'
    GROUP BY date_key, site_id, site_name
)
SELECT 
    *,
    CASE 
        WHEN total_hours > 0 
        THEN (power_available_hours / total_hours) * 100 
        ELSE NULL 
    END as availability_percent
FROM daily_availability
```

### 8.4 YTD/MTD Window Functions

```sql
SELECT 
    *,
    -- MTD Aggregations
    SUM(energy_actual_mwh) OVER (
        PARTITION BY site_id, year, month 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as energy_mtd_actual_mwh,
    
    -- YTD Aggregations
    SUM(energy_actual_mwh) OVER (
        PARTITION BY site_id, year 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as energy_ytd_actual_mwh
FROM fact_site_performance_metrics_daily
```

---

## 9. Validation Queries

### 9.1 Verify Energy Calculation

```sql
-- Verify daily energy calculation (MAX - FIRST_VALUE method)
SELECT 
    date_key,
    site_id,
    -- Method 1: MAX - FIRST_VALUE (CORRECT)
    MAX(metric_value) - FIRST_VALUE(metric_value) OVER (
        PARTITION BY site_id, date_key 
        ORDER BY timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as method1_max_first,
    -- Method 2: Direct from daily fact table
    MAX(energy_actual_mwh) as method2_daily_fact
FROM mart_meter_performance_5min m
LEFT JOIN fact_site_performance_metrics_daily f
    ON m.date_key = f.date_key AND m.site_id = f.site_id
WHERE m.metric_name = 'positive_active_energy'
GROUP BY date_key, site_id, timestamp, metric_value
HAVING ABS(method1_max_first - method2_daily_fact) > 0.01  -- Flag discrepancies
-- Should match (within rounding tolerance)

-- Verify energy = positive - negative
WITH daily_positive AS (
    SELECT 
        date_key,
        site_id,
        MAX(metric_value) - FIRST_VALUE(metric_value) OVER (
            PARTITION BY site_id, date_key ORDER BY timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as positive_energy
    FROM mart_meter_performance_5min
    WHERE metric_name = 'positive_active_energy'
    GROUP BY date_key, site_id, timestamp, metric_value
),
daily_negative AS (
    SELECT 
        date_key,
        site_id,
        MAX(metric_value) - FIRST_VALUE(metric_value) OVER (
            PARTITION BY site_id, date_key ORDER BY timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as negative_energy
    FROM mart_meter_performance_5min
    WHERE metric_name = 'negative_active_energy'
    GROUP BY date_key, site_id, timestamp, metric_value
)
SELECT 
    p.date_key,
    p.site_id,
    MAX(p.positive_energy) - COALESCE(MAX(n.negative_energy), 0) as calculated_net,
    MAX(f.energy_actual_mwh) * 1000 as fact_table_net  -- Convert MWh to kWh
FROM daily_positive p
LEFT JOIN daily_negative n ON p.date_key = n.date_key AND p.site_id = n.site_id
LEFT JOIN fact_site_performance_metrics_daily f ON p.date_key = f.date_key AND p.site_id = f.site_id
GROUP BY p.date_key, p.site_id
-- calculated_net should equal fact_table_net
```

### 9.2 Verify YTD Calculation

```sql
-- Verify YTD = sum of all days from Jan 1
SELECT 
    site_id,
    date_key,
    energy_ytd_actual_mwh,
    -- Manual calculation
    (SELECT SUM(energy_actual_mwh) 
     FROM fact_site_performance_metrics_daily d2
     WHERE d2.site_id = d1.site_id
     AND d2.year = d1.year
     AND d2.date_key <= d1.date_key
    ) as manual_ytd_sum
FROM fact_site_performance_metrics_daily d1
WHERE date_key = '2025-09-30'
-- Should be equal
```

---

## 10. Key Metrics Summary

| Metric | Source Table | Source Metric | Calculation Method | Unit |
|--------|-------------|---------------|-------------------|------|
| **Energy Daily** | `mart_meter_performance_5min` | `positive_active_energy`, `negative_active_energy` | MAX - FIRST_VALUE (positive - negative) | MWh |
| **Energy MTD** | `fact_site_performance_metrics_daily` | `energy_actual_mwh` | SUM() OVER (month) | MWh |
| **Energy YTD** | `fact_site_performance_metrics_daily` | `energy_actual_mwh` | SUM() OVER (year) | MWh |
| **GHI Daily** | `mart_sensor_measurements_5min` | `daily_ghi` | MAX (cumulative) | kWh/m² |
| **GHI MTD** | `fact_site_performance_metrics_daily` | `ghi_actual_kwh_m2` | SUM() OVER (month) | kWh/m² |
| **GHI YTD** | `fact_site_performance_metrics_daily` | `ghi_actual_kwh_m2` | SUM() OVER (year) | kWh/m² |
| **Availability Daily** | `mart_inverter_performance_5min` | `active_power` | COUNT(>0) / COUNT(*) | % |
| **Availability MTD** | `fact_site_performance_metrics_daily` | `power_available_hours` | SUM() OVER (month) | % |
| **Availability YTD** | `fact_site_performance_metrics_daily` | `power_available_hours` | SUM() OVER (year) | % |
| **PR Daily** | Calculated | `energy_actual_mwh` / `ghi_actual_kwh_m2` | Formula | % |
| **PR MTD** | Calculated | MTD aggregates | Formula | % |
| **PR YTD** | Calculated | YTD aggregates | Formula | % |

---

## 11. Edge Cases and Handling

### 11.1 Meter Reset

**Scenario**: Meter reading decreases (reset or replacement)

**Detection**:
```sql
-- For positive_active_energy
positive_delta < 0  -- Should always increase

-- For negative_active_energy  
negative_delta > 0  -- Should always increase (negative value increasing)
```

**Handling**:
```sql
CASE 
    WHEN positive_delta < 0 THEN NULL  -- Ignore reset (positive)
    WHEN negative_delta > 0 THEN NULL  -- Ignore reset (negative)
    WHEN energy_delta > threshold THEN NULL  -- Data quality issue
    ELSE energy_delta
END
```

**Important**: When meter resets, exclude that interval from daily sum

### 11.2 Missing Data

**Scenario**: No readings for a time period

**Handling**:
- Set to NULL (don't use 0)
- Exclude from aggregations
- Document gaps in data quality report

### 11.3 Month/Year Boundaries

**Scenario**: MTD/YTD calculations at boundaries

**Handling**:
- Window functions automatically reset at boundaries
- Partition by `(site_id, year, month)` ensures correct reset
- Verify first day of month/year has correct values

---

## 12. Testing Checklist

- [ ] Verify energy delta calculation (5-min)
- [ ] Verify daily energy aggregation
- [ ] Verify MTD energy calculation (resets at month start)
- [ ] Verify YTD energy calculation (resets at year start)
- [ ] Verify GHI daily aggregation
- [ ] Verify GHI MTD/YTD calculations
- [ ] Verify availability calculation (5-min)
- [ ] Verify daily availability percentage
- [ ] Verify MTD/YTD availability
- [ ] Verify PR calculation formula
- [ ] Verify unit conversions
- [ ] Test edge cases (meter reset, missing data)
- [ ] Compare with Excel calculations
- [ ] Validate data quality thresholds

---

**Last Updated**: [Current Date]
**Related Docs**: 
- `docs/POWER_BI_IMPLEMENTATION_PLAN.md` - Implementation plan
- `seeds/seed_metric_mapper.csv` - Metric definitions

