# Meter Polarity Detection and Correction

## Problem Statement

Sometimes meter installations are incorrect, causing:
- `positive_active_energy` to actually measure **import/consumption** (should be negative)
- `negative_active_energy` to actually measure **generation/export** (should be positive)

This leads to incorrect energy calculations: `energy = positive - negative` becomes wrong if polarity is swapped.

## Solution: Automatic Detection + Manual Override

### Detection Logic

**Principle**: For solar sites, **generation should always be higher than consumption** over time.

**Detection Method**:
1. Compare cumulative MAX values of `positive_active_energy` vs `negative_active_energy` over a period (e.g., last 30 days)
2. If `MAX(positive_active_energy) < MAX(negative_active_energy)` → **Likely swapped**
3. If `MAX(positive_active_energy) > MAX(negative_active_energy)` → **Likely correct**

**Note**: This assumes the site is primarily generating (not consuming more than generating).

---

## 1. Automatic Detection Query

Run this query to detect meters with potentially swapped polarity:

```sql
-- Detect meters with potentially swapped polarity
-- Assumption: Generation should be higher than consumption for solar sites
WITH meter_max_values AS (
    SELECT 
        m.asset_id,
        m.asset_name,
        m.site_name,
        m.system,
        -- Get MAX values for both positive and negative energy
        MAX(CASE WHEN m.metric_name = 'positive_active_energy' THEN m.metric_value END) as max_positive,
        MAX(CASE WHEN m.metric_name = 'negative_active_energy' THEN m.metric_value END) as max_negative,
        -- Count readings to ensure sufficient data
        COUNT(DISTINCT CASE WHEN m.metric_name = 'positive_active_energy' THEN m.timestamp END) as positive_readings,
        COUNT(DISTINCT CASE WHEN m.metric_name = 'negative_active_energy' THEN m.timestamp END) as negative_readings,
        -- Date range
        MIN(m.timestamp) as first_reading,
        MAX(m.timestamp) as last_reading
    FROM {{ ref('mart_meter_performance_5min') }} m
    WHERE m.metric_name IN ('positive_active_energy', 'negative_active_energy')
        AND m.timestamp >= CURRENT_DATE - INTERVAL '30 days'  -- Last 30 days
    GROUP BY m.asset_id, m.asset_name, m.site_name, m.system
),
polarity_check AS (
    SELECT 
        asset_id,
        asset_name,
        site_name,
        system,
        max_positive,
        max_negative,
        positive_readings,
        negative_readings,
        first_reading,
        last_reading,
        -- Detection logic: if negative > positive, likely swapped
        CASE 
            WHEN max_positive IS NULL OR max_negative IS NULL THEN 'INSUFFICIENT_DATA'
            WHEN max_negative > max_positive THEN 'LIKELY_SWAPPED'
            WHEN max_positive > max_negative THEN 'LIKELY_CORRECT'
            ELSE 'UNCERTAIN'
        END as polarity_status,
        -- Calculate ratio for confidence
        CASE 
            WHEN max_positive > 0 AND max_negative > 0 THEN
                ROUND(max_negative / NULLIF(max_positive, 0)::NUMERIC, 2)
            ELSE NULL
        END as negative_to_positive_ratio
    FROM meter_max_values
    WHERE positive_readings > 0 AND negative_readings > 0  -- Both metrics must exist
)
SELECT 
    asset_id,
    asset_name,
    site_name,
    system,
    polarity_status,
    negative_to_positive_ratio,
    max_positive,
    max_negative,
    positive_readings,
    negative_readings,
    first_reading,
    last_reading,
    -- Recommendation
    CASE 
        WHEN polarity_status = 'LIKELY_SWAPPED' AND negative_to_positive_ratio > 1.5 THEN 
            'HIGH_CONFIDENCE: Set polarity_swapped=TRUE in seed_meter_config.csv for this meter'
        WHEN polarity_status = 'LIKELY_SWAPPED' AND negative_to_positive_ratio > 1.1 THEN 
            'MEDIUM_CONFIDENCE: Review manually, set polarity_swapped=TRUE in seed_meter_config.csv if confirmed'
        WHEN polarity_status = 'LIKELY_SWAPPED' THEN 
            'LOW_CONFIDENCE: Review manually'
        WHEN polarity_status = 'LIKELY_CORRECT' THEN 
            'No action needed'
        ELSE 
            'Review manually - insufficient data or uncertain'
    END as recommendation
FROM polarity_check
ORDER BY 
    CASE polarity_status
        WHEN 'LIKELY_SWAPPED' THEN 1
        WHEN 'UNCERTAIN' THEN 2
        WHEN 'INSUFFICIENT_DATA' THEN 3
        ELSE 4
    END,
    negative_to_positive_ratio DESC;
```

### Interpretation

- **LIKELY_SWAPPED**: `max_negative > max_positive` → Meter polarity is likely incorrect
- **LIKELY_CORRECT**: `max_positive > max_negative` → Meter polarity appears correct
- **INSUFFICIENT_DATA**: Missing one or both metrics → Cannot determine
- **UNCERTAIN**: Values are equal or very close → Manual review needed

**Confidence Levels**:
- **High**: Ratio > 1.5 (negative is 50%+ higher than positive) → Very likely swapped
- **Medium**: Ratio > 1.1 (negative is 10%+ higher than positive) → Possibly swapped
- **Low**: Ratio < 1.1 but still swapped → Review manually

---

## 2. Manual Override Configuration

Update `seeds/seed_meter_config.csv` to manually override polarity detection by setting the `polarity_swapped` column:

```csv
"Source","site_id","dev_name","esn_code","voltage_level","meter_type","polarity_swapped"
"iSolarCloud","1445767","RevenueMeter_ACComb","1445767_7_12_1","","Revenue","TRUE"
"FusionSolar","NE=50488260","Meter-MV","AM00102287046729","MV","Revenue","FALSE"
```

**Schema**:
- `polarity_swapped`: Set to `"TRUE"` if polarity is swapped, `"FALSE"` if correct, or leave empty (`""`) for auto-detection
- The `esn_code` column matches `device_ps_key` in meter data, which is used for joining

**Note**: All meter configuration is stored in `seed_meter_config.csv` - no separate override file needed.

---

## 3. Updated Calculation Logic

### 3.1 In Fact Tables

Update energy calculations to handle polarity swaps:

```sql
-- In fact_site_performance_metrics_5min and fact_site_performance_metrics_daily
-- Join with seed_meter_config to get polarity_swapped flag
WITH meter_polarity AS (
    SELECT 
        m.asset_id,
        -- Convert empty string to FALSE, "TRUE" to TRUE
        CASE 
            WHEN UPPER(TRIM(mc.polarity_swapped)) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as is_polarity_swapped
    FROM {{ ref('mart_meter_performance_5min') }} m
    LEFT JOIN {{ ref('seed_meter_config') }} mc
        ON m.device_ps_key = mc.esn_code  -- Join on device_ps_key = esn_code
    WHERE m.metric_name IN ('positive_active_energy', 'negative_active_energy')
    GROUP BY m.asset_id, mc.polarity_swapped
),
positive_energy AS (
    SELECT 
        m.timestamp,
        m.asset_id,
        m.metric_value as positive_value,
        p.is_polarity_swapped
    FROM {{ ref('mart_meter_performance_5min') }} m
    JOIN meter_polarity p ON m.asset_id = p.asset_id
    WHERE m.metric_name = 'positive_active_energy'
),
negative_energy AS (
    SELECT 
        m.timestamp,
        m.asset_id,
        m.metric_value as negative_value,
        p.is_polarity_swapped
    FROM {{ ref('mart_meter_performance_5min') }} m
    JOIN meter_polarity p ON m.asset_id = p.asset_id
    WHERE m.metric_name = 'negative_active_energy'
),
energy_corrected AS (
    SELECT 
        p.timestamp,
        p.asset_id,
        -- If swapped: swap the values in calculation
        CASE 
            WHEN p.is_polarity_swapped THEN
                COALESCE(n.negative_value, 0) - COALESCE(p.positive_value, 0)
            ELSE
                COALESCE(p.positive_value, 0) - COALESCE(n.negative_value, 0)
        END as energy_value
    FROM positive_energy p
    LEFT JOIN negative_energy n 
        ON p.timestamp = n.timestamp 
        AND p.asset_id = n.asset_id
)
SELECT * FROM energy_corrected;
```

### 3.2 Daily Aggregation with Polarity Correction

```sql
-- Daily energy with polarity correction
WITH meter_polarity AS (
    SELECT 
        m.asset_id,
        -- Convert empty string to FALSE, "TRUE" to TRUE
        CASE 
            WHEN UPPER(TRIM(mc.polarity_swapped)) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as is_polarity_swapped
    FROM {{ ref('mart_meter_performance_5min') }} m
    LEFT JOIN {{ ref('seed_meter_config') }} mc
        ON m.device_ps_key = mc.esn_code  -- Join on device_ps_key = esn_code
    WHERE m.metric_name IN ('positive_active_energy', 'negative_active_energy')
    GROUP BY m.asset_id, mc.polarity_swapped
),
daily_positive AS (
    SELECT 
        dd.date_key,
        m.asset_id,
        MAX(m.metric_value) as positive_max,
        FIRST_VALUE(m.metric_value) OVER (
            PARTITION BY m.asset_id, dd.date_key 
            ORDER BY m.timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as positive_first,
        p.is_polarity_swapped
    FROM {{ ref('mart_meter_performance_5min') }} m
    JOIN {{ ref('dim_date_generated') }} dd ON DATE(m.timestamp) = dd.date
    JOIN meter_polarity p ON m.asset_id = p.asset_id
    WHERE m.metric_name = 'positive_active_energy'
    GROUP BY dd.date_key, m.asset_id, m.timestamp, m.metric_value, p.is_polarity_swapped
),
daily_negative AS (
    SELECT 
        dd.date_key,
        m.asset_id,
        MAX(m.metric_value) as negative_max,
        FIRST_VALUE(m.metric_value) OVER (
            PARTITION BY m.asset_id, dd.date_key 
            ORDER BY m.timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as negative_first,
        p.is_polarity_swapped
    FROM {{ ref('mart_meter_performance_5min') }} m
    JOIN {{ ref('dim_date_generated') }} dd ON DATE(m.timestamp) = dd.date
    JOIN meter_polarity p ON m.asset_id = p.asset_id
    WHERE m.metric_name = 'negative_active_energy'
    GROUP BY dd.date_key, m.asset_id, m.timestamp, m.metric_value, p.is_polarity_swapped
),
daily_positive_agg AS (
    SELECT 
        date_key,
        asset_id,
        MAX(positive_max) - MIN(positive_first) as positive_energy_kwh,
        MAX(is_polarity_swapped) as is_polarity_swapped
    FROM daily_positive
    GROUP BY date_key, asset_id
),
daily_negative_agg AS (
    SELECT 
        date_key,
        asset_id,
        MAX(negative_max) - MIN(negative_first) as negative_energy_kwh,
        MAX(is_polarity_swapped) as is_polarity_swapped
    FROM daily_negative
    GROUP BY date_key, asset_id
)
SELECT 
    p.date_key,
    p.asset_id,
    -- Apply polarity correction: if swapped, reverse the calculation
    CASE 
        WHEN p.is_polarity_swapped THEN
            (COALESCE(n.negative_energy_kwh, 0) - COALESCE(p.positive_energy_kwh, 0)) / 1000.0
        ELSE
            (COALESCE(p.positive_energy_kwh, 0) - COALESCE(n.negative_energy_kwh, 0)) / 1000.0
    END as energy_actual_mwh
FROM daily_positive_agg p
LEFT JOIN daily_negative_agg n 
    ON p.date_key = n.date_key 
    AND p.asset_id = n.asset_id;
```

---

## 4. Workflow

### Step 1: Run Detection Query

Run the detection query above to identify meters with potentially swapped polarity.

### Step 2: Review Results

- **High Confidence Swapped**: Add to `seed_meter_polarity_override.csv` with `is_polarity_swapped=TRUE`
- **Medium/Low Confidence**: Review manually:
  - Check site configuration
  - Verify with site operators
  - Compare with inverter generation data
  - Add to override file if confirmed

### Step 3: Update Meter Config

Update `seeds/seed_meter_config.csv` to set `polarity_swapped` column:

```csv
"Source","site_id","dev_name","esn_code","voltage_level","meter_type","polarity_swapped"
"iSolarCloud","1445767","RevenueMeter_ACComb","1445767_7_12_1","","Revenue","TRUE"
"FusionSolar","NE=50488260","Meter-MV","AM00102287046729","MV","Revenue","FALSE"
```

**Note**: Find the meter by `esn_code` (matches `device_ps_key` in meter data) and set `polarity_swapped` to `"TRUE"` if swapped, `"FALSE"` if correct, or leave empty for auto-detection.

### Step 4: Update dbt Models

Update fact table models to use polarity override in calculations (see Section 3 above).

### Step 5: Re-run dbt

```bash
dbt run --select fact_site_performance_metrics_daily fact_site_performance_metrics_5min
```

### Step 6: Validate

Compare corrected energy values with:
- Inverter generation totals
- Expected generation based on capacity
- Previous manual calculations

---

## 5. Edge Cases

### Case 1: Site Consumes More Than Generates

**Problem**: Detection assumes generation > consumption, but some sites may consume more.

**Solution**: 
- Use manual override for these sites
- Or adjust detection logic to compare with inverter generation instead

### Case 2: Missing One Metric

**Problem**: Some meters only have `positive_active_energy` or only `negative_active_energy`.

**Solution**: 
- Cannot detect automatically
- Use manual override if known
- Or assume correct polarity if only one metric exists

### Case 3: Meter Reset During Period

**Problem**: Meter reset causes MAX values to be lower than expected.

**Solution**: 
- Detection uses MAX over 30 days, should handle resets
- If reset occurs, MAX will be post-reset value, which is still valid for comparison

---

## 6. Validation Queries

### Check Polarity Override Coverage

```sql
SELECT 
    COUNT(DISTINCT m.asset_id) as total_meters,
    COUNT(DISTINCT CASE WHEN UPPER(TRIM(mc.polarity_swapped)) = 'TRUE' THEN m.asset_id END) as meters_with_swapped_polarity,
    COUNT(DISTINCT CASE WHEN UPPER(TRIM(mc.polarity_swapped)) = 'FALSE' THEN m.asset_id END) as meters_confirmed_correct,
    COUNT(DISTINCT CASE WHEN COALESCE(TRIM(mc.polarity_swapped), '') = '' THEN m.asset_id END) as meters_using_auto_detection
FROM {{ ref('mart_meter_performance_5min') }} m
LEFT JOIN {{ ref('seed_meter_config') }} mc
    ON m.device_ps_key = mc.esn_code
WHERE m.metric_name IN ('positive_active_energy', 'negative_active_energy');
```

### Compare Energy Before/After Correction

```sql
-- Compare energy calculations with and without polarity correction
WITH energy_without_correction AS (
    -- Original calculation (assumes correct polarity)
    SELECT 
        date_key,
        site_id,
        SUM(energy_actual_mwh) as energy_original
    FROM fact_site_performance_metrics_daily_old  -- Before correction
    GROUP BY date_key, site_id
),
energy_with_correction AS (
    -- Corrected calculation
    SELECT 
        date_key,
        site_id,
        SUM(energy_actual_mwh) as energy_corrected
    FROM fact_site_performance_metrics_daily  -- After correction
    GROUP BY date_key, site_id
)
SELECT 
    o.date_key,
    o.site_id,
    o.energy_original,
    c.energy_corrected,
    c.energy_corrected - o.energy_original as difference,
    ROUND((c.energy_corrected - o.energy_original) / NULLIF(o.energy_original, 0) * 100, 2) as pct_change
FROM energy_without_correction o
JOIN energy_with_correction c 
    ON o.date_key = c.date_key 
    AND o.site_id = c.site_id
WHERE ABS(c.energy_corrected - o.energy_original) > 0.01  -- Significant difference
ORDER BY ABS(c.energy_corrected - o.energy_original) DESC;
```

---

## Summary

1. **Automatic Detection**: Compare MAX(positive) vs MAX(negative) - if negative > positive, likely swapped
2. **Manual Override**: Use `seed_meter_config.csv` `polarity_swapped` column to override detection results
   - Set to `"TRUE"` if swapped, `"FALSE"` if correct, or leave empty (`""`) for auto-detection
3. **Calculation Update**: Apply polarity swap in energy calculations: if swapped, use `negative - positive` instead of `positive - negative`
4. **Validation**: Compare corrected values with inverter generation and expected values

This approach provides both automatic detection (for efficiency) and manual override (for accuracy and edge cases), all managed in a single configuration file (`seed_meter_config.csv`).

