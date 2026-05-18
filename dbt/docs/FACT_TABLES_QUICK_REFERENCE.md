# Fact Tables Quick Reference Guide

## Quick Overview

| Fact Table | Grain | Purpose | Key Metrics |
|------------|-------|---------|-------------|
| `fact_sensor_calculations_5min` | `timestamp × sensor_id` | Sensor metrics dengan MIT calculation | `mit`, `mit_irradiance_source` |
| `fact_inverter_calculations_5min` | `timestamp × inverter_id` | Inverter availability | `inverter_availability`, `inverter_availability_with_mit` |
| `fact_site_calculations_5min` | `timestamp × site_id` | Site-level aggregation | `power_available_ratio`, `unavailability_ratio` |

---

## MIT Fallback Priority

```
Priority 1: GHI (pyranometer) dari site yang sama
    ↓ (jika tidak tersedia)
Priority 2: POA dari site yang sama
    ↓ (jika tidak tersedia)
Priority 3: GHI dari site lain (configured fallback)
```

**Track via**: `mit_irradiance_source` column in `fact_sensor_calculations_5min`

---

## Common Use Cases

### 1. Find Inverters That Are Down

```sql
SELECT 
    timestamp, site_name, inverter_id, inverter_name, active_power_kw
FROM mart.fact_inverter_calculations_5min
WHERE inverter_availability = 0
    AND date_key >= '2025-01-01'
ORDER BY timestamp DESC;
```

### 2. Calculate Daily Availability

```sql
SELECT 
    date_key,
    site_name,
    SUM(power_available_ratio) / 12.0 as power_available_hours,
    SUM(unavailability_ratio) / 12.0 as unavailability_hours,
    CASE 
        WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
        THEN (SUM(power_available_ratio) / NULLIF(
            SUM(power_available_ratio) + SUM(unavailability_ratio), 0
        )) * 100
        ELSE NULL
    END as availability_percent
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-01-01'
GROUP BY date_key, site_name;
```

### 3. Check POA Fallback Usage

```sql
SELECT date_key, site_name, COUNT(*) as intervals_using_poa
FROM mart.fact_sensor_calculations_5min
WHERE mit_irradiance_source = 'POA'
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name;
```

### 4. Site Availability per 5 Minutes

```sql
SELECT 
    timestamp, site_name, 
    total_inverters, available_inverters,
    power_available_ratio, unavailability_ratio, mit
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-01-01'
ORDER BY timestamp DESC;
```

---

## Build Commands

### Full Build
```bash
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min
```

### Re-process Date Range
```bash
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min \
  --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-01-31"}'
```

### Build Order (Important!)
1. `fact_sensor_calculations_5min` (first)
2. `fact_inverter_calculations_5min` (depends on sensor)
3. `fact_site_calculations_5min` (depends on inverter)

---

## Key Columns Reference

### fact_sensor_calculations_5min
- `mit` (INTEGER): 0 or 1
- `mit_irradiance_source` (VARCHAR): 'GHI', 'POA', or 'GHI_FALLBACK'
- `is_fallback` (BOOLEAN): true if GHI is from fallback

### fact_inverter_calculations_5min
- `inverter_availability` (INTEGER): 0 or 1
- `inverter_availability_with_mit` (INTEGER): 0 or 1
- `mit` (INTEGER): 0 or 1 (from sensor fact)

### fact_site_calculations_5min
- `power_available_ratio` (DECIMAL): 0-1
- `unavailability_ratio` (DECIMAL): 0-1
- `total_inverters` (INTEGER)
- `available_inverters` (INTEGER)

---

## Calculation Formulas

### MIT
```sql
mit = CASE WHEN irradiance_w_m2 > 40 THEN 1 ELSE 0 END
```

### Inverter Availability
```sql
inverter_availability = CASE WHEN active_power_kw > 0 THEN 1 ELSE 0 END
```

### Power Available Ratio
```sql
power_available_ratio = available_inverters / total_inverters
```

### Unavailability Ratio
```sql
unavailability_ratio = CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END
```

### Daily Availability Percent
```sql
availability_percent = (power_available_hours / total_hours) * 100
WHERE
    power_available_hours = SUM(power_available_ratio) / 12.0
    total_hours = power_available_hours + unavailability_hours
    unavailability_hours = SUM(unavailability_ratio) / 12.0
```

---

## Validation Queries Location

- **Full validation**: `analyses/validate_fact_tables.sql` (7 queries)
- **Quick checks**: `analyses/quick_validation.sql` (7 queries)

---

## Troubleshooting

### No Data in Fact Tables
1. Check if source tables have data: `mart_sensor_measurements_5min`, `mart_inverter_performance_5min`
2. Check if seeds are loaded: `seed_sensor_config`, `seed_sensor_site_mapping`
3. Check build logs for errors

### MIT Always 0
1. Check `mit_irradiance_source` - might be using fallback
2. Check if irradiance values are > 40 W/m²
3. Check if GHI/POA sensors have data

### Availability Mismatch with Excel
1. Check if all inverters are included
2. Check MIT calculation (should match Excel logic)
3. Check if POA fallback is working correctly
4. Compare daily aggregation formula

---

## Related Files

- **Models**: `dbt/models/facts/*.sql`
- **Validation**: `dbt/analyses/validate_fact_tables.sql`
- **Documentation**: 
  - `dbt/docs/FACT_5MIN_CALCULATED_METRICS_DESIGN.md`
  - `dbt/docs/FACT_TABLES_IMPLEMENTATION_SUMMARY.md`
  - `dbt/models/facts/README.md`

