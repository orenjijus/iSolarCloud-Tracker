# Mart Tables vs Fact Tables: Use Cases for Power BI

## Quick Answer

**Mart Tables** = **Raw metric exploration** (flexible, ad-hoc analysis)
**Fact Tables** = **Pre-calculated KPIs** (dashboard metrics, performance)

Both are for Power BI, but serve **different purposes**.

---

## The Key Difference

### Mart Tables (LONG Format)
```
mart_inverter_performance_5min:
  timestamp | site_id | metric_name | metric_value
  00:00 | FS_123 | active_power | 100.5
  00:00 | FS_123 | voltage | 220.0
  00:00 | FS_123 | current | 0.5
```

**Structure**: One row per metric per timestamp
**Use Case**: "Show me ALL metrics" or "What metrics are available?"

### Fact Tables (WIDE Format)
```
fact_site_performance_metrics_daily:
  date_key | site_id | energy_actual_mwh | pr_ghi_percent | availability_percent
  2025-09-01 | FS_123 | 90.2 | 72.3 | 96.5
```

**Structure**: One row per site per day with calculated columns
**Use Case**: "Show me energy YTD" or "What's the PR?"

---

## When to Use Mart Tables

### ✅ Use Mart Tables For:

#### 1. **Raw Metric Exploration**
**Question**: "What metrics are available for this inverter?"
```dax
-- Power BI: Show all metrics
Metric Explorer = 
    SUMMARIZE(
        mart_inverter_performance_5min,
        mart_inverter_performance_5min[metric_name],
        "Value", SUM(mart_inverter_performance_5min[metric_value])
    )
```

**Use Case**: 
- Engineers exploring raw data
- Debugging sensor issues
- Finding which metrics exist
- Ad-hoc analysis

#### 2. **Individual Metric Analysis**
**Question**: "Show me voltage over time"
```dax
-- Power BI: Filter by metric_name
Voltage Over Time = 
    CALCULATE(
        SUM(mart_inverter_performance_5min[metric_value]),
        mart_inverter_performance_5min[metric_name] = "voltage"
    )
```

**Use Case**:
- Analyzing specific metrics
- Comparing metrics side-by-side
- Technical troubleshooting

#### 3. **Dynamic Metric Selection**
**Question**: "Let user choose which metric to display"
```dax
-- Power BI: User selects metric from slicer
Selected Metric = 
    CALCULATE(
        SUM(mart_inverter_performance_5min[metric_value]),
        mart_inverter_performance_5min[metric_name] = SELECTEDVALUE(MetricSlicer[metric_name])
    )
```

**Use Case**:
- Flexible dashboards
- User-driven analysis
- Metric comparison tools

#### 4. **Device-Level Analysis**
**Question**: "Show me all inverters' power readings"
```dax
-- Power BI: Group by asset
Inverter Power = 
    CALCULATE(
        SUM(mart_inverter_performance_5min[metric_value]),
        mart_inverter_performance_5min[metric_name] = "active_power"
    )
```

**Use Case**:
- Device performance comparison
- Identifying underperforming devices
- Technical analysis

#### 5. **New Metrics Discovery**
**Question**: "What new metrics were added?"
```sql
-- SQL: Find new metrics
SELECT DISTINCT metric_name 
FROM mart_inverter_performance_5min
WHERE metric_name NOT IN (SELECT metric_name FROM fact_site_performance_metrics_daily)
```

**Use Case**:
- Discovering new metrics
- Testing new calculations
- Flexible schema (no code changes needed)

---

## When to Use Fact Tables

### ✅ Use Fact Tables For:

#### 1. **Executive Dashboards**
**Question**: "Show me energy YTD vs target"
```dax
-- Power BI: Simple measure
Energy YTD = SUM(fact_site_performance_metrics_daily[energy_ytd_actual_mwh])
```

**Use Case**:
- KPI dashboards
- Executive reports
- Performance summaries

#### 2. **Pre-Calculated Metrics**
**Question**: "What's the Performance Ratio?"
```dax
-- Power BI: Direct column
PR GHI = AVERAGE(fact_site_performance_metrics_daily[pr_ghi_percent])
```

**Use Case**:
- Business metrics (PR, Availability, Energy)
- Pre-aggregated values (YTD, MTD)
- Standard KPIs

#### 3. **Hierarchical Drill-Down**
**Question**: "YTD → MTD → Daily → 5-Min"
```dax
-- Power BI: Use fact tables at each level
-- YTD: fact_site_performance_metrics_daily[energy_ytd_actual_mwh]
-- Daily: fact_site_performance_metrics_daily[energy_actual_mwh]
-- 5-Min: fact_site_performance_metrics_5min[energy_delta_mwh]
```

**Use Case**:
- Standard drill-down flow
- Performance dashboards
- User-friendly navigation

#### 4. **Complex Calculations**
**Question**: "Show me energy delta from cumulative readings"
```sql
-- SQL: Pre-calculated in fact table
energy_delta_mwh = metric_value - LAG(metric_value) OVER (...)
```

**Use Case**:
- Expensive calculations (LAG, window functions)
- Multi-table joins (meter + sensor + inverter)
- Performance optimization

#### 5. **Consistent Business Logic**
**Question**: "How is energy calculated?"
```sql
-- SQL: Standardized calculation in fact table
-- Everyone uses the same logic
```

**Use Case**:
- Standardized metrics
- Consistent calculations
- Single source of truth for KPIs

---

## Real-World Scenarios

### Scenario 1: Executive Dashboard
**User**: CEO wants to see "Energy YTD vs Target"

**Solution**: Use **Fact Table**
- `fact_site_performance_metrics_daily[energy_ytd_actual_mwh]`
- Simple, fast, pre-calculated
- No filtering needed

**Why NOT Mart?**
- Would need to filter by metric_name
- Would need to calculate YTD in DAX (slow)
- Complex measure

---

### Scenario 2: Engineer Troubleshooting
**User**: Engineer wants to see "All voltage metrics for inverter FS_123"

**Solution**: Use **Mart Table**
- `mart_inverter_performance_5min` WHERE `metric_name LIKE '%voltage%'`
- Flexible, shows all metrics
- Can explore different metrics

**Why NOT Fact?**
- Fact table doesn't have individual metrics
- Fact table only has calculated KPIs
- Not flexible enough

---

### Scenario 3: Performance Dashboard
**User**: Operations team wants "YTD → MTD → Daily → 5-Min drill-down"

**Solution**: Use **Fact Tables**
- Daily: `fact_site_performance_metrics_daily`
- 5-Min: `fact_site_performance_metrics_5min`
- Consistent structure
- Pre-calculated values

**Why NOT Mart?**
- Would need complex DAX for each level
- Performance issues
- Inconsistent structure

---

### Scenario 4: Ad-Hoc Analysis
**User**: Analyst wants to "Compare all metrics side-by-side"

**Solution**: Use **Mart Table**
- `mart_inverter_performance_5min` with metric_name slicer
- User can select any metric
- Flexible exploration

**Why NOT Fact?**
- Fact table only has specific metrics
- Can't dynamically select metrics
- Limited flexibility

---

## Power BI Model Structure

### Recommended Setup

```
┌─────────────────────────────────────┐
│ Dimensions (Import)                 │
│ - dim_assets                         │
│ - dim_date_generated                 │
└─────────────────────────────────────┘
           │
           ├──────────────────┬──────────────────┐
           │                  │                  │
┌──────────▼──────────┐ ┌─────▼──────────┐ ┌─────▼──────────┐
│ Fact Tables         │ │ Mart Tables    │ │ Other Tables  │
│ (Import/DirectQuery)│ │ (DirectQuery)  │ │ (Import)       │
│                     │ │                │ │                │
│ - Daily metrics     │ │ - Raw metrics  │ │ - Targets      │
│ - 5-min metrics     │ │ - All metrics  │ │ - Config       │
│                     │ │                │ │                │
│ USE FOR:            │ │ USE FOR:       │ │                │
│ - Dashboards        │ │ - Exploration  │ │                │
│ - KPIs              │ │ - Ad-hoc       │ │                │
│ - Drill-down        │ │ - Debugging    │ │                │
└─────────────────────┘ └────────────────┘ └────────────────┘
```

---

## Summary Table

| Aspect | Mart Tables | Fact Tables |
|--------|-------------|-------------|
| **Format** | LONG (metric_id rows) | WIDE (calculated columns) |
| **Purpose** | Raw metric exploration | Pre-calculated KPIs |
| **Use Case** | "Show me all metrics" | "Show me energy YTD" |
| **Flexibility** | High (dynamic metrics) | Low (fixed metrics) |
| **Performance** | Slower (filtering needed) | Faster (pre-calculated) |
| **Complexity** | Simple structure | Complex calculations |
| **Users** | Engineers, Analysts | Executives, Operations |
| **Dashboards** | Ad-hoc, exploration | Standard, KPI |
| **Power BI Mode** | DirectQuery | Import (daily) / DirectQuery (5-min) |

---

## Decision Tree

```
Need to show business KPIs?
├─ YES → Use Fact Tables
│   ├─ Energy YTD/MTD → fact_site_performance_metrics_daily
│   ├─ PR, Availability → fact_site_performance_metrics_daily
│   └─ 5-min detail → fact_site_performance_metrics_5min
│
└─ NO → Use Mart Tables
    ├─ Explore raw metrics → mart_inverter_performance_5min
    ├─ Compare metrics → mart_meter_performance_5min
    ├─ Debug sensors → mart_sensor_measurements_5min
    └─ Device analysis → mart_*_performance_5min
```

---

## Bottom Line

**Both are needed for Power BI:**

1. **Fact Tables** = **Standard dashboards** (80% of use cases)
   - Executive dashboards
   - KPI reports
   - Performance monitoring
   - Drill-down flows

2. **Mart Tables** = **Ad-hoc exploration** (20% of use cases)
   - Engineering analysis
   - Metric discovery
   - Troubleshooting
   - Flexible queries

**Think of it like:**
- **Fact Tables** = Restaurant menu (prepared dishes)
- **Mart Tables** = Raw ingredients (cook your own)

Both serve Power BI, but for different purposes!

---

**Last Updated**: [Current Date]
**Related Docs**: 
- `docs/POWER_BI_IMPLEMENTATION_PLAN.md` - Full implementation plan
- `docs/POWER_BI_QUICK_REFERENCE.md` - Quick reference

