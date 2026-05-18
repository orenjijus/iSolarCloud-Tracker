# Power BI Implementation - Quick Reference

## Overview

This quick reference guide provides essential information for implementing Power BI dashboards based on the existing dbt data warehouse. For complete details, see `docs/POWER_BI_IMPLEMENTATION_PLAN.md`.

## Key Tables for Power BI

### Dimension Tables (Import Mode)
- `dim_assets` - Site and device information
- `dim_date_generated` - Date dimension for time intelligence

### Fact Tables (Current - LONG Format)
- `mart_inverter_performance_5min` - Raw inverter metrics
- `mart_meter_performance_5min` - Raw meter metrics
- `mart_sensor_measurements_5min` - Raw sensor metrics
- `mart_site_performance_daily` - Daily aggregated metrics
- `mart_simulation_targets_daily` - Target/KPI values

### Fact Tables (To Be Created - WIDE Format)
- `fact_site_performance_metrics_daily` - Calculated metrics with YTD/MTD
- `fact_site_performance_metrics_5min` - 5-minute detail metrics

## Hierarchical Drill-Down Flow

```
YTD Overview (All Sites)
    ↓ Click site
MTD Overview (All Sites)
    ↓ Click site
Site MTD (Single Site)
    ↓ Click underperforming day
Daily Performance (Single Site, Month)
    ↓ Click date
5-Minute Detail (Single Site, Single Day)
```

## Essential DAX Measures

### YTD Measures
```dax
Energy YTD Actual = 
    CALCULATE(
        SUM(fact_site_performance_metrics_daily[energy_ytd_actual_mwh]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )
```

### MTD Measures
```dax
Energy MTD Actual = 
    CALCULATE(
        SUM(fact_site_performance_metrics_daily[energy_mtd_actual_mwh]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )
```

### Daily Measures
```dax
Daily Energy Actual = 
    SUM(fact_site_performance_metrics_daily[energy_actual_mwh])

Is Underperforming = 
    IF([Daily Energy %] < 90, "Underperforming", "Normal")
```

## Power BI Model Relationships

```
dim_date_generated[date_key] → fact_site_performance_metrics_daily[date_key]
dim_date_generated[date_key] → fact_site_performance_metrics_5min[date_key]
dim_assets[asset_id] → fact_site_performance_metrics_daily[site_id]
dim_assets[asset_id] → fact_site_performance_metrics_5min[site_id]
```

## Storage Mode Recommendations

| Table | Mode | Reason |
|-------|------|--------|
| Dimensions | Import | Small, static |
| Daily Fact | Import | Aggregated, full history |
| 5-Min Fact | DirectQuery or Import (30 days) | Large, detail data |

## Performance Optimization

1. **Use Filters Early**: Date range → Site
2. **Configure Aggregations**: Daily → 5-Min auto-routing
3. **Index Strategy**: 
   - `(date_key, site_id)` for daily queries
   - `(site_id, date_key, timestamp_5min)` for 5-min queries

## Implementation Checklist

- [ ] Create fact tables (`fact_site_performance_metrics_daily`, `fact_site_performance_metrics_5min`)
- [ ] Connect Power BI to database
- [ ] Set up relationships
- [ ] Create DAX measures
- [ ] Build 5 dashboard pages (YTD → MTD → Site → Daily → 5-Min)
- [ ] Configure drill-through actions
- [ ] Test performance
- [ ] Validate data

## Documentation Reference

- **Full Plan**: `docs/POWER_BI_IMPLEMENTATION_PLAN.md`
- **SQL Reference**: `docs/FACT_TABLE_SQL_REFERENCE.md` (to be created)
- **Architecture**: `docs/architecture.md`

