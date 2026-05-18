# Power BI Implementation Plan

## Overview

This document outlines the complete implementation plan for Power BI dashboards using the existing dbt data warehouse architecture. The plan supports hierarchical drill-down analysis from Year-to-Date (YTD) overviews down to 5-minute detail metrics.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Model Structure](#data-model-structure)
3. [Fact Table Design](#fact-table-design)
4. [Power BI Model Setup](#power-bi-model-setup)
5. [DAX Measures](#dax-measures)
6. [Hierarchical Drill-Down Implementation](#hierarchical-drill-down-implementation)
7. [Performance Optimization](#performance-optimization)
8. [Implementation Checklist](#implementation-checklist)

---

## Architecture Overview

### Current Database Structure

```
Raw Tables (Python Loaded)
    ↓
Staging Layer (stg_*_perf_unpivoted)
    ↓
Intermediate Layer (int_*_unified)
    ↓
Mart Layer (mart_*_performance_5min, mart_*_performance_daily)
    ↓
Fact Tables (TO BE CREATED: fact_site_performance_metrics_*)
    ↓
Power BI Dashboards
```

### Key Principles

1. **Mart Tables (LONG format)**: Raw metric exploration
   - `mart_inverter_performance_5min`
   - `mart_meter_performance_5min`
   - `mart_sensor_measurements_5min`
   - `mart_string_performance_5min`

2. **Fact Tables (WIDE format)**: Pre-calculated business metrics
   - `fact_site_performance_metrics_daily` (to be created)
   - `fact_site_performance_metrics_5min` (to be created)

3. **Separation of Concerns**:
   - Mart = Enrichment (dimensions, standardized names)
   - Fact = Calculations (business metrics, KPIs)

---

## Data Model Structure

### Existing Tables (Ready for Power BI)

#### Dimension Tables

**dim_assets**
- `asset_id` (PK)
- `asset_name`
- `site_name`
- `asset_level` (Site/Device)
- `system` (fusionsolar/isolarcloud)

**dim_date_generated**
- `date_key` (PK)
- `year`
- `month`
- `month_name`
- `quarter`
- `day_of_year`
- `day_type`
- `season`

#### Fact Tables (Current - LONG Format)

**mart_inverter_performance_5min**
- Grain: `timestamp_5min × asset_id × metric_id`
- Columns: `timestamp_5min`, `asset_id`, `metric_id`, `metric_name`, `metric_value`
- Purpose: Raw metric exploration

**mart_meter_performance_5min**
- Grain: `timestamp × asset_id × metric_id`
- Columns: `timestamp`, `asset_id`, `metric_id`, `metric_name`, `metric_value`
- Purpose: Raw meter metric exploration

**mart_sensor_measurements_5min**
- Grain: `timestamp × asset_id × metric_id`
- Columns: `timestamp`, `asset_id`, `metric_id`, `metric_name`, `metric_value`
- Purpose: Raw sensor metric exploration

**mart_site_performance_daily**
- Grain: `date_key × site_id × metric_id`
- Columns: `date_key`, `site_id`, `metric_id`, `metric_name`, `avg_value`, `max_value`, `min_value`
- Purpose: Daily aggregated metrics

**mart_simulation_targets_daily**
- Grain: `date_key × site_id`
- Columns: `date_key`, `site_id`, `energy_target_mwh`, `energy_simulation_mwh`, `ghi`, `poa`
- Purpose: Target/KPI values for comparison

### Fact Tables (To Be Created - WIDE Format)

#### fact_site_performance_metrics_daily

**Purpose**: Pre-calculated daily performance metrics with YTD/MTD aggregations

**Grain**: `date_key × site_id` (ONE row per site per day)

**Columns**:
```sql
-- Identity
date_key (PK)
site_id (PK)
site_name
system

-- Date Attributes
year
month
month_name
quarter
day_of_year

-- Energy Metrics (Daily)
energy_actual_mwh
energy_target_mwh
energy_simulation_mwh
energy_actual_vs_target_pct

-- GHI Metrics (Daily)
ghi_actual_kwh_m2
ghi_target_kwh_m2
ghi_actual_vs_target_pct
ghi_vs_energy_diff_pct

-- Performance Ratio (Daily)
pr_ghi_percent
pr_poa_percent

-- Availability Metrics (Daily)
power_available_hours
total_hours
unavailability_hours
availability_percent

-- YTD Aggregations (Pre-calculated)
energy_ytd_actual_mwh
energy_ytd_target_mwh
energy_ytd_actual_vs_target_pct
ghi_ytd_actual_kwh_m2
ghi_ytd_target_kwh_m2
power_available_ytd_hours
total_ytd_hours
availability_ytd_percent

-- MTD Aggregations (Pre-calculated)
energy_mtd_actual_mwh
energy_mtd_target_mwh
energy_mtd_actual_vs_target_pct
ghi_mtd_actual_kwh_m2
ghi_mtd_target_kwh_m2
power_available_mtd_hours
total_mtd_hours
availability_mtd_percent
```

#### fact_site_performance_metrics_5min

**Purpose**: 5-minute detail metrics for drill-down analysis

**Grain**: `timestamp_5min × site_id` (ONE row per site per 5-minute interval)

**Columns**:
```sql
-- Identity
timestamp_5min (PK)
date_key
site_id (PK)
site_name

-- Date Attributes
year
month
month_name

-- Energy Metrics (5-min delta)
energy_delta_mwh

-- GHI Metrics (5-min)
ghi_kwh_m2

-- Availability Metrics (5-min)
active_power_kw
is_available (0 or 1)
```

---

## Fact Table Design

> **📖 Important**: For complete calculation flow documentation, see [`docs/CALCULATION_FLOW_5MIN_TO_YTD.md`](CALCULATION_FLOW_5MIN_TO_YTD.md)

### Calculation Logic

#### Energy Calculation (from Cumulative Meter Readings)

**Source**: `mart_meter_performance_5min` WHERE `metric_name = 'positive_active_energy'`

**Daily Calculation**:
```sql
energy_actual_mwh = 
    MAX(metric_value) - MIN(metric_value)  -- Delta for cumulative readings
    WHERE metric_name = 'positive_active_energy'
    AND date_key = [selected_date]
    GROUP BY site_id
```

**5-Minute Calculation**:
```sql
energy_delta_mwh = 
    metric_value - LAG(metric_value) OVER (
        PARTITION BY site_id 
        ORDER BY timestamp
    )
```

#### GHI Calculation (from Sensor Data)

**Source**: `mart_sensor_measurements_5min` WHERE `metric_name = 'daily_ghi'`

**Daily Calculation**:
```sql
ghi_actual_kwh_m2 = 
    SUM(metric_value)  -- Daily sum
    WHERE metric_name = 'daily_ghi'
    AND date_key = [selected_date]
    GROUP BY site_id
```

**5-Minute Calculation**:
```sql
ghi_kwh_m2 = metric_value  -- Direct value
```

#### Availability Calculation (from Inverter Power)

**Source**: `mart_inverter_performance_5min` WHERE `metric_name = 'active_power'`

**Daily Calculation**:
```sql
power_available_hours = 
    COUNT(CASE WHEN metric_value > 0 THEN 1 END) * 5.0 / 60.0
    WHERE metric_name = 'active_power'
    AND date_key = [selected_date]
    GROUP BY site_id

availability_percent = 
    (power_available_hours / total_hours) * 100
```

**5-Minute Calculation**:
```sql
is_available = CASE WHEN metric_value > 0 THEN 1 ELSE 0 END
```

#### Performance Ratio Calculation

**Daily Calculation**:
```sql
pr_ghi_percent = 
    CASE 
        WHEN ghi_target_kwh_m2 > 0 
        THEN (energy_actual_mwh / NULLIF(ghi_target_kwh_m2, 0)) * 100
        ELSE NULL
    END
```

#### YTD/MTD Aggregations (Window Functions)

**YTD Calculation**:
```sql
energy_ytd_actual_mwh = 
    SUM(energy_actual_mwh) OVER (
        PARTITION BY site_id, year 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
```

**MTD Calculation**:
```sql
energy_mtd_actual_mwh = 
    SUM(energy_actual_mwh) OVER (
        PARTITION BY site_id, year, month 
        ORDER BY date_key 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
```

### SQL Implementation (Reference - Not Executed)

See `docs/FACT_TABLE_SQL_REFERENCE.md` for complete SQL implementations.

---

## Power BI Model Setup

### 1. Data Source Connection

**Connection Type**: DirectQuery (recommended) or Import Mode

**Connection String**:
```
PostgreSQL Database
Server: [your_server]
Database: [your_database]
Schema: public
```

### 2. Table Relationships

```
┌─────────────────────────┐
│ dim_date_generated       │
│ PK: date_key            │
└──────────┬──────────────┘
           │
           │ 1:many
           ├──────────────────┐
           │                   │
┌──────────▼──────────────┐   │
│ fact_site_performance_   │   │
│ metrics_daily            │   │
│ FK: date_key            │   │
└──────────┬──────────────┘   │
           │                   │
           │ many:1            │
           ├──────────────────┐ │
           │                  │ │
┌──────────▼──────────────┐   │ │
│ dim_assets              │   │ │
│ PK: asset_id            │◄──┘ │
└─────────────────────────┘   │
                              │
                              │
┌─────────────────────────────┐
│ fact_site_performance_      │
│ metrics_5min                │
│ FK: date_key, site_id       │
└─────────────────────────────┘
```

**Relationship Configuration**:
- `dim_date_generated[date_key]` → `fact_site_performance_metrics_daily[date_key]` (Active)
- `dim_date_generated[date_key]` → `fact_site_performance_metrics_5min[date_key]` (Active)
- `dim_assets[asset_id]` → `fact_site_performance_metrics_daily[site_id]` (Active)
- `dim_assets[asset_id]` → `fact_site_performance_metrics_5min[site_id]` (Active)
- `dim_assets[asset_id]` → `mart_site_performance_daily[site_id]` (Active)
- `dim_date_generated[date_key]` → `mart_site_performance_daily[date_key]` (Active)

### 3. Storage Mode Configuration

**Recommended Setup**:

| Table | Storage Mode | Reason |
|-------|-------------|--------|
| `dim_date_generated` | Import | Small, static |
| `dim_assets` | Import | Small, static |
| `fact_site_performance_metrics_daily` | Import | Full history, aggregated |
| `fact_site_performance_metrics_5min` | DirectQuery (or Import last 30 days) | Large, detail data |
| `mart_site_performance_daily` | Import | Useful for raw metric exploration |
| `mart_*_performance_5min` | DirectQuery | Large, filtered by date |

**Hybrid Mode Setup**:
- Use **Composite Model** in Power BI
- Daily fact tables: Import Mode
- 5-minute tables: DirectQuery Mode (with date filters)

### 4. Power BI Aggregations Setup

**Purpose**: Automatically route queries to daily table when possible

**Configuration**:
1. Right-click `fact_site_performance_metrics_daily` → Manage Aggregations
2. Set up aggregation rules:

```
fact_site_performance_metrics_daily (Import)
    ↓ Aggregation
fact_site_performance_metrics_5min (DirectQuery)

Rules:
- If query filters by date_key AND site_id → Use daily table
- If query aggregates (SUM, AVG) → Use daily table
- If query requests individual timestamps → Use 5min table
```

**Aggregation Settings**:
- `energy_ytd_actual_mwh`: SUM → SUM
- `energy_mtd_actual_mwh`: SUM → SUM
- `availability_percent`: AVG → AVG
- `date_key`: GROUP BY → GROUP BY
- `site_id`: GROUP BY → GROUP BY

---

## DAX Measures

### YTD Overview Measures (All Sites)

```dax
-- Energy YTD Actual
Energy YTD Actual = 
    CALCULATE(
        SUM(fact_site_performance_metrics_daily[energy_ytd_actual_mwh]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )

-- Energy YTD Target
Energy YTD Target = 
    CALCULATE(
        SUM(fact_site_performance_metrics_daily[energy_ytd_target_mwh]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )

-- Energy YTD Actual vs Target %
Energy YTD % = 
    DIVIDE(
        [Energy YTD Actual],
        [Energy YTD Target],
        0
    ) * 100

-- GHI YTD Actual
GHI YTD Actual = 
    CALCULATE(
        SUM(fact_site_performance_metrics_daily[ghi_ytd_actual_kwh_m2]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )

-- Availability YTD %
Availability YTD % = 
    CALCULATE(
        AVERAGE(fact_site_performance_metrics_daily[availability_ytd_percent]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )
```

### MTD Overview Measures (All Sites)

```dax
-- Energy MTD Actual
Energy MTD Actual = 
    CALCULATE(
        SUM(fact_site_performance_metrics_daily[energy_mtd_actual_mwh]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )

-- Energy MTD Target
Energy MTD Target = 
    CALCULATE(
        SUM(fact_site_performance_metrics_daily[energy_mtd_target_mwh]),
        FILTER(
            ALLSELECTED(fact_site_performance_metrics_daily),
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )

-- Energy MTD Actual vs Target %
Energy MTD % = 
    DIVIDE(
        [Energy MTD Actual],
        [Energy MTD Target],
        0
    ) * 100
```

### Site-Level Measures (Single Site)

```dax
-- Site Energy MTD
Site Energy MTD = 
    CALCULATE(
        MAX(fact_site_performance_metrics_daily[energy_mtd_actual_mwh]),
        FILTER(
            fact_site_performance_metrics_daily,
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )

-- Site Energy YTD
Site Energy YTD = 
    CALCULATE(
        MAX(fact_site_performance_metrics_daily[energy_ytd_actual_mwh]),
        FILTER(
            fact_site_performance_metrics_daily,
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )

-- Site Availability MTD
Site Availability MTD % = 
    CALCULATE(
        MAX(fact_site_performance_metrics_daily[availability_mtd_percent]),
        FILTER(
            fact_site_performance_metrics_daily,
            fact_site_performance_metrics_daily[date_key] = 
                MAX(fact_site_performance_metrics_daily[date_key])
        )
    )
```

### Daily Performance Measures (Single Site, Month)

```dax
-- Daily Energy Actual
Daily Energy Actual = 
    SUM(fact_site_performance_metrics_daily[energy_actual_mwh])

-- Daily Energy vs Target %
Daily Energy % = 
    AVERAGE(fact_site_performance_metrics_daily[energy_actual_vs_target_pct])

-- Daily Availability
Daily Availability % = 
    AVERAGE(fact_site_performance_metrics_daily[availability_percent])

-- Daily PR GHI
Daily PR GHI % = 
    AVERAGE(fact_site_performance_metrics_daily[pr_ghi_percent])

-- Identify Underperforming Days
Is Underperforming = 
    IF(
        [Daily Energy %] < 90,  -- Threshold: 90%
        "Underperforming",
        "Normal"
    )
```

### 5-Minute Detail Measures (Single Site, Single Day)

```dax
-- 5-Min Energy Detail
5Min Energy Detail = 
    SUM(fact_site_performance_metrics_5min[energy_delta_mwh])

-- 5-Min GHI Detail
5Min GHI = 
    AVERAGE(fact_site_performance_metrics_5min[ghi_kwh_m2])

-- 5-Min Active Power
5Min Active Power = 
    AVERAGE(fact_site_performance_metrics_5min[active_power_kw])

-- 5-Min Availability Status
5Min Availability = 
    IF(
        SUM(fact_site_performance_metrics_5min[is_available]) > 0,
        "Available",
        "Unavailable"
    )
```

---

## Hierarchical Drill-Down Implementation

### UX Flow Design

```
Level 1: YTD Overview (All Sites)
    ↓ User clicks on site
Level 2: MTD Overview (All Sites)
    ↓ User clicks on site
Level 3: Site MTD (Single Site)
    ↓ User clicks on underperforming indicator
Level 4: Daily Performance (Single Site, Month)
    ↓ User clicks on underperforming day
Level 5: 5-Minute Detail (Single Site, Single Day)
```

### Page Structure

#### Page 1: YTD Overview Dashboard

**Visuals**:
- Matrix: Sites × Energy YTD Actual vs Target %
- Bar Chart: Sites × Energy YTD Actual (MWh)
- KPI Card: Total Energy YTD Actual
- KPI Card: Average Energy YTD %

**Filters**:
- Year slicer: `dim_date_generated[year]`
- Site slicer: `dim_assets[site_name]` (optional)

**Drill-Through Action**:
- Click on site → Navigate to "MTD Overview" page
- Pass filter: `site_id`

#### Page 2: MTD Overview Dashboard

**Visuals**:
- Matrix: Sites × Energy MTD Actual vs Target %
- Bar Chart: Sites × Energy MTD Actual (MWh)
- KPI Card: Total Energy MTD Actual
- KPI Card: Average Energy MTD %

**Filters**:
- Year slicer: `dim_date_generated[year]`
- Month slicer: `dim_date_generated[month]`
- Site slicer: `dim_assets[site_name]` (optional)

**Drill-Through Action**:
- Click on site → Navigate to "Site MTD" page
- Pass filter: `site_id`

#### Page 3: Site MTD Dashboard

**Visuals**:
- KPI Cards: Site Energy MTD, Site Energy YTD, Site Availability MTD %
- Line Chart: Daily Energy Actual vs Target (Month view)
- Table: Daily metrics with conditional formatting (red if < under daily target/simulation)
- Gauge: MTD Performance %

**Filters**:
- Site slicer: `dim_assets[site_name]` (pre-filtered from drill-through)
- Year slicer: `dim_date_generated[year]`
- Month slicer: `dim_date_generated[month]`

**Drill-Through Action**:
- Click on underperforming day → Navigate to "Daily Detail" page
- Pass filters: `site_id`, `date_key`

#### Page 4: Daily Performance Dashboard

**Visuals**:
- Line Chart: Daily Energy Actual vs Target (Month view)
- Bar Chart: Daily Availability % (Month view)
- Table: Daily metrics with conditional formatting
- Cards: Selected day metrics (Energy, PR, Availability)

**Filters**:
- Site slicer: `dim_assets[site_name]` (pre-filtered)
- Date slicer: `dim_date_generated[date_key]` (single day selection)
- Month slicer: `dim_date_generated[month]`

**Drill-Through Action**:
- Click on date → Navigate to "5-Minute Detail" page
- Pass filters: `site_id`, `date_key`

#### Page 5: 5-Minute Detail Dashboard

**Visuals**:
- Line Chart: Energy Delta (5-min intervals)
- Line Chart: GHI (5-min intervals)
- Line Chart: Active Power (5-min intervals)
- Area Chart: Availability Status (5-min intervals)
- Table: All 5-minute metrics

**Filters**:
- Site slicer: `dim_assets[site_name]` (pre-filtered)
- Date slicer: `dim_date_generated[date_key]` (single day)

**Drill-Through Action**:
- None (bottom level)

### Drill-Through Configuration

**Step 1**: Enable Drill-Through

1. Right-click visual → Enable Drill-Through
2. Set target page
3. Configure filter fields

**Step 2**: Configure Drill-Through Filters

For each drill-through:
- Source: `fact_site_performance_metrics_daily[site_id]` → Target: `dim_assets[site_id]`
- Source: `fact_site_performance_metrics_daily[date_key]` → Target: `dim_date_generated[date_key]`

**Step 3**: Pass Context Filters

Use DAX to pass filters:
```dax
-- In target page
Selected Site = 
    SELECTEDVALUE(dim_assets[site_name])

Selected Date = 
    SELECTEDVALUE(dim_date_generated[date_key])
```

---

## Performance Optimization

### 1. Query Performance by Level

| Level | Query Pattern | Rows Returned | Performance |
|-------|--------------|---------------|-------------|
| **YTD Overview** | `WHERE year = 2025` | ~16 sites × 1 row | **Fast** (indexed on year) |
| **MTD Overview** | `WHERE year = 2025 AND month = 9` | ~16 sites × 1 row | **Fast** (indexed on year, month) |
| **Site MTD** | `WHERE site_id = 'X' AND year = 2025 AND month = 9` | 1 site × 1 row | **Very Fast** (indexed on site_id, year, month) |
| **Daily Performance** | `WHERE site_id = 'X' AND year = 2025 AND month = 9` | 1 site × ~30 rows | **Fast** (indexed on site_id, date_key) |
| **5-Min Detail** | `WHERE site_id = 'X' AND date_key = '2025-09-15'` | 1 site × 1 day × 288 rows | **Very Fast** (indexed on site_id, date_key) |

### 2. Database Indexes (To Be Created)

```sql
-- For fact_site_performance_metrics_daily
CREATE INDEX IF NOT EXISTS idx_fact_daily_date_site 
ON fact_site_performance_metrics_daily(date_key, site_id);

CREATE INDEX IF NOT EXISTS idx_fact_daily_site_year_month 
ON fact_site_performance_metrics_daily(site_id, year, month);

CREATE INDEX IF NOT EXISTS idx_fact_daily_year_month 
ON fact_site_performance_metrics_daily(year, month);

-- For fact_site_performance_metrics_5min
CREATE INDEX IF NOT EXISTS idx_fact_5min_date_site_ts 
ON fact_site_performance_metrics_5min(date_key, site_id, timestamp_5min);

CREATE INDEX IF NOT EXISTS idx_fact_5min_site_date 
ON fact_site_performance_metrics_5min(site_id, date_key);

CREATE INDEX IF NOT EXISTS idx_fact_5min_ts 
ON fact_site_performance_metrics_5min(timestamp_5min DESC);
```

### 3. Power BI Query Optimization

**Use Filters Early**:
- Always filter by date range first
- Then filter by site
- Use slicers instead of visual-level filters

**Limit DirectQuery Data**:
- Only use DirectQuery for 5-minute detail
- Use Import mode for daily aggregations
- Set date range filters on DirectQuery tables

**Use Aggregations**:
- Configure Power BI aggregations for automatic query routing
- Use daily table for summaries
- Use 5-minute table only for detail drill-down

### 4. Incremental Refresh Strategy

**Daily Fact Table**:
- Full history: Import mode
- Refresh: Daily incremental load

**5-Minute Fact Table**:
- Recent data only: Last 30-90 days
- Refresh: Daily incremental load
- Older data: Archive or remove

---

## Implementation Checklist

### Phase 1: Database Setup

- [ ] Create `fact_site_performance_metrics_daily` table
  - [ ] Implement energy calculation logic
  - [ ] Implement GHI calculation logic
  - [ ] Implement availability calculation logic
  - [ ] Implement PR calculation logic
  - [ ] Implement YTD window functions
  - [ ] Implement MTD window functions
  - [ ] Create indexes

- [ ] Create `fact_site_performance_metrics_5min` table
  - [ ] Implement energy delta calculation
  - [ ] Implement GHI calculation
  - [ ] Implement availability calculation
  - [ ] Create indexes

- [ ] Verify data quality
  - [ ] Check for NULL values
  - [ ] Verify calculations match Excel
  - [ ] Test incremental loads

### Phase 2: Power BI Connection

- [ ] Connect to PostgreSQL database
- [ ] Import dimension tables (`dim_assets`, `dim_date_generated`)
- [ ] Import daily fact table (`fact_site_performance_metrics_daily`)
- [ ] Connect to 5-minute fact table (`fact_site_performance_metrics_5min`)
  - [ ] Configure as DirectQuery OR Import last 30 days

### Phase 3: Data Model Setup

- [ ] Create relationships
  - [ ] `dim_date_generated[date_key]` → `fact_site_performance_metrics_daily[date_key]`
  - [ ] `dim_date_generated[date_key]` → `fact_site_performance_metrics_5min[date_key]`
  - [ ] `dim_assets[asset_id]` → `fact_site_performance_metrics_daily[site_id]`
  - [ ] `dim_assets[asset_id]` → `fact_site_performance_metrics_5min[site_id]`

- [ ] Configure storage modes
  - [ ] Dimensions: Import
  - [ ] Daily fact: Import
  - [ ] 5-minute fact: DirectQuery or Import (filtered)

- [ ] Set up Power BI aggregations
  - [ ] Configure daily → 5-minute aggregation rules
  - [ ] Test aggregation routing

### Phase 4: DAX Measures

- [ ] Create YTD overview measures
  - [ ] Energy YTD Actual
  - [ ] Energy YTD Target
  - [ ] Energy YTD %
  - [ ] GHI YTD Actual
  - [ ] Availability YTD %

- [ ] Create MTD overview measures
  - [ ] Energy MTD Actual
  - [ ] Energy MTD Target
  - [ ] Energy MTD %
  - [ ] GHI MTD Actual
  - [ ] Availability MTD %

- [ ] Create site-level measures
  - [ ] Site Energy MTD
  - [ ] Site Energy YTD
  - [ ] Site Availability MTD %

- [ ] Create daily performance measures
  - [ ] Daily Energy Actual
  - [ ] Daily Energy %
  - [ ] Daily Availability %
  - [ ] Daily PR GHI %
  - [ ] Is Underperforming

- [ ] Create 5-minute detail measures
  - [ ] 5Min Energy Detail
  - [ ] 5Min GHI
  - [ ] 5Min Active Power
  - [ ] 5Min Availability

### Phase 5: Dashboard Creation

- [ ] Create YTD Overview page
  - [ ] Add visuals (matrix, bar chart, KPI cards)
  - [ ] Add slicers (year, site)
  - [ ] Configure drill-through to MTD Overview

- [ ] Create MTD Overview page
  - [ ] Add visuals (matrix, bar chart, KPI cards)
  - [ ] Add slicers (year, month, site)
  - [ ] Configure drill-through to Site MTD

- [ ] Create Site MTD page
  - [ ] Add visuals (KPI cards, line chart, table, gauge)
  - [ ] Add slicers (site, year, month)
  - [ ] Configure drill-through to Daily Performance

- [ ] Create Daily Performance page
  - [ ] Add visuals (line chart, bar chart, table, cards)
  - [ ] Add slicers (site, date, month)
  - [ ] Configure drill-through to 5-Minute Detail

- [ ] Create 5-Minute Detail page
  - [ ] Add visuals (line charts, area chart, table)
  - [ ] Add slicers (site, date)

### Phase 6: Testing & Optimization

- [ ] Test drill-down flow
  - [ ] YTD → MTD → Site MTD → Daily → 5-Min
  - [ ] Verify filters pass correctly
  - [ ] Verify data displays correctly

- [ ] Performance testing
  - [ ] Test query performance at each level
  - [ ] Optimize slow queries
  - [ ] Verify aggregation routing works

- [ ] Data validation
  - [ ] Compare Power BI results with Excel
  - [ ] Verify calculations are correct
  - [ ] Check for data quality issues

### Phase 7: Documentation & Training

- [ ] Document DAX measures
- [ ] Create user guide
- [ ] Create training materials
- [ ] Document drill-down flow

---

## SQL Reference Files

For complete SQL implementations and calculation flows, see:
- `docs/CALCULATION_FLOW_5MIN_TO_YTD.md` - **Complete calculation flow from 5-minute to YTD** ⭐
- `docs/FACT_TABLE_SQL_REFERENCE.md` (to be created)

---

## Notes

1. **No Code Changes Required**: This plan documents the implementation without modifying existing dbt code
2. **Fact Tables**: Will be created as new dbt models when implementation begins
3. **Incremental Strategy**: Both fact tables will use incremental materialization
4. **Performance**: Indexes and query optimization strategies are documented
5. **Flexibility**: Architecture supports both Import and DirectQuery modes

## Mart Tables vs Fact Tables

**Question**: "Why do we need both mart tables and fact tables for Power BI?"

**Answer**: They serve different purposes:

- **Mart Tables** (LONG format): Raw metric exploration, ad-hoc analysis, flexible queries
- **Fact Tables** (WIDE format): Pre-calculated KPIs, standard dashboards, performance metrics

See `docs/MART_VS_FACT_USE_CASES.md` for complete comparison and decision guide.

---

## Questions & Support

For questions about this implementation plan:
1. Review the architecture overview section
2. Check the SQL reference files
3. Consult the dbt documentation
4. Review existing mart table structures for reference

---

**Last Updated**: [Current Date]
**Version**: 1.0
**Status**: Planning Phase

