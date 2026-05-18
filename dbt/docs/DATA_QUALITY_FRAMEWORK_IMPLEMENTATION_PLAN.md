# Data Quality Framework Implementation Plan

## Executive Summary

This document outlines the comprehensive data quality framework for the MMSR Solar Asset Management System. The framework implements industry best practices (DAMA framework) with time-aware validation rules that account for PV system operating hours (5 AM - 7 PM). This ensures accurate data quality monitoring while reducing false positives from expected nighttime zero values.

---

## Table of Contents

1. [Overview & Objectives](#overview--objectives)
2. [Architecture & Components](#architecture--components)
3. [Implementation Phases](#implementation-phases)
4. [Time-Aware Data Quality Rules](#time-aware-data-quality-rules)
5. [Technical Specifications](#technical-specifications)
6. [Usage Examples](#usage-examples)
7. [Monitoring & Alerting](#monitoring--alerting)
8. [Maintenance & Governance](#maintenance--governance)
9. [Appendix](#appendix)

---

## Overview & Objectives

### Business Objectives

1. **Ensure Data Reliability**: Identify and flag data quality issues that could impact business decisions
2. **Reduce False Positives**: Time-aware rules prevent flagging expected nighttime zero values
3. **Enable Proactive Monitoring**: Automated detection of data anomalies and equipment issues
4. **Support Asset Management**: Provide data quality metrics for asset performance analysis
5. **Compliance & Auditing**: Maintain audit trail of all data quality issues and resolutions

### Technical Objectives

1. **Rule-Driven Architecture**: Centralized rule catalog for easy maintenance and updates
2. **Scalable Design**: Support incremental processing and high-volume time-series data
3. **Performance Optimized**: Indexed tables and efficient queries for real-time monitoring
4. **Integration Ready**: Compatible with existing dbt models and PowerBI dashboards
5. **Extensible Framework**: Easy to add new rules and validation types

---

## Architecture & Components

### Framework Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Quality Framework                    │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Layer 1: Rules Definition                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │ seed_data_quality_rules.csv                        │    │
│  │ - Rule catalog with metadata                       │    │
│  │ - Time windows and validation logic                │    │
│  │ - Severity classification                           │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  Layer 2: Validation Engine                                 │
│  ┌────────────────────────────────────────────────────┐    │
│  │ mart_data_quality_flags.sql                         │    │
│  │ - Time-aware validation logic                       │    │
│  │ - Multi-source flag detection                       │    │
│  │ - Incremental processing                            │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  Layer 3: Metrics & Scoring                                 │
│  ┌────────────────────────────────────────────────────┐    │
│  │ mart_data_quality_metrics.sql                       │    │
│  │ - Daily quality scores per site                    │    │
│  │ - Issue type aggregations                           │    │
│  │ - Trend analysis                                    │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  Layer 4: Monitoring & Alerting                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │ mart_data_quality_alerts.sql                        │    │
│  │ - Critical issue alerts                            │    │
│  │ - Automated notifications                          │    │
│  │ - Resolution tracking                              │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  Layer 5: Testing & Validation                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │ models/marts/schema.yml                             │    │
│  │ - dbt automated tests                              │    │
│  │ - Range validations                                │    │
│  │ - Not null constraints                             │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Component Overview

| Component | Purpose | Materialization | Update Frequency |
|-----------|---------|-----------------|------------------|
| `seed_data_quality_rules.csv` | Rule catalog and metadata | Seed | Manual (as needed) |
| `mart_data_quality_flags` | Individual data quality flags | Incremental Table | After each ETL run |
| `mart_data_quality_metrics` | Daily quality scores per site | Table | Daily (after flags) |
| `mart_data_quality_alerts` | Critical issue alerts | Table | Real-time (on-demand) |
| `schema.yml` | Automated dbt tests | Tests | On dbt run/test |

---

## Implementation Phases

### Phase 1: Foundation (Week 1)

**Objective**: Set up core infrastructure and basic rules

**Tasks**:
1. ✅ Create `seeds/seed_data_quality_rules.csv` with initial rule catalog
2. ✅ Create `models/marts/mart_data_quality_flags.sql` with time-aware logic
3. ✅ Set up database schema and indexes
4. ✅ Test with sample data

**Deliverables**:
- Rules catalog with 10+ initial rules
- Flags table with time-aware validation
- Basic documentation

**Success Criteria**:
- Flags table successfully flags issues during daylight hours
- Zero values at night (7 PM - 5 AM) are not flagged
- Incremental processing works correctly

---

### Phase 2: Core Validation Rules (Week 2)

**Objective**: Implement all critical validation rules

**Tasks**:
1. ✅ Add sensor validation rules (GHI, POA, irradiance)
2. ✅ Add meter validation rules (active power, energy)
3. ✅ Add inverter validation rules (active power)
4. ✅ Add site-level PR validation rules
5. ✅ Add cross-metric validation (GHI-POA delta)

**Deliverables**:
- Complete rule catalog (13+ rules)
- All validation logic implemented
- Test cases for each rule type

**Success Criteria**:
- All critical rules operational
- Time-aware logic working for all applicable rules
- No false positives during nighttime

---

### Phase 3: Metrics & Scoring (Week 3)

**Objective**: Implement quality scoring and metrics

**Tasks**:
1. ✅ Create `mart_data_quality_metrics.sql`
2. ✅ Implement quality score calculation
3. ✅ Add trend analysis capabilities
4. ✅ Create summary views for reporting

**Deliverables**:
- Daily quality scores per site
- Issue type aggregations
- Quality trend analysis

**Success Criteria**:
- Quality scores calculated correctly
- Scores reflect actual data quality
- Metrics available for PowerBI dashboards

---

### Phase 4: Monitoring & Alerting (Week 4)

**Objective**: Set up monitoring and alerting infrastructure

**Tasks**:
1. ✅ Create `mart_data_quality_alerts.sql`
2. ✅ Set up automated dbt tests in `schema.yml`
3. ✅ Create monitoring dashboards (PowerBI)
4. ✅ Configure alert notifications (optional)

**Deliverables**:
- Alert table for critical issues
- Automated test suite
- Monitoring dashboard
- Alert configuration

**Success Criteria**:
- Critical issues automatically detected
- Alerts generated in real-time
- Dashboard provides actionable insights

---

## Time-Aware Data Quality Rules

### Concept

PV systems only generate power during daylight hours (approximately 5 AM - 7 PM). Zero values for GHI, POA, and active power are **expected and normal** outside these hours. The framework implements time-aware validation that:

- ✅ **Flags zero values** during daylight hours (5 AM - 7 PM) as potential issues
- ✅ **Ignores zero values** during nighttime (7 PM - 5 AM) as expected behavior
- ✅ **Always validates** certain rules (e.g., negative energy) regardless of time

### Time Window Configuration

| Rule Type | Time Window | Rationale |
|-----------|-------------|------------|
| GHI Zero | 5 AM - 7 PM | GHI should be > 0 during daylight |
| POA Zero | 5 AM - 7 PM | POA should be > 0 during daylight |
| Inverter Active Power Zero | 5 AM - 7 PM | Inverters should produce power during daylight |
| Meter Active Power Zero | 5 AM - 7 PM | Meters should record power during daylight |
| GHI-POA Delta | 5 AM - 7 PM | Comparison only meaningful during daylight |
| MIT High Irradiance | 5 AM - 7 PM | MIT condition only relevant during daylight |
| Negative Energy | 24/7 | Always invalid, regardless of time |
| PR Validation | Daily (not time-dependent) | Daily aggregated metric |

### Implementation Logic

```sql
-- Time-aware check example
CASE 
    WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 5 AND 19 THEN true
    ELSE false
END as is_daylight_hours

-- Only flag during daylight hours
WHERE is_daylight_hours = true
    AND metric_value = 0  -- This is now a problem during daylight
```

---

## Technical Specifications

### 1. Rules Catalog Schema

**File**: `seeds/seed_data_quality_rules.csv`

**Columns**:
- `rule_id`: Unique identifier
- `rule_code`: Short code (e.g., GHI_ZERO)
- `rule_name`: Human-readable name
- `rule_category`: Validity, Consistency, Completeness, Accuracy, Timeliness, Uniqueness
- `rule_type`: threshold, range, comparison, percentage, statistical, missing
- `severity`: critical, error, warning, info
- `is_active`: Boolean flag
- `description`: Rule description
- `validation_logic`: SQL logic description
- `remediation_guidance`: How to fix issues
- `applies_to_table`: Target table name
- `applies_to_metric`: Target metric name
- `time_window_start`: Hour (0-23) for time-aware rules
- `time_window_end`: Hour (0-23) for time-aware rules

### 2. Flags Table Schema

**File**: `models/marts/mart_data_quality_flags.sql`

**Key Columns**:
- `flag_id`: Primary key
- `timestamp`: 5-minute timestamp (NULL for daily flags)
- `date_key`: Date key
- `asset_id`: Asset identifier
- `site_name`: Site name
- `system`: Source system (isolarcloud/fusionsolar)
- `flag_code`: Rule code
- `flag_name`: Rule name
- `severity`: Issue severity
- `rule_category`: Rule category
- `flag_description`: Description
- `data_source`: sensor/meter/inverter/site_daily
- `metric_name`: Affected metric
- `metric_value`: Metric value that triggered flag
- `hour_of_day`: Hour (0-23) for time analysis
- `status`: open/resolved/acknowledged
- `resolved_at`: Resolution timestamp
- `resolved_by`: Resolver name
- `resolution_notes`: Resolution details
- `flagged_at`: When flag was created

**Indexes**:
- `(timestamp, asset_id, flag_code)` - Primary lookup
- `(date_key, flag_code)` - Daily aggregations
- `(flag_code, severity)` - Severity filtering
- `(status)` - Open issues
- `(timestamp)` - Time-based queries

### 3. Metrics Table Schema

**File**: `models/marts/mart_data_quality_metrics.sql`

**Key Columns**:
- `date_key`: Date
- `site_id`: Site identifier
- `site_name`: Site name
- `critical_issue_types`: Count of critical issue types
- `error_issue_types`: Count of error issue types
- `warning_issue_types`: Count of warning issue types
- `critical_flag_count`: Total critical flags
- `error_flag_count`: Total error flags
- `warning_flag_count`: Total warning flags
- `total_flag_count`: Total flags
- `data_quality_score`: Quality score (0-100)
- `expected_intervals_5min`: Expected 5-minute intervals
- `total_assets`: Total assets at site

**Quality Score Formula**:
```
Quality Score = 100 - ((critical_count * 10 + error_count * 5 + warning_count * 1) / expected_intervals / total_assets * 100)
```

### 4. Alerts Table Schema

**File**: `models/marts/mart_data_quality_alerts.sql`

**Key Columns**:
- `date_key`: Date
- `site_name`: Site name
- `flag_code`: Rule code
- `flag_name`: Rule name
- `severity`: Issue severity
- `issue_count`: Number of occurrences
- `first_occurrence`: First timestamp
- `last_occurrence`: Last timestamp
- `affected_assets`: Comma-separated asset IDs

---

## Usage Examples

### Example 1: Query Flags for a Specific Site

```sql
-- Get all open flags for a site today
SELECT 
    flag_code,
    flag_name,
    severity,
    COUNT(*) as flag_count,
    MIN(timestamp) as first_occurrence,
    MAX(timestamp) as last_occurrence
FROM mart.mart_data_quality_flags
WHERE site_name = 'Site ABC'
    AND date_key = CURRENT_DATE
    AND status = 'open'
GROUP BY flag_code, flag_name, severity
ORDER BY 
    CASE severity 
        WHEN 'critical' THEN 1 
        WHEN 'error' THEN 2 
        WHEN 'warning' THEN 3 
        ELSE 4 
    END,
    flag_count DESC;
```

### Example 2: Quality Score Trend Analysis

```sql
-- Quality score trend over last 30 days
SELECT 
    date_key,
    site_name,
    data_quality_score,
    critical_flag_count,
    error_flag_count,
    warning_flag_count,
    LAG(data_quality_score) OVER (PARTITION BY site_name ORDER BY date_key) as prev_score,
    data_quality_score - LAG(data_quality_score) OVER (PARTITION BY site_name ORDER BY date_key) as score_change
FROM mart.mart_data_quality_metrics
WHERE date_key >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY site_name, date_key DESC;
```

### Example 3: Time-of-Day Analysis

```sql
-- Analyze when issues occur (day vs night)
SELECT 
    CASE 
        WHEN hour_of_day BETWEEN 5 AND 19 THEN 'Daylight (5 AM - 7 PM)'
        ELSE 'Nighttime (7 PM - 5 AM)'
    END as time_period,
    flag_code,
    COUNT(*) as flag_count,
    COUNT(DISTINCT asset_id) as affected_assets
FROM mart.mart_data_quality_flags
WHERE date_key = CURRENT_DATE
    AND status = 'open'
    AND hour_of_day IS NOT NULL
GROUP BY 
    CASE 
        WHEN hour_of_day BETWEEN 5 AND 19 THEN 'Daylight (5 AM - 7 PM)'
        ELSE 'Nighttime (7 PM - 5 AM)'
    END,
    flag_code
ORDER BY flag_count DESC;
```

### Example 4: Join Flags with Original Data

```sql
-- Get sensor data with quality flags
SELECT 
    s.timestamp,
    s.asset_id,
    s.metric_name,
    s.metric_value,
    f.flag_code,
    f.flag_name,
    f.severity,
    f.flag_description
FROM mart.mart_sensor_measurements_5min s
LEFT JOIN mart.mart_data_quality_flags f
    ON s.timestamp = f.timestamp
    AND s.asset_id = f.asset_id
    AND f.status = 'open'
WHERE s.date_key = CURRENT_DATE
    AND s.metric_name = 'irradiance'
ORDER BY s.timestamp, s.asset_id;
```

### Example 5: Resolve a Flag

```sql
-- Mark a flag as resolved
UPDATE mart.mart_data_quality_flags
SET 
    status = 'resolved',
    resolved_at = CURRENT_TIMESTAMP,
    resolved_by = 'Data Engineer',
    resolution_notes = 'Sensor recalibrated and verified working'
WHERE flag_id = 12345;
```

---

## Monitoring & Alerting

### Automated Tests (dbt)

**File**: `models/marts/schema.yml`

```yaml
models:
  - name: mart_site_performance_daily
    description: "Daily site performance metrics"
    columns:
      - name: pr_ghi_actual
        description: "Performance Ratio based on GHI"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1.2
              inclusive: true
      - name: daily_energy_mwh
        description: "Daily energy production in MWh"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000
              inclusive: true
```

### Alert Configuration

**Critical Alerts** (Immediate notification):
- PR below 50% (fault condition)
- Negative energy values
- Critical sensor failures during daylight

**Warning Alerts** (Daily summary):
- PR between 50-60% (low performance)
- GHI-POA delta > 10%
- Multiple zero values during daylight

**Info Alerts** (Weekly summary):
- Quality score trends
- Issue type distributions
- Resolution rates

### Dashboard Metrics

**PowerBI Dashboard Sections**:

1. **Quality Score Overview**
   - Current quality score per site
   - 30-day trend
   - Score distribution

2. **Issue Summary**
   - Open issues by severity
   - Issues by type
   - Affected assets

3. **Time Analysis**
   - Issues by time of day
   - Daylight vs nighttime comparison
   - Peak issue times

4. **Resolution Tracking**
   - Open vs resolved issues
   - Average resolution time
   - Resolution rate trends

---

## Maintenance & Governance

### Rule Management

**Adding New Rules**:
1. Add row to `seeds/seed_data_quality_rules.csv`
2. Update `mart_data_quality_flags.sql` with validation logic
3. Test with sample data
4. Run `dbt seed` and `dbt run`
5. Verify flags are created correctly

**Deactivating Rules**:
1. Set `is_active = false` in rules catalog
2. Run `dbt seed`
3. Existing flags remain, but no new flags created

### Data Steward Workflow

1. **Daily Review**: Check quality metrics dashboard
2. **Issue Triage**: Prioritize critical and error issues
3. **Investigation**: Review flagged data and root cause
4. **Resolution**: Fix data source or mark as false positive
5. **Documentation**: Update resolution notes

### Performance Optimization

**Incremental Processing**:
- Flags table uses incremental materialization
- Only processes new data since last run
- Significantly faster for large datasets

**Index Strategy**:
- Indexes on frequently queried columns
- Composite indexes for common query patterns
- Regular index maintenance

**Query Optimization**:
- Use date_key for date filtering
- Limit timestamp ranges in queries
- Aggregate at appropriate granularity

---

## Appendix

### A. Complete Rule Catalog

| Rule Code | Rule Name | Severity | Time Window | Description |
|-----------|-----------|----------|-------------|-------------|
| GHI_ZERO | GHI Zero | Critical | 5 AM - 7 PM | GHI irradiance is zero during daylight |
| POA_ZERO | POA Zero | Critical | 5 AM - 7 PM | POA irradiance is zero during daylight |
| GHI_POA_DELTA | GHI-POA Delta High | Warning | 5 AM - 7 PM | Delta between GHI and POA exceeds 10% |
| PR_HIGH | PR Exceeds 100% | Error | N/A | Performance Ratio exceeds 100% |
| PR_FAULT | PR Below 50% | Critical | N/A | Performance Ratio below 50% (fault) |
| PR_LOW | PR Low Performance | Warning | N/A | Performance Ratio between 50-60% |
| METER_DELTA_NEG | Energy Delta Negative | Error | N/A | Delta between negative and positive energy is negative |
| METER_POWER_ZERO | Meter Active Power Zero | Warning | 5 AM - 7 PM | Meter active power is zero during daylight |
| INV_POWER_ZERO | Inverter Active Power Zero | Warning | 5 AM - 7 PM | Inverter active power is zero during daylight |
| MIT_HIGH_IRRADIANCE | MIT High Irradiance | Info | 5 AM - 7 PM | Irradiance exceeds 40 W/m² |
| ENERGY_NEGATIVE | Negative Energy | Critical | 24/7 | Energy values should never be negative |
| DATA_GAP | Data Gap Detected | Warning | N/A | Missing expected 5-minute interval |
| PR_OUTLIER | PR Statistical Outlier | Warning | N/A | PR value is statistical outlier |

### B. SQL Functions Reference

**Time-Aware Helper Function** (Future Enhancement):
```sql
CREATE OR REPLACE FUNCTION dq.is_daylight_hours(
    p_timestamp TIMESTAMPTZ,
    p_latitude NUMERIC DEFAULT NULL,
    p_longitude NUMERIC DEFAULT NULL
)
RETURNS BOOLEAN AS $$
BEGIN
    -- Simple implementation: 5 AM to 7 PM
    RETURN EXTRACT(HOUR FROM p_timestamp) BETWEEN 5 AND 19;
    
    -- Future: Calculate actual sunrise/sunset based on location
    -- Requires PostGIS or similar library
END;
$$ LANGUAGE plpgsql IMMUTABLE;
```

### C. dbt Commands Reference

```bash
# Seed rules catalog
dbt seed --select seed_data_quality_rules

# Run flags table
dbt run --select mart_data_quality_flags

# Run metrics table
dbt run --select mart_data_quality_metrics

# Run alerts table
dbt run --select mart_data_quality_alerts

# Run all data quality models
dbt run --select mart_data_quality_*

# Run tests
dbt test --select mart_data_quality_*

# Full refresh (if needed)
dbt run --select mart_data_quality_flags --full-refresh
```

### D. Troubleshooting Guide

**Issue**: Flags not being created
- Check if rules are active (`is_active = true`)
- Verify time window matches data timestamps
- Check incremental logic is working

**Issue**: Too many false positives
- Review time window settings
- Adjust validation thresholds
- Check for data source issues

**Issue**: Performance slow
- Verify indexes are created
- Check incremental processing
- Review query patterns

**Issue**: Missing flags
- Verify rule logic matches data
- Check time window configuration
- Review data completeness

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-XX | Data Engineering Team | Initial implementation plan |

---

## Related Documentation

- [Database Design](./database-design.md) - Overall database architecture
- [Architecture Overview](./architecture.md) - System architecture
- [MART Design Decision](./MART_DESIGN_DECISION.md) - Mart table design
- [5-Minute Metrics Design](./FACT_5MIN_CALCULATED_METRICS_DESIGN.md) - Calculated metrics

---

**Document Status**: Draft  
**Last Updated**: 2025-01-XX  
**Next Review**: After Phase 1 completion

