# Data Quality Framework - Quick Reference Guide

## Quick Start

### 1. Seed Rules Catalog
```bash
dbt seed --select seed_data_quality_rules
```

### 2. Run Flags Table
```bash
dbt run --select mart_data_quality_flags
```

### 3. Run Metrics Table
```bash
dbt run --select mart_data_quality_metrics
```

### 4. Check Quality Scores
```sql
SELECT 
    date_key,
    site_name,
    data_quality_score,
    critical_flag_count,
    error_flag_count,
    warning_flag_count
FROM mart.mart_data_quality_metrics
WHERE date_key = CURRENT_DATE
ORDER BY data_quality_score ASC;
```

---

## Common Queries

### Get Open Flags for Today
```sql
SELECT 
    flag_code,
    flag_name,
    severity,
    COUNT(*) as count
FROM mart.mart_data_quality_flags
WHERE date_key = CURRENT_DATE
    AND status = 'open'
GROUP BY flag_code, flag_name, severity
ORDER BY 
    CASE severity WHEN 'critical' THEN 1 WHEN 'error' THEN 2 ELSE 3 END,
    count DESC;
```

### Find Sites with Low Quality Scores
```sql
SELECT 
    site_name,
    data_quality_score,
    critical_flag_count,
    error_flag_count
FROM mart.mart_data_quality_metrics
WHERE date_key = CURRENT_DATE
    AND data_quality_score < 80
ORDER BY data_quality_score ASC;
```

### Check Time-Aware Flags (Daylight Hours Only)
```sql
SELECT 
    flag_code,
    COUNT(*) as flag_count,
    COUNT(DISTINCT asset_id) as affected_assets
FROM mart.mart_data_quality_flags
WHERE date_key = CURRENT_DATE
    AND status = 'open'
    AND hour_of_day BETWEEN 5 AND 19  -- Daylight hours
GROUP BY flag_code
ORDER BY flag_count DESC;
```

---

## Rule Codes Reference

| Code | Name | Severity | Time Window |
|------|------|----------|-------------|
| `GHI_ZERO` | GHI Zero | Critical | 5 AM - 7 PM |
| `POA_ZERO` | POA Zero | Critical | 5 AM - 7 PM |
| `GHI_POA_DELTA` | GHI-POA Delta High | Warning | 5 AM - 7 PM |
| `PR_HIGH` | PR Exceeds 100% | Error | N/A |
| `PR_FAULT` | PR Below 50% | Critical | N/A |
| `PR_LOW` | PR Low Performance | Warning | N/A |
| `METER_POWER_ZERO` | Meter Active Power Zero | Warning | 5 AM - 7 PM |
| `INV_POWER_ZERO` | Inverter Active Power Zero | Warning | 5 AM - 7 PM |
| `ENERGY_NEGATIVE` | Negative Energy | Critical | 24/7 |
| `MIT_HIGH_IRRADIANCE` | MIT High Irradiance | Info | 5 AM - 7 PM |

---

## Time Windows

**Daylight Hours**: 5 AM - 7 PM (Hour 5-19)
- GHI, POA, Active Power checks only during this window
- Zero values at night are expected and not flagged

**Always Active**: 24/7
- Negative energy values
- Data gaps
- Statistical outliers

**Daily Aggregates**: Not time-dependent
- PR calculations
- Daily energy totals

---

## Severity Levels

| Severity | Action Required | Example |
|----------|----------------|---------|
| **Critical** | Immediate investigation | PR < 50%, Negative energy |
| **Error** | Investigate within 24 hours | PR > 100%, Meter delta issues |
| **Warning** | Review in weekly report | PR 50-60%, GHI-POA delta |
| **Info** | Monitor trends | MIT high irradiance |

---

## File Structure

```
seeds/
  └── seed_data_quality_rules.csv          # Rule catalog

models/marts/
  ├── mart_data_quality_flags.sql          # Flags table
  ├── mart_data_quality_metrics.sql        # Quality scores
  └── mart_data_quality_alerts.sql         # Critical alerts

models/marts/
  └── schema.yml                           # dbt tests
```

---

## Troubleshooting

**Problem**: No flags being created
- ✅ Check `is_active = true` in rules catalog
- ✅ Verify time window matches data
- ✅ Check incremental logic

**Problem**: Too many flags
- ✅ Review time window (should be 5 AM - 7 PM)
- ✅ Check if nighttime data is being flagged (shouldn't be)
- ✅ Verify thresholds are appropriate

**Problem**: Missing flags
- ✅ Check rule logic matches data structure
- ✅ Verify metric names match
- ✅ Review time window configuration

---

## Quality Score Formula

```
Quality Score = 100 - (
    (critical_count * 10 + 
     error_count * 5 + 
     warning_count * 1) 
    / expected_intervals 
    / total_assets 
    * 100
)
```

**Score Interpretation**:
- 90-100: Excellent
- 80-89: Good
- 70-79: Fair
- < 70: Poor (needs attention)

---

## Contact & Support

For questions or issues:
1. Check full documentation: `DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md`
2. Review rule catalog: `seeds/seed_data_quality_rules.csv`
3. Contact Data Engineering team

---

**Last Updated**: 2025-01-XX

