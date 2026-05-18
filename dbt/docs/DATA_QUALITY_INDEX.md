# Data Quality Framework - Documentation Index

## Overview

This index provides quick access to all data quality framework documentation.

---

## Main Documentation

### 📘 [Data Quality Framework Implementation Plan](./DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md)
**Comprehensive implementation guide covering:**
- Architecture & components
- Implementation phases (4 weeks)
- Time-aware data quality rules
- Technical specifications
- Usage examples
- Monitoring & alerting
- Maintenance & governance

**Use this for**: Complete understanding of the framework, planning, and implementation

---

## Quick References

### ⚡ [Data Quality Quick Reference Guide](./DATA_QUALITY_QUICK_REFERENCE.md)
**Quick access to:**
- Common SQL queries
- Rule codes reference
- Time windows
- Severity levels
- Troubleshooting tips

**Use this for**: Daily operations, quick lookups, common tasks

---

## Implementation Tools

### ✅ [Data Quality Implementation Checklist](./DATA_QUALITY_IMPLEMENTATION_CHECKLIST.md)
**Step-by-step checklist for:**
- Phase 1: Foundation (Week 1)
- Phase 2: Core Validation Rules (Week 2)
- Phase 3: Metrics & Scoring (Week 3)
- Phase 4: Monitoring & Alerting (Week 4)
- Post-implementation tasks

**Use this for**: Tracking implementation progress, ensuring nothing is missed

---

## Key Concepts

### Time-Aware Validation
- **Daylight Hours**: 5 AM - 7 PM (Hour 5-19)
- **Zero values during daylight**: Flagged as issues
- **Zero values at night**: Expected, not flagged
- **Always-valid checks**: Negative energy, data gaps (24/7)

### Rule Categories
1. **Validity**: Data conforms to expected formats/ranges
2. **Consistency**: Data is consistent across sources
3. **Completeness**: All required data is present
4. **Accuracy**: Data is correct and true
5. **Timeliness**: Data is available when needed
6. **Uniqueness**: No duplicate records

### Severity Levels
- **Critical**: Immediate investigation required
- **Error**: Investigate within 24 hours
- **Warning**: Review in weekly report
- **Info**: Monitor trends

---

## File Structure

```
seeds/
  └── seed_data_quality_rules.csv          # Rule catalog (to be created)

models/marts/
  ├── mart_data_quality_flags.sql          # Flags table (to be created)
  ├── mart_data_quality_metrics.sql        # Quality scores (to be created)
  └── mart_data_quality_alerts.sql         # Critical alerts (to be created)

models/marts/
  └── schema.yml                           # dbt tests (to be updated)

docs/
  ├── DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md  # Main documentation
  ├── DATA_QUALITY_QUICK_REFERENCE.md                 # Quick reference
  ├── DATA_QUALITY_IMPLEMENTATION_CHECKLIST.md       # Implementation checklist
  └── DATA_QUALITY_INDEX.md                          # This file
```

---

## Getting Started

### For Project Managers
1. Read [Implementation Plan](./DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md) - Overview section
2. Review [Implementation Checklist](./DATA_QUALITY_IMPLEMENTATION_CHECKLIST.md) - Timeline
3. Assign resources for 4-week implementation

### For Data Engineers
1. Read [Implementation Plan](./DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md) - Technical sections
2. Follow [Implementation Checklist](./DATA_QUALITY_IMPLEMENTATION_CHECKLIST.md) - Step by step
3. Use [Quick Reference](./DATA_QUALITY_QUICK_REFERENCE.md) - Daily operations

### For Data Analysts
1. Read [Quick Reference](./DATA_QUALITY_QUICK_REFERENCE.md) - Common queries
2. Review [Implementation Plan](./DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md) - Usage examples
3. Learn quality score interpretation

### For Data Stewards
1. Read [Implementation Plan](./DATA_QUALITY_FRAMEWORK_IMPLEMENTATION_PLAN.md) - Maintenance section
2. Review [Quick Reference](./DATA_QUALITY_QUICK_REFERENCE.md) - Rule codes
3. Learn flag resolution workflow

---

## Related Documentation

- [Database Design](./database-design.md) - Overall database architecture
- [Architecture Overview](./architecture.md) - System architecture
- [MART Design Decision](../MART_DESIGN_DECISION.md) - Mart table design
- [5-Minute Metrics Design](../FACT_5MIN_CALCULATED_METRICS_DESIGN.md) - Calculated metrics

---

## Support & Questions

For questions or issues:
1. Check relevant documentation section
2. Review troubleshooting guides
3. Contact Data Engineering team

---

**Last Updated**: 2025-01-XX  
**Documentation Version**: 1.0

