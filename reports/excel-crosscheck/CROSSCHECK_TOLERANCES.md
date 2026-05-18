# Cross-Check Tolerances

This document defines acceptable differences between database and Excel data due to rounding, calculation methods, or other acceptable variations.

**Last Updated**: [YYYY-MM-DD]

---

## Tolerance Levels

### Energy (MWh)

**Tolerance**: ±0.01 MWh

**Reason**: 
- Rounding differences in meter readings
- Different precision in calculations

**Example**:
- DB: 5.1234 MWh
- Excel: 5.12 MWh
- Difference: 0.0034 MWh → **Acceptable**

---

### GHI (kWh/m²)

**Tolerance**: ±0.01 kWh/m²

**Reason**:
- Rounding in sensor readings
- Unit conversion precision

**Example**:
- DB: 5.6789 kWh/m²
- Excel: 5.68 kWh/m²
- Difference: 0.0011 kWh/m² → **Acceptable**

---

### POA (kWh/m²)

**Tolerance**: ±0.01 kWh/m²

**Reason**:
- Rounding in sensor readings
- Weighted average calculation precision

**Example**:
- DB: 5.6789 kWh/m²
- Excel: 5.68 kWh/m²
- Difference: 0.0011 kWh/m² → **Acceptable**

---

### Availability (%)

**Tolerance**: ±1.0%

**Reason**:
- Rounding in percentage calculations
- Different aggregation methods (if applicable)

**Example**:
- DB: 95.67%
- Excel: 95.5%
- Difference: 0.17% → **Acceptable**

---

### Performance Ratio (PR)

**Tolerance**: ±0.001

**Reason**:
- Rounding in division calculations
- Precision in capacity values

**Example**:
- DB: 0.7890
- Excel: 0.789
- Difference: 0.0000 → **Acceptable**

---

## Special Cases

### Date-Specific Tolerances

**Case**: Data migration or system changes on specific dates
- **Dates**: [List dates]
- **Tolerance**: [Higher tolerance if applicable]
- **Reason**: [Explanation]

---

### Site-Specific Tolerances

**Case**: Sites with known calculation differences
- **Site**: [Site Name]
- **Metric**: [Metric]
- **Tolerance**: [Tolerance]
- **Reason**: [Explanation]

---

## When to Flag as Issue

Differences should be flagged for investigation if they exceed:
- **Energy**: > 0.01 MWh
- **GHI**: > 0.01 kWh/m²
- **POA**: > 0.01 kWh/m²
- **Availability**: > 1.0%
- **PR**: > 0.001

**Exception**: If the difference is consistent and explainable (e.g., known Excel error), document in `KNOWN_EXCEL_ISSUES.md` instead.

---

## Notes

- Tolerances may be adjusted based on data quality requirements
- Always investigate patterns, even if within tolerance (e.g., consistent 0.009 MWh difference)
- Document any exceptions to standard tolerances

