# Excel Crosscheck - Comparison Table Template

## Instructions

1. Copy data dari database (query di bawah)
2. Paste ke Excel
3. Export data dari Excel untuk same date range
4. Compare side by side
5. Fill in "Excel" columns
6. Calculate differences

---

## Comparison Table Template

| Date | Site Name | DB Power Available Hours | DB Unavailability Hours | DB Total Hours | DB Availability % | Excel Power Available Hours | Excel Unavailability Hours | Excel Total Hours | Excel Availability % | Difference % | Notes |
|------|-----------|-------------------------|------------------------|----------------|-------------------|---------------------------|--------------------------|-------------------|---------------------|--------------|-------|
| 2025-11-16 | PT. MMKI 1.75 MWp - Painting Building | 6.2396 | 0.0833 | 6.3229 | 98.68 | ? | ? | ? | ? | ? | |
| 2025-11-16 | PT. MMKI 4.292 MWP - Phase 3 | 4.5470 | 1.4808 | 6.0278 | 75.43 | ? | ? | ? | ? | ? | |
| 2025-11-16 | PT. MMKI 5.7 MWp - Phase 2 | 6.2300 | 0.1569 | 6.3869 | 97.54 | ? | ? | ? | ? | ? | |
| 2025-11-15 | PT. MMKI 1.75 MWp - Painting Building | 12.1146 | 0.4167 | 12.5313 | 96.67 | ? | ? | ? | ? | ? | |
| 2025-11-15 | PT. MMKI 4.292 MWP - Phase 3 | 12.1458 | 0.4201 | 12.5660 | 96.66 | ? | ? | ? | ? | ? | |
| 2025-11-15 | PT. MMKI 5.7 MWp - Phase 2 | 12.2843 | 0.5049 | 12.7892 | 96.05 | ? | ? | ? | ? | ? | |
| 2025-11-12 | PT. MMKI 1.75 MWp - Painting Building | 12.0938 | 0.4063 | 12.5000 | 96.75 | ? | ? | ? | ? | ? | |
| 2025-11-12 | PT. MMKI 4.292 MWP - Phase 3 | 12.1111 | 0.4757 | 12.5868 | 96.22 | ? | ? | ? | ? | ? | |
| 2025-11-12 | PT. MMKI 5.7 MWp - Phase 2 | 12.3529 | 0.1618 | 12.5147 | 98.71 | ? | ? | ? | ? | ? | |

---

## Data dari Database (Copy ini ke Excel)

### Query untuk Export

```sql
SELECT 
    date_key,
    site_name,
    ROUND(SUM(power_available_ratio) / 12.0, 4) as power_available_hours,
    ROUND(SUM(unavailability_ratio) / 12.0, 4) as unavailability_hours,
    ROUND((SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0), 4) as total_hours,
    ROUND(
        CASE 
            WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
            THEN (SUM(power_available_ratio) / NULLIF(
                SUM(power_available_ratio) + SUM(unavailability_ratio),
                0
            )) * 100
            ELSE NULL
        END, 2
    ) as availability_percent
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-11-11'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;
```

---

## Sites dengan Availability < 100% (Priority untuk Crosscheck)

### 1. PT. MMKI 1.75 MWp - Painting Building

**Dates dengan Availability < 100%**:
- 2025-11-16: 98.68%
- 2025-11-15: 96.67%
- 2025-11-12: 96.75%

**Inverter Count**: 8-10 inverters (varies by date)

### 2. PT. MMKI 4.292 MWP - Phase 3

**Dates dengan Availability < 100%**:
- 2025-11-17: 57.50% (partial day)
- 2025-11-16: 75.43%
- 2025-11-15: 96.66%
- 2025-11-12: 96.22%

**Inverter Count**: 24 inverters
**Inverters Down (2025-11-17)**: 12 inverters (INV-13 to INV-24)

### 3. PT. MMKI 5.7 MWp - Phase 2

**Dates dengan Availability < 100%**:
- 2025-11-16: 97.54%
- 2025-11-15: 96.05%
- 2025-11-12: 98.71%

**Inverter Count**: 13-17 inverters (varies by date)

---

## What to Check in Excel

1. **Inverter Count**: Apakah jumlah inverter sama?
2. **MIT Calculation**: Apakah menggunakan sumber irradiance yang sama?
3. **POA Fallback**: Apakah Excel menggunakan POA fallback pada waktu yang sama?
4. **Formula**: Apakah formula di Excel sama dengan database?

---

## Expected Results

Jika semua benar, availability percent seharusnya **sama atau sangat dekat** (perbedaan < 0.1%).

Jika ada perbedaan signifikan (> 1%), cek:
1. Inverter count
2. MIT calculation
3. POA fallback timing
4. Formula

---

**See**: [EXCEL_CROSSCHECK_RESULTS.md](./EXCEL_CROSSCHECK_RESULTS.md) for complete data

