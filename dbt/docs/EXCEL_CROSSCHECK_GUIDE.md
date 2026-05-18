# Excel Crosscheck Guide - Daily Availability

> **Panduan & hasil crosscheck lengkap (daily, monthly, toleransi)**: [reports/excel-crosscheck/README.md](../../reports/excel-crosscheck/README.md). Dokumen ini fokus pada **availability dari fact tables** (query, export, banding dengan Excel).

## Overview

Dokumen ini menjelaskan cara melakukan crosscheck antara hasil dari fact tables dengan Excel untuk memastikan availability calculation benar.

---

## Data untuk Crosscheck

### Query 1: Daily Availability Summary

**File**: `analyses/crosscheck_with_excel.sql` - Query 1

**Output Columns**:
- `date_key`: Tanggal
- `site_name`: Nama site
- `power_available_hours`: Jam ketika power available (dalam hours)
- `unavailability_hours`: Jam ketika unavailability (dalam hours)
- `total_hours`: Total jam (hanya ketika MIT = 1)
- `availability_percent`: Persentase availability

**Cara Export**:
1. Jalankan query di database client
2. Export hasil ke CSV atau copy ke Excel
3. Bandingkan dengan Excel calculation

---

## Step-by-Step Crosscheck Process

### Step 1: Export Data dari Database

**Query untuk Export**:
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
WHERE date_key >= '2025-11-11'  -- Adjust date range as needed
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;
```

**Export ke Excel**:
- Copy hasil query ke Excel sheet baru
- Atau export langsung ke CSV dan buka di Excel

---

### Step 2: Export Data dari Excel

**Dari Excel**:
1. Export daily availability calculation untuk same date range
2. Pastikan kolom yang diexport:
   - Date
   - Site Name
   - Power Available Hours
   - Unavailability Hours
   - Total Hours
   - Availability Percent

---

### Step 3: Compare Results

**Bandingkan**:
1. **Availability Percent**: Apakah sama?
2. **Power Available Hours**: Apakah sama?
3. **Unavailability Hours**: Apakah sama?
4. **Total Hours**: Apakah sama?

**Jika ada perbedaan**:
- Cek inverter mana yang dihitung
- Cek MIT calculation (apakah menggunakan GHI atau POA fallback)
- Cek apakah semua inverters terhitung

---

## Detailed Comparison for Sites with < 100% Availability

### Sites to Focus On

Berdasarkan validasi, sites berikut memiliki availability < 100%:
- **PT. MMKI 1.75 MWp - Painting Building**: ~96-99%
- **PT. MMKI 4.292 MWP - Phase 3**: ~57-96%
- **PT. MMKI 5.7 MWp - Phase 2**: ~96-98%

### Query untuk Detailed Analysis

**File**: `analyses/crosscheck_with_excel.sql` - Query 2, 3, 4

**Query 2**: 5-minute detail data
**Query 3**: Inverter-level detail
**Query 4**: MIT calculation detail

---

## Common Issues to Check

### 1. Inverter Count Mismatch

**Problem**: Jumlah inverter di database berbeda dengan Excel

**Check**:
```sql
SELECT 
    date_key,
    site_name,
    COUNT(DISTINCT inverter_id) as total_inverters
FROM mart.fact_inverter_calculations_5min
WHERE date_key = '2025-11-16'  -- Adjust date
GROUP BY date_key, site_name;
```

**Solution**: Pastikan semua inverters terdaftar di `dim_assets`

---

### 2. MIT Calculation Mismatch

**Problem**: MIT calculation berbeda antara database dan Excel

**Check**:
```sql
SELECT 
    date_key,
    site_name,
    mit_irradiance_source,
    COUNT(*) as intervals,
    SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) as mit_1_count,
    SUM(CASE WHEN mit = 0 THEN 1 ELSE 0 END) as mit_0_count
FROM mart.fact_sensor_calculations_5min
WHERE date_key = '2025-11-16'  -- Adjust date
GROUP BY date_key, site_name, mit_irradiance_source;
```

**Possible Causes**:
- Excel menggunakan GHI, database menggunakan POA fallback
- Excel menggunakan manual lookup, database menggunakan automatic fallback
- Threshold berbeda (database: > 40 W/m²)

---

### 3. POA Fallback Usage

**Problem**: Excel manual switch, database automatic

**Check**:
```sql
SELECT 
    date_key,
    site_name,
    COUNT(*) as intervals_using_poa
FROM mart.fact_sensor_calculations_5min
WHERE mit_irradiance_source = 'POA'
    AND date_key = '2025-11-16'  -- Adjust date
GROUP BY date_key, site_name;
```

**Solution**: 
- Jika Excel manual switch terlambat atau miss, database akan lebih akurat
- Pastikan Excel juga menggunakan POA fallback pada waktu yang sama

---

### 4. Availability Calculation Formula

**Database Formula**:
```sql
availability_percent = (power_available_hours / total_hours) * 100
WHERE
    power_available_hours = SUM(power_available_ratio) / 12.0
    unavailability_hours = SUM(unavailability_ratio) / 12.0
    total_hours = power_available_hours + unavailability_hours
```

**Excel Formula**: Pastikan sama dengan database

---

## Comparison Template

### Excel Template untuk Comparison

| Date | Site Name | DB Power Available Hours | Excel Power Available Hours | Diff | DB Availability % | Excel Availability % | Diff % |
|------|-----------|-------------------------|----------------------------|------|-------------------|---------------------|--------|
| 2025-11-16 | PT. MMKI 1.75 MWp | 6.2396 | ? | ? | 98.68 | ? | ? |
| 2025-11-16 | PT. MMKI 4.292 MWP | 4.5470 | ? | ? | 75.43 | ? | ? |
| ... | ... | ... | ... | ... | ... | ... | ... |

---

## Validation Results dari Database

### Recent 7 Days (2025-11-11 to 2025-11-17)

**Sites dengan Availability < 100%**:

| Date | Site | Power Available Hours | Unavailability Hours | Total Hours | Availability % |
|------|------|----------------------|---------------------|-------------|----------------|
| 2025-11-17 | PT. MMKI 4.292 MWP - Phase 3 | 0.5073 | 0.3750 | 0.8823 | 57.50% |
| 2025-11-16 | PT. MMKI 1.75 MWp - Painting Building | 6.2396 | 0.0833 | 6.3229 | 98.68% |
| 2025-11-16 | PT. MMKI 4.292 MWP - Phase 3 | 4.5470 | 1.4808 | 6.0278 | 75.43% |
| 2025-11-16 | PT. MMKI 5.7 MWp - Phase 2 | 6.2300 | 0.1569 | 6.3869 | 97.54% |
| 2025-11-15 | PT. MMKI 1.75 MWp - Painting Building | 12.1146 | 0.4167 | 12.5313 | 96.67% |
| 2025-11-15 | PT. MMKI 4.292 MWP - Phase 3 | 12.1458 | 0.4201 | 12.5660 | 96.66% |
| 2025-11-15 | PT. MMKI 5.7 MWp - Phase 2 | 12.2843 | 0.5049 | 12.7892 | 96.05% |

**Gunakan data ini untuk comparison dengan Excel**.

---

## Troubleshooting

### Jika Availability Percent Berbeda

1. **Cek Inverter Count**:
   - Apakah jumlah inverter sama?
   - Apakah semua inverter terhitung?

2. **Cek MIT Calculation**:
   - Apakah MIT sama?
   - Apakah menggunakan sumber irradiance yang sama (GHI/POA)?

3. **Cek POA Fallback**:
   - Apakah Excel menggunakan POA fallback pada waktu yang sama?
   - Apakah Excel manual switch terlambat atau miss?

4. **Cek Formula**:
   - Apakah formula di Excel sama dengan database?
   - Apakah total_hours hanya menghitung waktu ketika MIT = 1?

---

## Next Steps

1. ✅ Export data dari database (query di atas)
2. ⚠️ Export data dari Excel untuk same date range
3. ⚠️ Compare results side by side
4. ⚠️ Document any differences
5. ⚠️ Investigate and fix if needed

---

**See Also**:
- [FACT_TABLES_VALIDATION_ANALYSIS.md](./FACT_TABLES_VALIDATION_ANALYSIS.md) - Detailed validation results
- [crosscheck_with_excel.sql](../analyses/crosscheck_with_excel.sql) - All crosscheck queries

