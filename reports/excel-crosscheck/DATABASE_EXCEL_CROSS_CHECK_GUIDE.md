# Panduan Cross-Check Database vs Excel - Site Daily Performance

## 📋 Overview

Dokumentasi ini menjelaskan cara melakukan cross-check data antara database (`mart_site_performance_daily`) dengan Excel report untuk memastikan konsistensi data. Excel digunakan sebagai **benchmark** (data yang dianggap benar).

## 🎯 Tujuan

1. **Validasi Data**: Memastikan data database sesuai dengan Excel
2. **Identifikasi Discrepancy**: Menemukan perbedaan antara database dan Excel
3. **Root Cause Analysis**: Mencari penyebab perbedaan (apakah di database atau Excel)
4. **Audit Trail**: Mendokumentasikan temuan untuk perbaikan

---

## 📊 Struktur Data

### Database Table: `mart_site_performance_daily`

Kolom utama yang akan dibandingkan:
- `date_key` - Tanggal
- `site_name` - Nama site
- `site_id` - ID site
- `daily_energy_mwh` - Energi harian (MWh)
- `daily_ghi_kwh_m2` - GHI harian (kWh/m²)
- `daily_poa_weighted_kwh_m2` - POA weighted harian (kWh/m²)
- `availability_percent` - Persentase availability
- `pr_ghi_actual` - Performance Ratio GHI
- `pr_poa_actual` - Performance Ratio POA
- `energy_target_mwh` - Target energi
- `energy_actual_vs_target_pct` - Persentase aktual vs target

### Excel Table Structure

Pastikan Excel memiliki kolom yang sama dengan database:
- Date (format: YYYY-MM-DD atau DD/MM/YYYY)
- Site Name
- Daily Energy (MWh)
- Daily GHI (kWh/m²)
- Daily POA (kWh/m²)
- Availability (%)
- PR GHI
- PR POA
- (kolom lainnya sesuai kebutuhan)

---

## 🔄 Workflow Cross-Check

### Step 1: Export Data dari Database ke CSV

Jalankan query berikut untuk export data database ke format CSV yang bisa dibandingkan dengan Excel:

```sql
-- ============================================
-- STEP 1: Export Database Data ke CSV
-- ============================================
-- Simpan hasil query ini sebagai CSV: database_daily_performance.csv
-- Format: CSV dengan header, delimiter comma

SELECT 
    date_key,
    site_name,
    site_id,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    daily_poa_weighted_kwh_m2,
    availability_percent,
    pr_ghi_actual,
    pr_poa_actual,
    energy_target_mwh,
    energy_actual_vs_target_pct,
    ghi_actual_vs_target_pct,
    poa_actual_vs_target_pct,
    actual_capacity_kw,
    system
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE date_key >= '2024-01-01'  -- Sesuaikan dengan range tanggal yang ingin dicek
    AND date_key <= CURRENT_DATE
ORDER BY site_name, date_key;
```

**Cara Export:**
1. Jalankan query di pgAdmin / DBeaver / tool SQL lainnya
2. Klik kanan hasil query → Export → CSV
3. Pilih opsi:
   - Header: Yes
   - Delimiter: Comma
   - Quote: Double Quote
4. Simpan sebagai `database_daily_performance.csv`

---

### Step 2: Import Excel Data ke Database (Temporary Table)

Buat temporary table untuk menyimpan data Excel agar bisa dibandingkan langsung di database:

```sql
-- ============================================
-- STEP 2: Buat Temporary Table untuk Excel Data
-- ============================================

-- Drop table jika sudah ada (untuk refresh data)
DROP TABLE IF EXISTS temp_excel_daily_performance;

-- Buat temporary table dengan struktur yang sesuai
CREATE TEMP TABLE temp_excel_daily_performance (
    date_key DATE,
    site_name VARCHAR(255),
    site_id VARCHAR(100),
    daily_energy_mwh NUMERIC(12, 4),
    daily_ghi_kwh_m2 NUMERIC(10, 4),
    daily_poa_weighted_kwh_m2 NUMERIC(10, 4),
    availability_percent NUMERIC(5, 2),
    pr_ghi_actual NUMERIC(5, 4),
    pr_poa_actual NUMERIC(5, 4),
    energy_target_mwh NUMERIC(12, 4),
    energy_actual_vs_target_pct NUMERIC(5, 2),
    ghi_actual_vs_target_pct NUMERIC(5, 2),
    poa_actual_vs_target_pct NUMERIC(5, 2),
    actual_capacity_kw NUMERIC(10, 2),
    system VARCHAR(50),
    notes TEXT  -- Untuk catatan tambahan
);

-- Import data Excel ke temporary table
-- Opsi 1: Jika Excel sudah di-export ke CSV
COPY temp_excel_daily_performance (
    date_key,
    site_name,
    site_id,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    daily_poa_weighted_kwh_m2,
    availability_percent,
    pr_ghi_actual,
    pr_poa_actual,
    energy_target_mwh,
    energy_actual_vs_target_pct,
    ghi_actual_vs_target_pct,
    poa_actual_vs_target_pct,
    actual_capacity_kw,
    system
)
FROM 'C:\path\to\excel_export.csv'  -- Ganti dengan path file CSV Excel Anda
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

-- Opsi 2: Manual INSERT (jika data sedikit)
-- INSERT INTO temp_excel_daily_performance VALUES (...);

-- Verifikasi data yang di-import
SELECT 
    COUNT(*) as total_rows,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT site_name) as total_sites
FROM temp_excel_daily_performance;
```

**Cara Import Excel ke CSV:**
1. Buka file Excel
2. Pastikan kolom sesuai dengan struktur table
3. File → Save As → CSV (Comma delimited)
4. Simpan sebagai `excel_daily_performance.csv`
5. Gunakan path lengkap file tersebut di query COPY

**Catatan:**
- Temporary table hanya ada selama session database aktif
- Jika session terputus, table akan hilang dan perlu dibuat ulang
- Untuk data yang sering digunakan, pertimbangkan membuat permanent table

---

### Step 3: Comparison Query - Identifikasi Discrepancy

Query ini membandingkan database vs Excel dan menampilkan semua perbedaan:

```sql
-- ============================================
-- STEP 3: Comparison Query - Identifikasi Discrepancy
-- ============================================
-- Query ini membandingkan database dengan Excel (Excel sebagai benchmark)
-- Menampilkan hanya data yang berbeda atau missing

WITH db_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent,
        pr_ghi_actual,
        pr_poa_actual,
        energy_target_mwh,
        energy_actual_vs_target_pct,
        ghi_actual_vs_target_pct,
        poa_actual_vs_target_pct,
        actual_capacity_kw,
        system
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE date_key >= (SELECT MIN(date_key) FROM temp_excel_daily_performance)
        AND date_key <= (SELECT MAX(date_key) FROM temp_excel_daily_performance)
),
excel_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent,
        pr_ghi_actual,
        pr_poa_actual,
        energy_target_mwh,
        energy_actual_vs_target_pct,
        ghi_actual_vs_target_pct,
        poa_actual_vs_target_pct,
        actual_capacity_kw,
        system
    FROM temp_excel_daily_performance
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        COALESCE(db.site_id, excel.site_id) as site_id,
        
        -- Database values
        db.daily_energy_mwh as db_energy_mwh,
        db.daily_ghi_kwh_m2 as db_ghi,
        db.daily_poa_weighted_kwh_m2 as db_poa,
        db.availability_percent as db_availability,
        db.pr_ghi_actual as db_pr_ghi,
        db.pr_poa_actual as db_pr_poa,
        db.energy_actual_vs_target_pct as db_energy_vs_target_pct,
        
        -- Excel values (benchmark)
        excel.daily_energy_mwh as excel_energy_mwh,
        excel.daily_ghi_kwh_m2 as excel_ghi,
        excel.daily_poa_weighted_kwh_m2 as excel_poa,
        excel.availability_percent as excel_availability,
        excel.pr_ghi_actual as excel_pr_ghi,
        excel.pr_poa_actual as excel_pr_poa,
        excel.energy_actual_vs_target_pct as excel_energy_vs_target_pct,
        
        -- Absolute differences
        COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0) as energy_diff_mwh,
        COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0) as ghi_diff,
        COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0) as poa_diff,
        COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0) as availability_diff,
        
        -- Percentage differences (relative to Excel - benchmark)
        CASE 
            WHEN excel.daily_energy_mwh > 0 
            THEN ((COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) / excel.daily_energy_mwh) * 100
            ELSE NULL
        END as energy_diff_pct,
        
        CASE 
            WHEN excel.daily_ghi_kwh_m2 > 0 
            THEN ((COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) / excel.daily_ghi_kwh_m2) * 100
            ELSE NULL
        END as ghi_diff_pct,
        
        CASE 
            WHEN excel.availability_percent > 0 
            THEN ((COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) / excel.availability_percent) * 100
            ELSE NULL
        END as availability_diff_pct,
        
        -- Status flags untuk setiap metric
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_energy_mwh IS NULL AND excel.daily_energy_mwh IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_energy_mwh IS NOT NULL AND excel.daily_energy_mwh IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as energy_status,
        
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_ghi_kwh_m2 IS NULL AND excel.daily_ghi_kwh_m2 IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_ghi_kwh_m2 IS NOT NULL AND excel.daily_ghi_kwh_m2 IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as ghi_status,
        
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_poa_weighted_kwh_m2 IS NULL AND excel.daily_poa_weighted_kwh_m2 IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_poa_weighted_kwh_m2 IS NOT NULL AND excel.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as poa_status,
        
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 'MISMATCH'  -- 1% tolerance
            WHEN db.availability_percent IS NULL AND excel.availability_percent IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.availability_percent IS NOT NULL AND excel.availability_percent IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as availability_status,
        
        -- Data source indicator
        CASE 
            WHEN db.date_key IS NULL THEN 'ONLY_IN_EXCEL'
            WHEN excel.date_key IS NULL THEN 'ONLY_IN_DB'
            ELSE 'BOTH'
        END as data_source
        
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    date_key,
    site_name,
    site_id,
    data_source,
    
    -- Energy comparison
    energy_status,
    db_energy_mwh,
    excel_energy_mwh,
    ROUND(energy_diff_mwh::numeric, 4) as energy_diff_mwh,
    ROUND(energy_diff_pct::numeric, 2) as energy_diff_pct,
    
    -- GHI comparison
    ghi_status,
    db_ghi,
    excel_ghi,
    ROUND(ghi_diff::numeric, 4) as ghi_diff,
    ROUND(ghi_diff_pct::numeric, 2) as ghi_diff_pct,
    
    -- POA comparison
    poa_status,
    db_poa,
    excel_poa,
    ROUND(poa_diff::numeric, 4) as poa_diff,
    
    -- Availability comparison
    availability_status,
    db_availability,
    excel_availability,
    ROUND(availability_diff::numeric, 2) as availability_diff,
    ROUND(availability_diff_pct::numeric, 2) as availability_diff_pct,
    
    -- PR comparison
    db_pr_ghi,
    excel_pr_ghi,
    db_pr_poa,
    excel_pr_poa,
    
    -- Overall status
    CASE 
        WHEN energy_status != 'MATCH' OR ghi_status != 'MATCH' 
            OR poa_status != 'MATCH' OR availability_status != 'MATCH' 
        THEN '⚠️ DISCREPANCY'
        WHEN data_source != 'BOTH'
        THEN '⚠️ MISSING_DATA'
        ELSE '✓ MATCH'
    END as overall_status
    
FROM comparison
WHERE 
    -- Tampilkan hanya discrepancy dan missing data
    energy_status != 'MATCH' 
    OR ghi_status != 'MATCH' 
    OR poa_status != 'MATCH'
    OR availability_status != 'MATCH'
    OR data_source != 'BOTH'
ORDER BY 
    overall_status DESC,
    site_name,
    date_key DESC;
```

**Interpretasi Hasil:**

- **MATCH**: Data sama antara database dan Excel
- **MISMATCH**: Ada perbedaan nilai (melebihi threshold)
- **MISSING_IN_DB**: Data ada di Excel tapi tidak ada di database
- **MISSING_IN_EXCEL**: Data ada di database tapi tidak ada di Excel
- **ONLY_IN_EXCEL**: Record hanya ada di Excel
- **ONLY_IN_DB**: Record hanya ada di database

**Threshold Tolerance:**
- Energy: 0.01 MWh (10 kWh)
- GHI/POA: 0.01 kWh/m²
- Availability: 1.0% (absolute difference)

---

### Step 4: Summary Report - Overview Discrepancy

Query untuk mendapatkan ringkasan discrepancy per site:

```sql
-- ============================================
-- STEP 4: Summary Report - Overview Discrepancy
-- ============================================
-- Menampilkan ringkasan discrepancy per site

WITH db_data AS (
    SELECT 
        date_key,
        site_name,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE date_key >= (SELECT MIN(date_key) FROM temp_excel_daily_performance)
        AND date_key <= (SELECT MAX(date_key) FROM temp_excel_daily_performance)
),
excel_data AS (
    SELECT 
        date_key,
        site_name,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent
    FROM temp_excel_daily_performance
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 1 ELSE 0
        END as energy_mismatch,
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 1 ELSE 0
        END as ghi_mismatch,
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 1 ELSE 0
        END as availability_mismatch,
        CASE 
            WHEN db.date_key IS NULL OR excel.date_key IS NULL THEN 1 ELSE 0
        END as missing_data
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    site_name,
    COUNT(*) as total_days,
    SUM(energy_mismatch) as energy_mismatch_count,
    SUM(ghi_mismatch) as ghi_mismatch_count,
    SUM(availability_mismatch) as availability_mismatch_count,
    SUM(missing_data) as missing_data_count,
    ROUND(100.0 * SUM(energy_mismatch) / COUNT(*)::numeric, 2) as energy_mismatch_pct,
    ROUND(100.0 * SUM(ghi_mismatch) / COUNT(*)::numeric, 2) as ghi_mismatch_pct,
    ROUND(100.0 * SUM(availability_mismatch) / COUNT(*)::numeric, 2) as availability_mismatch_pct,
    CASE 
        WHEN SUM(energy_mismatch) > 0 OR SUM(ghi_mismatch) > 0 
            OR SUM(availability_mismatch) > 0 OR SUM(missing_data) > 0
        THEN '⚠️ HAS_ISSUES'
        ELSE '✓ CLEAN'
    END as status
FROM comparison
GROUP BY site_name
ORDER BY 
    (SUM(energy_mismatch) + SUM(ghi_mismatch) + SUM(availability_mismatch) + SUM(missing_data)) DESC,
    site_name;
```

---

### Step 5: Root Cause Analysis - Drill Down ke Data Detail

Ketika menemukan discrepancy, gunakan query ini untuk investigasi lebih detail:

```sql
-- ============================================
-- STEP 5: Root Cause Analysis - Drill Down
-- ============================================
-- Gunakan query ini untuk investigasi detail ketika menemukan discrepancy
-- Ganti parameter di CTE problem_day dengan site dan tanggal yang bermasalah

WITH problem_day AS (
    SELECT 
        '2024-01-15'::DATE as problem_date,  -- Ganti dengan tanggal yang bermasalah
        'Site Name' as problem_site          -- Ganti dengan nama site yang bermasalah
),
-- Check meter data (untuk energy calculation)
meter_check AS (
    SELECT 
        m.date_key,
        m.site_name,
        m.asset_id,
        m.metric_name,
        COUNT(*) as reading_count,
        MIN(m.metric_value) as min_value,
        MAX(m.metric_value) as max_value,
        MAX(m.metric_value) - MIN(m.metric_value) as daily_delta,
        -- Check untuk cumulative meter
        LAG(MAX(m.metric_value)) OVER (
            PARTITION BY m.site_name, m.asset_id, m.metric_name 
            ORDER BY m.date_key
        ) as prev_day_max
    FROM "MMSR"."mart"."mart_meter_performance_5min" m
    CROSS JOIN problem_day pd
    WHERE m.date_key BETWEEN pd.problem_date - INTERVAL '1 day' AND pd.problem_date
        AND m.site_name = pd.problem_site
        AND m.metric_name IN ('positive_active_energy', 'negative_active_energy')
    GROUP BY m.date_key, m.site_name, m.asset_id, m.metric_name
),
-- Check sensor data (untuk GHI/POA calculation)
sensor_check AS (
    SELECT 
        s.date_key,
        s.site_name,
        s.asset_id,
        s.metric_name,
        COUNT(*) as reading_count,
        MAX(s.metric_value) as max_irradiance,
        AVG(s.metric_value) as avg_irradiance
    FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
    CROSS JOIN problem_day pd
    WHERE s.date_key = pd.problem_date
        AND s.site_name = pd.problem_site
        AND s.metric_name IN ('daily_irradiance', 'irradiance')
    GROUP BY s.date_key, s.site_name, s.asset_id, s.metric_name
),
-- Check inverter data (untuk availability calculation)
inverter_check AS (
    SELECT 
        i.date_key,
        i.site_name,
        i.asset_id,
        COUNT(DISTINCT i.timestamp) as timestamp_count,
        COUNT(DISTINCT i.asset_id) as inverter_count,
        COUNT(DISTINCT CASE WHEN i.metric_value > 0 THEN i.asset_id END) as available_inverter_count,
        AVG(i.metric_value) as avg_power
    FROM "MMSR"."mart"."mart_inverter_performance_5min" i
    CROSS JOIN problem_day pd
    WHERE i.date_key = pd.problem_date
        AND i.site_name = pd.problem_site
        AND i.metric_name = 'inv_active_power'
    GROUP BY i.date_key, i.site_name, i.asset_id
)
-- Combine results
SELECT 
    'METER_DATA' as check_type,
    date_key,
    site_name,
    asset_id as device_id,
    metric_name,
    reading_count,
    ROUND(min_value::numeric, 2) as min_value,
    ROUND(max_value::numeric, 2) as max_value,
    ROUND(daily_delta::numeric, 2) as daily_delta,
    ROUND(prev_day_max::numeric, 2) as prev_day_max,
    CASE 
        WHEN prev_day_max IS NOT NULL 
            AND ABS(min_value - prev_day_max) < 0.01 
        THEN 'CUMULATIVE'
        ELSE 'RESETTING'
    END as meter_type
FROM meter_check
WHERE date_key = (SELECT problem_date FROM problem_day)

UNION ALL

SELECT 
    'SENSOR_DATA' as check_type,
    date_key,
    site_name,
    asset_id as device_id,
    metric_name,
    reading_count,
    NULL as min_value,
    ROUND(max_irradiance::numeric, 2) as max_value,
    NULL as daily_delta,
    NULL as prev_day_max,
    NULL as meter_type
FROM sensor_check

UNION ALL

SELECT 
    'INVERTER_DATA' as check_type,
    date_key,
    site_name,
    asset_id as device_id,
    'inv_active_power' as metric_name,
    timestamp_count as reading_count,
    NULL as min_value,
    NULL as max_value,
    NULL as daily_delta,
    NULL as prev_day_max,
    CONCAT(available_inverter_count, '/', inverter_count, ' available') as meter_type
FROM inverter_check

ORDER BY check_type, device_id;
```

**Cara Menggunakan:**
1. Ganti `problem_date` dan `problem_site` dengan nilai dari discrepancy yang ditemukan
2. Query akan menampilkan:
   - **METER_DATA**: Data meter untuk perhitungan energy
   - **SENSOR_DATA**: Data sensor untuk perhitungan GHI/POA
   - **INVERTER_DATA**: Data inverter untuk perhitungan availability
3. Bandingkan dengan data di Excel untuk menemukan root cause

---

## 📝 Checklist Cross-Check

Gunakan checklist ini untuk memastikan proses cross-check dilakukan dengan lengkap:

### Pre-Check
- [ ] Data Excel sudah di-export ke CSV
- [ ] Struktur kolom Excel sesuai dengan database
- [ ] Range tanggal sudah ditentukan
- [ ] Temporary table sudah dibuat

### Execution
- [ ] Step 1: Export database data ke CSV ✓
- [ ] Step 2: Import Excel data ke temporary table ✓
- [ ] Step 3: Jalankan comparison query ✓
- [ ] Step 4: Review summary report ✓

### Analysis
- [ ] Identifikasi semua discrepancy
- [ ] Kategorikan jenis discrepancy (MISMATCH, MISSING_IN_DB, MISSING_IN_EXCEL)
- [ ] Untuk setiap discrepancy, jalankan root cause analysis (Step 5)
- [ ] Dokumentasikan temuan

### Resolution
- [ ] Tentukan apakah issue di database atau Excel
- [ ] Jika di database: perbaiki calculation logic atau data source
- [ ] Jika di Excel: verifikasi formula atau data input
- [ ] Re-run comparison setelah perbaikan
- [ ] Verifikasi semua discrepancy sudah resolved

---

## 🔍 Common Issues & Solutions

### Issue 1: Data Missing di Database
**Gejala**: Status `MISSING_IN_DB` atau `ONLY_IN_EXCEL`

**Kemungkinan Penyebab:**
- Data belum di-harvest dari API
- Filter di query terlalu ketat
- Site name tidak match (case sensitivity, whitespace)

**Solusi:**
1. Check apakah data ada di raw/staging tables
2. Verifikasi site name matching (case-insensitive, trim whitespace)
3. Check date range filter

### Issue 2: Energy Mismatch
**Gejala**: `energy_status = 'MISMATCH'`

**Kemungkinan Penyebab:**
- Meter calculation logic berbeda (cumulative vs resetting)
- Unit conversion issue (Wh vs kWh)
- Meter selection berbeda (revenue meter vs semua meter)

**Solusi:**
1. Gunakan Step 5 query untuk check meter data detail
2. Verifikasi meter type (cumulative vs resetting)
3. Check unit conversion (Wh → kWh)
4. Verifikasi meter config di `seed_meter_config`

### Issue 3: GHI/POA Mismatch
**Gejala**: `ghi_status = 'MISMATCH'` atau `poa_status = 'MISMATCH'`

**Kemungkinan Penyebab:**
- Unit conversion berbeda (MJ/m² vs kWh/m²)
- Sensor selection berbeda
- Weighted average calculation berbeda (untuk POA)

**Solusi:**
1. Gunakan Step 5 query untuk check sensor data detail
2. Verifikasi unit conversion (MJ/m² → kWh/m² = divide by 3.6)
3. Check sensor config di `seed_sensor_config`
4. Verifikasi weighted average calculation untuk POA

### Issue 4: Availability Mismatch
**Gejala**: `availability_status = 'MISMATCH'`

**Kemungkinan Penyebab:**
- MIT (Minimum Irradiance Threshold) calculation berbeda
- Inverter selection berbeda
- Availability calculation logic berbeda

**Solusi:**
1. Gunakan Step 5 query untuk check inverter data detail
2. Verifikasi MIT threshold (40 W/m²)
3. Check inverter availability ratio calculation
4. Verifikasi daily aggregation logic

### Issue 5: Site Name Mismatch
**Gejala**: `data_source = 'ONLY_IN_DB'` atau `'ONLY_IN_EXCEL'`

**Kemungkinan Penyebab:**
- Nama site berbeda (typo, case sensitivity, whitespace)
- Site tidak ada di salah satu source

**Solusi:**
1. Check site name di kedua source
2. Gunakan UPPER(TRIM()) untuk matching
3. Buat mapping table jika nama berbeda
4. Verifikasi site list di `dim_site`

---

## 🔧 Special Cases: POA Override & GHI Fallback

### POA Override Case (MMKI Sites)

**Problem**: 
- POA sensors dari MMKI II/III secara fisik tersimpan di MMKI I, tapi secara logis milik MMKI II/III
- Database perlu apply override untuk assign sensor ke logical site yang benar

**Configuration**:
- File: `dbt/seeds/seed_sensor_site_mapping.csv`
- Mapping type: `POA_OVERRIDE`
- Example: Sensor `EM06102287046729` (physically at MMKI I) → assigned to MMKI II starting 2025-09-16

**Expected Behavior**:
- Before override: POA sensor shows under MMKI I site
- After override: POA sensor shows under MMKI II site (logical assignment)
- Date-aware: Override only applies from `effective_date_start` onwards

**Cross-Check Notes**:
- MMKI II should have POA values matching Excel (after override implemented)
- Before override implementation: Expect POA discrepancies for MMKI II
- After override implementation: POA should match Excel

**Troubleshooting**:
- If POA still mismatched after override: Check `seed_sensor_site_mapping.csv` configuration
- Verify `effective_date_start` is correct
- Verify `device_id` matches sensor device_id in `seed_sensor_config.csv`
- Verify `logical_site_id` matches target site name

---

### GHI Fallback Case (MMKI Sites)

**Problem**:
- MMKI II dan III tidak punya GHI sensor sendiri
- Mereka perlu menggunakan GHI dari MMKI I (fallback)

**Configuration**:
- File: `dbt/seeds/seed_sensor_site_mapping.csv`
- Mapping type: `GHI_FALLBACK`
- Example: MMKI II uses GHI from MMKI I

**Expected Behavior**:
- MMKI I: Uses its own GHI sensor data
- MMKI II: Uses GHI from MMKI I (fallback)
- MMKI III: Uses GHI from MMKI I (fallback, if configured)

**Cross-Check Notes**:
- MMKI II GHI should equal MMKI I GHI (after fallback implemented)
- Before fallback implementation: Expect missing GHI values for MMKI II in database
- After fallback implementation: MMKI II GHI should match Excel

**Troubleshooting**:
- If GHI still missing after fallback: Check `seed_sensor_site_mapping.csv` configuration
- Verify `device_id` matches target site name (site that needs fallback)
- Verify `logical_site_id` matches source site name (site that provides GHI)
- Check that source site (MMKI I) has GHI data

---

### Verification Queries

**Check POA Override Configuration**:
```sql
SELECT * 
FROM "MMSR"."staging"."seed_sensor_site_mapping"
WHERE mapping_type = 'POA_OVERRIDE';
```

**Check GHI Fallback Configuration**:
```sql
SELECT * 
FROM "MMSR"."staging"."seed_sensor_site_mapping"
WHERE mapping_type = 'GHI_FALLBACK';
```

**Verify POA Override Working**:
```sql
-- Check if POA sensor is assigned to correct logical site
SELECT 
    date_key,
    site_name,
    sensor_id,
    max_daily_poa_kwh_m2
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name LIKE '%MMKI%'
    AND date_key >= '2025-09-16'
ORDER BY date_key, site_name;
```

**Verify GHI Fallback Working**:
```sql
-- Check if MMKI II GHI equals MMKI I GHI
SELECT 
    date_key,
    site_name,
    daily_ghi_kwh_m2
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name IN (
    'PT. MMKI 1.75 MWp - Painting Building',
    'PT. MMKI 5.7 MWp - Phase 2'
)
ORDER BY date_key, site_name;
```

---

## 📊 Export Comparison Results

Setelah menjalankan comparison query, export hasil untuk dokumentasi:

```sql
-- Export comparison results ke CSV untuk dokumentasi
-- Jalankan Step 3 query, lalu export hasilnya
-- Simpan sebagai: comparison_results_YYYYMMDD.csv
```

**Format File:**
- Nama file: `comparison_results_YYYYMMDD.csv`
- Include: Semua kolom dari comparison query
- Purpose: Dokumentasi dan tracking perbaikan

---

## 🚀 Tips untuk Performa

1. **Limit Date Range**: Jangan compare semua data sekaligus, gunakan range tanggal yang relevan
2. **Index**: Pastikan index pada `(date_key, site_name)` sudah ada (sudah ada di table)
3. **Batch Processing**: Jika data banyak, compare per site atau per bulan
4. **Materialized View**: Untuk comparison yang sering dilakukan, pertimbangkan membuat materialized view

---

## 📅 Recommended Schedule

- **Daily**: Compare data hari sebelumnya
- **Weekly**: Compare data 1 minggu terakhir
- **Monthly**: Full comparison untuk bulan sebelumnya
- **Quarterly**: Full audit untuk semua data

---

## 📞 Support

Jika menemukan issue yang tidak tercakup di dokumentasi ini:
1. Dokumentasikan issue dengan detail
2. Simpan query dan hasil comparison
3. Simpan root cause analysis result
4. Escalate ke team untuk investigasi lebih lanjut

---

**Last Updated**: 2025-01-30  
**Version**: 1.0  
**Author**: Database Team

