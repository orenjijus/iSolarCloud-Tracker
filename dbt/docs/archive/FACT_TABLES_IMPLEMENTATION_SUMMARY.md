# 5-Minute Fact Tables Implementation Summary

## Overview

Tiga fact tables 5 menit telah dibuat untuk menghitung availability dengan tracking yang lebih detail. Fact tables ini memungkinkan kita untuk:
1. **Track inverter mana yang mati** per 5 menit
2. **Validasi availability calculation** dengan melihat detail per inverter
3. **Menggunakan fallback logic** untuk MIT calculation (GHI → POA → GHI fallback)

---

## Fact Tables yang Dibuat

### 1. `fact_sensor_calculations_5min`

**Purpose**: Sensor-level calculated metrics dengan GHI dan POA fallback logic untuk MIT

**Grain**: `timestamp × sensor_id`

**Key Features**:
- GHI fallback logic (MMKI II/III pakai GHI dari MMKI I)
- **POA fallback untuk MIT** (jika GHI mati, gunakan POA dari site yang sama)
- MIT calculation dengan priority:
  1. GHI (pyranometer) dari site yang sama
  2. POA dari site yang sama (fallback jika GHI tidak tersedia)
  3. GHI dari site lain (configured fallback)

**Columns**:
- `timestamp`, `date_key`, `sensor_id`, `sensor_name`
- `site_id`, `site_name`, `system`
- `sensor_type` (GHI/POA)
- `irradiance_w_m2`
- `is_fallback` (BOOLEAN)
- `mit` (INTEGER: 0 or 1)
- `mit_irradiance_source` (VARCHAR: 'GHI', 'POA', or 'GHI_FALLBACK')
- `calculation_timestamp`

**Dependencies**: 
- `mart_sensor_measurements_5min`
- `seed_sensor_config`
- `seed_sensor_site_mapping`
- `dim_assets`

---

### 2. `fact_inverter_calculations_5min`

**Purpose**: Inverter-level calculated metrics termasuk availability

**Grain**: `timestamp × inverter_id`

**Key Features**:
- Inverter availability calculation
- Availability with MIT
- Menggunakan MIT dari `fact_sensor_calculations_5min`

**Columns**:
- `timestamp`, `date_key`, `inverter_id`, `inverter_name`
- `site_id`, `site_name`, `system`
- `active_power_kw`
- `mit` (INTEGER: 0 or 1)
- `inverter_availability` (INTEGER: 0 or 1)
- `inverter_availability_with_mit` (INTEGER: 0 or 1)
- `calculation_timestamp`

**Dependencies**:
- `mart_inverter_performance_5min`
- `fact_sensor_calculations_5min` (untuk MIT)
- `dim_assets`

---

### 3. `fact_site_calculations_5min`

**Purpose**: Site-level calculated metrics yang di-aggregate dari inverter level

**Grain**: `timestamp × site_id`

**Key Features**:
- Aggregasi dari inverter level
- Power available ratio dan unavailability ratio
- Siap untuk daily aggregation

**Columns**:
- `timestamp`, `date_key`, `site_id`, `site_name`, `system`
- `total_inverters` (INTEGER)
- `available_inverters` (INTEGER)
- `mit` (INTEGER: 0 or 1)
- `power_available_ratio` (DECIMAL: 0-1)
- `unavailability_ratio` (DECIMAL: 0-1)
- `calculation_timestamp`

**Dependencies**:
- `fact_inverter_calculations_5min`

---

## POA Fallback untuk MIT

### Problem
Di Excel, jika pyranometer (GHI) mati tapi POA sensor masih aktif, user harus manual ganti lookup dari GHI ke POA untuk menghitung MIT.

### Solution
Sistem sekarang otomatis menggunakan POA sebagai fallback jika GHI tidak tersedia.

### Priority Logic
1. **Priority 1**: GHI (pyranometer) dari site yang sama
2. **Priority 2**: POA dari site yang sama (fallback jika GHI tidak tersedia)
3. **Priority 3**: GHI dari site lain (configured fallback dari `seed_sensor_site_mapping`)

### Tracking
Kolom `mit_irradiance_source` menunjukkan sumber irradiance yang digunakan:
- `'GHI'`: Menggunakan GHI dari site yang sama
- `'POA'`: Menggunakan POA dari site yang sama (fallback)
- `'GHI_FALLBACK'`: Menggunakan GHI dari site lain (configured fallback)

---

## Cara Menggunakan

### 1. Build Fact Tables

```bash
cd dbt
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min
```

### 2. Cek Inverter yang Mati

```sql
SELECT 
    timestamp,
    site_name,
    inverter_id,
    inverter_name,
    active_power_kw,
    inverter_availability
FROM mart.fact_inverter_calculations_5min
WHERE inverter_availability = 0
    AND date_key >= '2025-01-01'
ORDER BY timestamp DESC, site_name, inverter_id;
```

### 3. Cek Availability per Site per 5 Menit

```sql
SELECT 
    timestamp,
    site_name,
    total_inverters,
    available_inverters,
    power_available_ratio,
    unavailability_ratio,
    mit
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-01-01'
ORDER BY timestamp DESC, site_name;
```

### 4. Hitung Daily Availability (untuk Crosscheck dengan Excel)

```sql
SELECT 
    date_key,
    site_name,
    SUM(power_available_ratio) / 12.0 as power_available_hours,
    SUM(unavailability_ratio) / 12.0 as unavailability_hours,
    (SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0) as total_hours,
    CASE 
        WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
        THEN (SUM(power_available_ratio) / NULLIF(
            SUM(power_available_ratio) + SUM(unavailability_ratio),
            0
        )) * 100
        ELSE NULL
    END as availability_percent
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-01-01'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;
```

### 5. Cek POA Fallback Usage

```sql
SELECT 
    date_key,
    site_name,
    mit_irradiance_source,
    COUNT(*) as interval_count
FROM mart.fact_sensor_calculations_5min
WHERE mit_irradiance_source = 'POA'
    AND date_key >= '2025-01-01'
GROUP BY date_key, site_name, mit_irradiance_source
ORDER BY date_key DESC, site_name;
```

---

## Validation Queries

File `analyses/validate_fact_tables.sql` berisi 7 query validasi:
1. Check MIT calculation dan fallback sources
2. Check POA fallback usage
3. Check inverter availability per site
4. Check daily availability summary
5. Check which inverters are down
6. Compare MIT sources across sites
7. Check GHI fallback sites (MMKI II, III)

---

## Status Implementasi

### ✅ Completed
1. ✅ Fact tables created and compiled successfully
2. ✅ Fact tables built with real data:
   - `fact_sensor_calculations_5min`: 5,840,445 rows
   - `fact_inverter_calculations_5min`: 8,614,969 rows
   - `fact_site_calculations_5min`: 963,838 rows
3. ✅ Validation queries created

### Next Steps

### Immediate
1. ✅ Run dbt untuk build fact tables dengan data real (DONE)
2. ⚠️ Validasi hasil dengan Excel (use queries in `analyses/validate_fact_tables.sql`)
   - See [FACT_TABLES_VALIDATION_RESULTS.md](./FACT_TABLES_VALIDATION_RESULTS.md) for validation checklist
3. ⚠️ Test POA fallback saat GHI mati (check `mit_irradiance_source = 'POA'`)
4. ⚠️ Compare daily availability dengan Excel calculation

### Phase 2 (Future)
1. Update `mart_site_performance_daily` untuk menggunakan `fact_site_calculations_5min`
2. Implement POA override logic di daily aggregation
3. Test dengan PowerBI

---

## Benefits

1. **Trackability**: Bisa track inverter mana yang mati per 5 menit
2. **Validasi**: Bisa validasi availability calculation dengan detail
3. **Otomatis**: POA fallback otomatis, tidak perlu manual seperti di Excel
4. **Transparansi**: Kolom `mit_irradiance_source` menunjukkan sumber yang digunakan
5. **Konsistensi**: Logic yang sama diterapkan untuk semua timestamp

---

## Files Created

1. `dbt/models/facts/fact_sensor_calculations_5min.sql`
2. `dbt/models/facts/fact_inverter_calculations_5min.sql`
3. `dbt/models/facts/fact_site_calculations_5min.sql`
4. `dbt/analyses/validate_fact_tables.sql`
5. Updated `dbt/dbt_project.yml` (added facts configuration)
6. Updated `dbt/docs/FACT_5MIN_CALCULATED_METRICS_DESIGN.md` (added POA fallback documentation)

---

## Notes

- Fact tables menggunakan incremental materialization untuk performa
- Indexes sudah dikonfigurasi untuk query performance
- Semua fact tables ada di schema `mart`
- Grain setiap table jelas dan konsisten

