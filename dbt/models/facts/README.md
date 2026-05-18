# 5-Minute Calculated Metrics Fact Tables

## Overview

Fact tables ini menghitung metrics yang diperlukan untuk availability calculation dengan tracking detail per 5 menit. Fact tables ini memungkinkan kita untuk:

1. **Track inverter mana yang mati** per 5 menit
2. **Validasi availability calculation** dengan melihat detail per inverter
3. **Menggunakan fallback logic** untuk MIT calculation (GHI → POA → GHI fallback)

---

## Fact Tables

### 1. `fact_sensor_calculations_5min`

**Purpose**: Sensor-level calculated metrics dengan GHI dan POA fallback logic untuk MIT

**Grain**: `timestamp × sensor_id`

**Dependencies**:
- `mart_sensor_measurements_5min`
- `seed_sensor_config`
- `seed_sensor_site_mapping`
- `dim_assets`

**Key Features**:
- **Stuck Sensor Detection**: Window functions untuk mendeteksi sensor yang stuck (reporting nilai konstan 30+ menit)
- **Sensor Validation**: Filter out sensor malfunctioning (nilai tidak realistis, stuck values, nighttime anomalies)
- GHI fallback logic (MMKI II/III pakai GHI dari MMKI I)
- **POA fallback untuk MIT** (jika GHI mati/stuck, gunakan POA dari site yang sama)
- MIT calculation dengan priority:
  1. GHI (pyranometer) dari site yang sama - jika available dan validated
  2. POA dari site yang sama - jika GHI NULL/mati/stuck dan POA validated
  3. GHI dari site lain - configured fallback (last resort)

**Schema**:
```sql
CREATE TABLE mart.fact_sensor_calculations_5min (
    timestamp TIMESTAMPTZ,
    date_key DATE,
    sensor_id VARCHAR,
    sensor_name VARCHAR,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    sensor_type VARCHAR,  -- 'GHI' or 'POA'
    irradiance_w_m2 DECIMAL,
    is_fallback BOOLEAN,
    mit INTEGER,  -- 0 or 1
    mit_irradiance_source VARCHAR,  -- 'GHI', 'POA', or 'GHI_FALLBACK'
    calculation_timestamp TIMESTAMPTZ
);
```

**Indexes**:
- `(timestamp, sensor_id)` - Primary lookup
- `(timestamp, site_id)` - Site-level queries
- `(sensor_id)` - Sensor lookup
- `(date_key)` - Date-based queries

---

### 2. `fact_inverter_calculations_5min`

**Purpose**: Inverter-level calculated metrics termasuk availability

**Grain**: `timestamp × inverter_id`

**Dependencies**:
- `mart_inverter_performance_5min`
- `fact_sensor_calculations_5min` (untuk MIT)
- `dim_assets`

**Key Features**:
- Inverter availability calculation
- Availability with MIT
- Menggunakan MIT dari `fact_sensor_calculations_5min`
- **LEFT JOIN dengan MIT data**: Data inverter tetap muncul meskipun sensor/MIT data tidak ada
- **MIT Default = 0**: Jika sensor data tidak ada, MIT default ke 0 (no sun = MIT 0)

**Schema**:
```sql
CREATE TABLE mart.fact_inverter_calculations_5min (
    timestamp TIMESTAMPTZ,
    date_key DATE,
    inverter_id VARCHAR,
    inverter_name VARCHAR,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    active_power_kw DECIMAL,
    mit INTEGER,  -- 0 or 1
    inverter_availability INTEGER,  -- 0 or 1
    inverter_availability_with_mit INTEGER,  -- 0 or 1
    calculation_timestamp TIMESTAMPTZ
);
```

**Indexes**:
- `(timestamp, inverter_id)` - Primary lookup
- `(timestamp, site_id)` - Site-level queries
- `(inverter_id)` - Inverter lookup
- `(date_key)` - Date-based queries

**Calculation Logic**:
```sql
-- Join inverter power with MIT (LEFT JOIN to preserve inverter data even when MIT is missing)
FROM inverter_power ip
LEFT JOIN site_mit sm
    ON ip.timestamp = sm.timestamp
    AND ip.site_id = sm.site_id

-- MIT: Default to 0 if sensor data is missing (no sun = MIT 0)
mit = COALESCE(sm.mit, 0)

-- Inverter availability: CASE WHEN active_power > 0 THEN 1 ELSE 0 END
inverter_availability = CASE 
    WHEN active_power_kw > 0 THEN 1
    ELSE 0
END

-- Inverter availability with MIT: CASE WHEN active_power > 0 AND mit = 1 THEN 1 ELSE 0 END
inverter_availability_with_mit = CASE 
    WHEN active_power_kw > 0 AND COALESCE(sm.mit, 0) = 1 THEN 1
    ELSE 0
END
```

**Important Design Decision**:
- **LEFT JOIN** (not INNER JOIN) ensures inverter data is not lost when sensor/MIT data is unavailable
- This is critical for accurate Power Available Hours calculation, which should count ALL intervals where inverters are on, regardless of MIT status
- MIT = 0 is a reasonable default when sensor data is missing (assumes no sun)

---

### 3. `fact_site_calculations_5min`

**Purpose**: Site-level calculated metrics yang di-aggregate dari inverter level

**Grain**: `timestamp × site_id`

**Dependencies**:
- `fact_inverter_calculations_5min`

**Key Features**:
- Aggregasi dari inverter level
- Power available ratio dan unavailability ratio
- Siap untuk daily aggregation

**Schema**:
```sql
CREATE TABLE mart.fact_site_calculations_5min (
    timestamp TIMESTAMPTZ,
    date_key DATE,
    site_id VARCHAR,
    site_name VARCHAR,
    system VARCHAR,
    total_inverters INTEGER,
    available_inverters INTEGER,
    mit INTEGER,  -- 0 or 1
    power_available_ratio DECIMAL,  -- 0-1
    unavailability_ratio DECIMAL,  -- 0-1
    calculation_timestamp TIMESTAMPTZ
);
```

**Indexes**:
- `(timestamp, site_id)` - Primary lookup
- `(site_id)` - Site lookup
- `(date_key)` - Date-based queries

**Calculation Logic**:
```sql
-- total_inverters = Fixed total from seed_site_config (NOT dynamic count)
-- This ensures missing inverters are counted as unavailable
total_inverters = MAX(dim_assets.total_inverters)  -- From seed_site_config

-- available_inverters = Count of inverters with power > 0
available_inverters = COUNT(DISTINCT CASE WHEN inverter_availability = 1 THEN inverter_id END)

-- power_available_ratio = available_inverters / total_inverters_fixed
-- Example: 9 inverters reporting out of 18 total = 9/18 = 0.5 (50%)
power_available_ratio = CASE 
    WHEN total_inverters > 0
    THEN available_inverters::DECIMAL / total_inverters::DECIMAL
    ELSE 0
END

-- unavailability_ratio = CASE WHEN mit = 1 THEN 1 - power_available_ratio ELSE 0 END
-- Only counts as unavailable when there is sunlight (MIT = 1)
-- Example: MIT=1, 9/18 inverters available → unavailability = 1 - 0.5 = 0.5 (50%)
unavailability_ratio = CASE 
    WHEN mit = 1 AND total_inverters > 0
    THEN 1 - (available_inverters::DECIMAL / total_inverters::DECIMAL)
    ELSE 0
END
```

**Important Notes**:
- **Fixed Total Inverters**: `total_inverters` comes from `seed_site_config.TotalInverters`, NOT from dynamic count of reporting inverters
- **Missing Inverters = Unavailable**: If a site has 18 inverters configured but only 9 report data, the 9 missing inverters are counted as unavailable
- **Accurate Availability**: This approach matches Excel calculation and provides accurate availability metrics even when some inverters fail to report

---

## MIT Fallback Logic

### Priority Order

1. **Priority 1**: GHI (pyranometer) dari site yang sama
2. **Priority 2**: POA dari site yang sama (fallback jika GHI tidak tersedia)
3. **Priority 3**: GHI dari site lain (configured fallback dari `seed_sensor_site_mapping`)

### Tracking

Kolom `mit_irradiance_source` di `fact_sensor_calculations_5min` menunjukkan sumber irradiance yang digunakan:
- `'GHI'`: Menggunakan GHI dari site yang sama
- `'POA'`: Menggunakan POA dari site yang sama (fallback)
- `'GHI_FALLBACK'`: Menggunakan GHI dari site lain (configured fallback)

### Example Scenarios

**Scenario 1: GHI Available**
- Site A punya GHI sensor yang aktif
- MIT menggunakan GHI → `mit_irradiance_source = 'GHI'`

**Scenario 2: GHI Down, POA Active**
- Site B punya GHI sensor tapi mati
- Site B punya POA sensor yang aktif
- MIT menggunakan POA → `mit_irradiance_source = 'POA'`

**Scenario 3: GHI and POA Down**
- Site C tidak punya GHI sensor atau GHI mati
- Site C tidak punya POA sensor atau POA mati
- MIT menggunakan GHI fallback dari site lain (jika dikonfigurasi) → `mit_irradiance_source = 'GHI_FALLBACK'`

**Scenario 4: GHI Stuck, POA Active**
- Site D punya GHI sensor tapi stuck (reporting nilai konstan 552.4 W/m² terus menerus)
- Stuck sensor detection filter out GHI → marked as NULL
- MIT fallback ke POA → `mit_irradiance_source = 'POA'`

---

## Sensor Validation and Stuck Detection

### Overview

Sensor validation menggunakan **window functions** untuk mendeteksi sensor yang stuck atau malfunctioning. Ini sangat penting untuk memastikan MIT calculation akurat.

### Stuck Sensor Detection

**Definition**: Sensor dianggap STUCK jika reporting **nilai yang sama persis** untuk **6+ intervals berturut-turut** (30+ menit) dengan nilai > 100 W/m².

**Logic**:
```sql
-- Check previous 5 values using LAG window function
WHEN raw_irradiance = prev_1 
    AND raw_irradiance = prev_2 
    AND raw_irradiance = prev_3
    AND raw_irradiance = prev_4
    AND raw_irradiance = prev_5
    AND raw_irradiance > 100  -- Only flag high values
THEN NULL  -- Mark as stuck, trigger fallback
```

**Example**:
- Sensor reports: 552.4, 552.4, 552.4, 552.4, 552.4, 552.4, 552.4 (7x sama) → **STUCK!** ❌
- Sensor reports: 597, 831, 945, 1007, 991, 938 (bervariasi) → **Valid** ✅

### Additional Validation Rules

**Deep Nighttime Validation** (00:00-05:59):
```sql
WHEN EXTRACT(HOUR FROM timestamp) < 6 
    AND raw_irradiance > 50
THEN NULL  -- Suspicious nighttime reading
```

**Unrealistic High Values**:
```sql
WHEN raw_irradiance > 2000  -- Max realistic ~1400 W/m²
THEN NULL
```

### Why Window Functions?

- **Dynamic Detection**: Tidak hardcode nilai atau jam tertentu
- **Context-Aware**: Check nilai sebelum dan sesudah untuk pattern detection
- **Automatic**: Detect stuck sensor secara otomatis tanpa manual configuration
- **Robust**: Bisa detect berbagai jenis stuck sensor (552.4, 583.3, atau nilai lain)

### Query to Check Stuck Sensors

```sql
SELECT 
    date_key,
    site_name,
    sensor_id,
    COUNT(*) as total_intervals,
    COUNT(CASE WHEN irradiance_w_m2 IS NULL THEN 1 END) as filtered_intervals,
    ROUND(COUNT(CASE WHEN irradiance_w_m2 IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as filter_rate
FROM mart.fact_sensor_calculations_5min
WHERE date_key >= '2025-10-01'
GROUP BY date_key, site_name, sensor_id
HAVING COUNT(CASE WHEN irradiance_w_m2 IS NULL THEN 1 END) > 0
ORDER BY date_key DESC, filter_rate DESC;
```

---

## Common Queries

### 1. Check Inverters That Are Down

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

### 2. Check Site Availability per 5 Minutes

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

### 3. Calculate Daily Availability

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

### 4. Check POA Fallback Usage

```sql
SELECT 
    date_key,
    site_name,
    COUNT(*) as intervals_using_poa
FROM mart.fact_sensor_calculations_5min
WHERE mit_irradiance_source = 'POA'
    AND date_key >= '2025-01-01'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;
```

### 5. Check MIT Sources Distribution

```sql
SELECT 
    site_name,
    mit_irradiance_source,
    COUNT(*) as total_intervals,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY site_name), 2) as percentage
FROM mart.fact_sensor_calculations_5min
WHERE date_key >= '2025-01-01'
GROUP BY site_name, mit_irradiance_source
ORDER BY site_name, mit_irradiance_source;
```

---

## Building Fact Tables

### Full Build

```bash
cd dbt
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min
```

### Incremental Build

Fact tables menggunakan incremental materialization, jadi hanya data baru yang akan diproses:

```bash
# Default: process new data since last run
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min

# Re-process specific date range
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min \
  --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-01-31"}'
```

### Build Order

Fact tables harus di-build dalam urutan ini karena dependencies:

1. `fact_sensor_calculations_5min` (no dependencies)
2. `fact_inverter_calculations_5min` (depends on sensor fact)
3. `fact_site_calculations_5min` (depends on inverter fact)

---

## Validation

### Validation Queries

File `analyses/validate_fact_tables.sql` berisi 7 query validasi:
1. Check MIT calculation dan fallback sources
2. Check POA fallback usage
3. Check inverter availability per site
4. Check daily availability summary
5. Check which inverters are down
6. Compare MIT sources across sites
7. Check GHI fallback sites (MMKI II, III)

### Quick Validation

File `analyses/quick_validation.sql` berisi quick checks:
1. Row counts per fact table
2. MIT sources distribution
3. POA fallback usage (recent 7 days)
4. GHI fallback sites check
5. Inverter availability summary
6. Site-level availability
7. Daily availability summary

---

## Data Flow

```
mart_sensor_measurements_5min
    ↓
fact_sensor_calculations_5min (GHI/POA → MIT)
    ↓
mart_inverter_performance_5min
    ↓
fact_inverter_calculations_5min (active_power + MIT → availability)
    ↓
fact_site_calculations_5min (aggregate → power_available_ratio, unavailability_ratio)
    ↓
mart_site_performance_daily (daily aggregation → availability_percent)
```

---

## Maintenance

### Incremental Updates

Fact tables menggunakan incremental materialization dengan `unique_key`:
- `fact_sensor_calculations_5min`: `(timestamp, sensor_id)`
- `fact_inverter_calculations_5min`: `(timestamp, inverter_id)`
- `fact_site_calculations_5min`: `(timestamp, site_id)`

### Re-processing Data

Untuk re-process data tertentu, gunakan variables:

```bash
dbt run --select fact_sensor_calculations_5min fact_inverter_calculations_5min fact_site_calculations_5min \
  --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-01-31"}'
```

### Performance

- Fact tables menggunakan indexes untuk query performance
- Incremental materialization untuk efficient updates
- Hyperscale enabled untuk large datasets

---

## Related Documentation

- [FACT_5MIN_CALCULATED_METRICS_DESIGN.md](../docs/FACT_5MIN_CALCULATED_METRICS_DESIGN.md) - Design document
- [FACT_TABLES_IMPLEMENTATION_SUMMARY.md](../docs/FACT_TABLES_IMPLEMENTATION_SUMMARY.md) - Implementation summary

---

## Notes

- All fact tables are in `mart` schema
- All timestamps are in UTC (TIMESTAMPTZ)
- MIT calculation: `CASE WHEN irradiance > 40 THEN 1 ELSE 0 END`
- Availability calculation: `CASE WHEN active_power > 0 THEN 1 ELSE 0 END`
- Ratios are DECIMAL (0-1), not percentages

