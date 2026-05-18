# Site Performance — Arsitektur & Implementasi

Dokumen master untuk site performance: arsitektur 3-level mart, perhitungan daily (energy, GHI/POA, availability, PR), dan rencana implementasi. Digabung dari: SITE_PERFORMANCE_ARCHITECTURE, SITE_PERFORMANCE_DAILY_IMPLEMENTATION, SITE_PERFORMANCE_IMPLEMENTATION_PLAN.

---

## 1. Architecture

### 1.1 Pemahaman

**Availability bersifat site-level, capacity-weighted:**
- Site punya N inverter; tiap inverter punya capacity = sum string capacities.
- Availability = SUM(available_inverter_capacity) / SUM(total_inverter_capacity).
- Contoh: 1 inverter (10% capacity) down → availability = 0.9; semua available → 1.0.

### 1.2 Three-Level Mart Structure

1. **Device-Level Marts** (granular):
   - `mart_inverter_performance_5min`, `mart_string_performance_5min`, `mart_meter_performance_5min`, `mart_sensor_measurements_5min`

2. **Site-Level Marts** (agregasi):
   - `mart_site_performance_5min` — agregasi 5 menit per site
   - `mart_site_performance_daily` — agregasi harian, MTD/YTD

3. **Analysis Layer**: Power BI / views untuk drill-down.

### 1.3 Struktur tabel (ringkas)

**mart_site_performance_5min**: timestamp_5min, site_id, site_name, system, date_key; total/available inverter capacity, site_availability_ratio, site_energy_kwh, poa/ghi irradiance, performance_ratio_poa/ghi.

**mart_site_performance_daily**: date_key, site_id, site_name; daily_energy_mwh, daily_availability_percent, daily_poa/ghi, daily_pr_poa/ghi; energy_mtd_ytd, availability_mtd_ytd, dll.

---

## 2. Daily Implementation (Requirements)

### 2.1 Energy (Daily Yield)

- **Source**: `mart_meter_performance_5min`, filter `meter_type = 'Revenue'` (seed_meter_config).
- **Method**: Gunakan negative_active_energy jika ABS(negative) > ABS(positive) (generation), else positive (consumption).
- **Daily**: LAST_VALUE − FIRST_VALUE atau MAX − MIN (exclude 0/NULL) per hari.
- **Multiple meters**: SUM daily energy per site.

### 2.2 Sensor (GHI & POA)

- **GHI**: Source `mart_sensor_measurements_5min`, filter `sensor_type = 'GHI'`. MAX(daily_irradiance) per sensor per day; site GHI = MAX atau average jika banyak GHI.
- **POA Weighted**: Filter `sensor_type = 'POA'`; weight by capacity dari `seed_sensor_config`. Formula: SUM(MAX_daily_irradiance × poa_capacity) / SUM(poa_capacity) per site per day. **Action**: Tambah kolom `capacity` di seed_sensor_config jika belum ada.

### 2.3 Availability

- **Source**: `mart_inverter_performance_5min`, metric `inv_active_power`.
- **5-min**: Inverter available jika active_power > 0; site availability = COUNT(available) / COUNT(total) (count-based sementara; nanti capacity-weighted).
- **Daily**: power_available_hours = COUNT(interval available) × 5/60; unavailability_hours serupa; availability_percent = power_available_hours / (power_available_hours + unavailability_hours) × 100.

### 2.4 Performance Ratio

- **PR GHI** = daily_energy_mwh / (daily_ghi_kwh_m2 / 1000) / site_capacity_mw
- **PR POA** = daily_energy_mwh / (daily_poa_weighted_kwh_m2 / 1000) / site_capacity_mw  
  (site capacity dari dim_site/dim_assets.)

### 2.5 Target Comparisons

- Join `mart_simulation_targets_daily` by date_key dan site_id/site_code.
- Kolom: energy_actual_vs_target_pct, ghi_actual_vs_target_pct, pr_ghi_actual, pr_poa_actual, pr_ghi/poa_variance_pct, dll.

---

## 3. Implementation Plan & Calculations

### 3.1 Site Availability (capacity-weighted)

**Formula**: site_availability = SUM(available_inverter_capacity) / SUM(total_inverter_capacity); available = capacity IF active_power > 0 ELSE 0.

**Sementara (tanpa seed)**: Count-based — COUNT(CASE WHEN active_power > 0 THEN asset_id END) / COUNT(asset_id).

**Nanti (dengan seed)**: Pakai capacity dari seed; SUM(available_capacity) / SUM(total_capacity) per site per timestamp.

### 3.2 POA Weighted Average

**Formula**: weighted_poa = SUM(poa_irradiance × capacity_for_poa) / SUM(capacity_for_poa).

**Sementara**: AVG(metric_value) untuk POA sensors di site.
**Nanti**: Join dengan seed_inverter_string_poa_mapping (atau seed_sensor_config capacity); weight per POA sensor.

### 3.3 Performance Ratio

- PR_POA = site_energy_kwh / (weighted_poa_irradiance_kwh_m2 × total_capacity_kw)
- PR_GHI = site_energy_kwh / (ghi_irradiance_kwh_m2 × total_capacity_kw)

### 3.4 Implementation Steps

1. **Phase 1 (sekarang)**: Build mart_site_performance_5min dan mart_site_performance_daily dengan count-based availability, simple POA/GHI average, energy dari meter/inverter.
2. **Phase 2 (nanti)**: Tambah kolom capacity di seed_sensor_config; buat/enhance dim_site; update availability jadi capacity-weighted; update POA jadi capacity-weighted.

### 3.5 Rekomendasi

- Build site performance **sekarang** dengan availability count-based dan POA/GHI simple average.
- Struktur siap untuk enhancement capacity-weighted ketika seed siap.
- Site energy: prefer meter (revenue), fallback inverter.

---

## 4. Questions / Decisions

- **Energy**: MAX − MIN (exclude 0/NULL) atau LAST − FIRST — keduanya OK; MAX−MIN lebih robust.
- **POA capacity**: Unit di seed_sensor_config — kW disarankan.
- **Multiple GHI**: MAX (atau average) — MAX sesuai requirement.
- **Target join**: By date_key dan site_id atau site_code (sesuai mart_simulation_targets_daily).

---

*Dokumen master site performance. File asli: SITE_PERFORMANCE_ARCHITECTURE, SITE_PERFORMANCE_DAILY_IMPLEMENTATION, SITE_PERFORMANCE_IMPLEMENTATION_PLAN — diarsipkan di dbt/docs/archive/.*
