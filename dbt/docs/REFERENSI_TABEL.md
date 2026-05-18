# Referensi Tabel Database

Struktur tabel database proyek dbt MMSR Solar Data. Digabung dari: REFERENSI_TABEL_DATABASE, REFERENSI_TABEL_SIMPLE.

---

## 1. Quick Reference

| Tabel | Deskripsi | Schema |
|-------|-----------|--------|
| `mart_site_performance_daily` | Tabel utama metrik harian site-level | `mart` |
| `mart_meter_performance_5min` | Data meter 5 menit | `mart` |
| `mart_sensor_measurements_5min` | Data sensor 5 menit | `mart` |
| `mart_inverter_performance_5min` | Data inverter 5 menit | `mart` |
| `fact_site_calculations_5min` | Kalkulasi availability 5 menit (MIT, power_available_ratio) | `mart` |
| `mart_simulation_targets_daily` | Target simulasi harian | `mart` |
| `dim_assets` | Dimensi unified (sites + devices); site-level pakai `asset_level = 'Site'` | `dimensions` |
| `dim_date_generated` | Dimensi tanggal | `dimensions` |
| `seed_meter_config` | Konfigurasi meter | `staging` |
| `seed_sensor_config` | Konfigurasi sensor | `staging` |
| `seed_sensor_site_mapping` | Mapping sensor ke site (POA_OVERRIDE, GHI_FALLBACK) | `staging` |
| `seed_issue_dates` | Tanggal issue/maintenance | `staging` |

**Catatan**: `dim_site` jika disebut di doc lain = subset dari `dim_assets` (asset_level = 'Site'). `fact_site_calculations_5min` ada di schema `mart` (bukan schema facts terpisah).

---

## 2. Detail (ringkas)

### mart_site_performance_daily

- **Tujuan**: Metrik harian site-level (energy, GHI, POA, availability, PR, target).
- **Kolom utama**: date_key, site_id, site_name, system, daily_energy_mwh, daily_ghi_kwh_m2, daily_poa_weighted_kwh_m2, power_available_hours, availability_percent, pr_ghi_actual, pr_poa_actual, energy_target_mwh, energy_actual_vs_target_pct, dll.
- **Sumber**: mart_meter_*, mart_sensor_daily, fact_site_calculations_5min, mart_simulation_targets_daily, mart_site_kpi_monthly, seed_issue_dates.

### mart_meter_performance_5min / mart_sensor_measurements_5min / mart_inverter_performance_5min

- **Tujuan**: Data 5 menit per device (meter, sensor, inverter); dipakai untuk daily aggregation dan availability.
- **Sumber**: stg_*_perf_unpivoted, dim_assets, dim_date_generated, seed_metric_mapper, seed_*_config.

### fact_site_calculations_5min

- **Tujuan**: Availability 5 menit (MIT, power_available_ratio, unavailability_ratio) dari inverter metrics.
- **Sumber**: mart_inverter_*, mart_sensor_*, seed_sensor_*, dim_assets.

### dimensions

- **dim_assets**: asset_id, site_id, site_name, asset_level ('Site'/'Device'), actual_capacity_kw, tariff, site_order, dll.
- **dim_date_generated**: date_key, year, month, week_of_month, season, day_type (2020-01-01 s/d current+2 years).

### staging (seeds)

- **seed_meter_config**: meter_type (Revenue, dll), polarity_swapped, voltage_level.
- **seed_sensor_config**: sensor_type (GHI/POA), device_id, sensor_capacity_kwp.
- **seed_sensor_site_mapping**: POA_OVERRIDE, GHI_FALLBACK.
- **seed_issue_dates**: site_id, site_name, issue_date, is_active.

---

*Dokumen master referensi tabel. File asli: REFERENSI_TABEL_DATABASE, REFERENSI_TABEL_SIMPLE — diarsipkan di dbt/docs/archive/.*
