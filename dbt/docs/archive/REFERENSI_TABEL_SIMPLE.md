# Referensi Tabel Database - Format Simple

## Tabel | Deskripsi | Schema

| Tabel | Deskripsi | Schema |
|-------|-----------|--------|
| `mart_site_performance_daily` | Tabel utama untuk semua metrik harian | `mart` |
| `mart_meter_performance_5min` | Data meter 5 menit | `mart` |
| `mart_sensor_measurements_5min` | Data sensor 5 menit | `mart` |
| `mart_inverter_performance_5min` | Data inverter 5 menit | `mart` |
| `fact_site_calculations_5min` | Kalkulasi availability 5 menit | `mart` |
| `mart_simulation_targets_daily` | Target dari simulasi | `mart` |
| `dim_assets` | Dimensi unified untuk semua assets (sites + devices) | `dimensions` |
| `dim_date_generated` | Dimensi tanggal | `dimensions` |
| `dim_site` | Dimensi site (kapasitas, dll) - Note: Info site ada di dim_assets dengan asset_level = 'Site' | `dimensions` |
| `seed_meter_config` | Konfigurasi meter | `staging` |
| `seed_sensor_config` | Konfigurasi sensor | `staging` |
| `seed_sensor_site_mapping` | Mapping sensor ke site | `staging` |
| `seed_issue_dates` | Tanggal issue/maintenance | `staging` |

