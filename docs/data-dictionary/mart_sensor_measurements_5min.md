# Kamus Data: `mart.mart_sensor_measurements_5min`

**Grain:** 1 baris per `(timestamp, asset_id, metric_id)`  
**Model dbt:** `dbt/models/marts/mart_sensor_measurements_5min.sql`  
**Peran pipeline:** sumber sensor 5-menit untuk kalkulasi MIT, GHI/POA, dan agregasi sensor harian.

---

## Tujuan bisnis

Menyatukan metrik sensor 5-menit dari dua platform menjadi format yang konsisten agar perhitungan irradiance dan availability bisa stabil.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `asset_id` | ID sensor | hasil mapping ke `dim_assets` |
| `site_name` | Nama site | site sensor |
| `metric_id` | ID metrik mentah | dari API sumber |
| `metric_name` | Nama metrik standar | dari `seed_metric_mapper` |
| `metric_unit` | Unit metrik | unit standar |
| `metric_value` | Nilai metrik | nilai numerik 5-menit |
| `sensor_type` | Tipe sensor | dari `seed_sensor_config` (GHI/POA/Weather) |
| `sensor_dev_name` | Nama sensor di konfigurasi | metadata konfigurasi sensor |

---

## Catatan penting

- Hanya metrik group `sensor` yang `used = yes`.
- Digunakan langsung oleh `fact_sensor_calculations_5min` dan `mart_sensor_daily`.

---

## Koreksi jika hasil aneh

- Cek `seed_sensor_config` (sensor_type, device_id).
- Cek `seed_metric_mapper` untuk mapping metrik sensor.
- Cek data staging sumber jika nilai hilang/aneh.
