# Kamus Data: `mart.mart_sensor_daily`

**Grain:** 1 baris per `(date_key, asset_id, sensor_type)`  
**Model dbt:** `dbt/models/marts/mart_sensor_daily.sql`  
**Peran pipeline:** agregasi sensor harian (GHI/POA/weather) untuk perhitungan site daily.

---

## Tujuan bisnis

Merangkum seluruh pembacaan sensor 5-menit menjadi metrik harian per sensor agar:

- diagnosa sensor lebih mudah,
- perhitungan GHI/POA harian lebih stabil,
- input ke `mart_site_performance_daily` konsisten.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `date_key` | Tanggal operasi | grain harian |
| `asset_id` | ID sensor | kunci sensor |
| `site_name` | Nama site (setelah override) | bisa terpengaruh POA override |
| `sensor_type` | Tipe sensor | GHI / POA / weather |
| `sensor_capacity_kwp` | Kapasitas sensor POA | dari seed config |
| `daily_irradiance_kwh_m2` | Irradiance harian | konversi unit ke kWh/m2 |
| `avg_irradiance_w_m2` | Rata-rata irradiance | rata-rata dari slot 5-menit |
| `max_irradiance_w_m2` | Puncak irradiance | maksimum harian |
| `daily_horizontal_irradiation_kwh_m2` | Horizontal irradiation harian | konversi unit ke kWh/m2 |
| `measurement_count` | Jumlah record | total data sensor |
| `timestamp_count` | Jumlah timestamp unik | indikator kelengkapan data |

---

## Koreksi jika hasil aneh

- Cek `seed_sensor_config` (sensor_type, kapasitas, device_id).
- Cek `seed_sensor_site_mapping` untuk POA override / fallback.
- Cek `mart_sensor_measurements_5min` jika agregasi terasa tidak wajar.
