# Kamus Data: `mart.fact_sensor_calculations_5min`

**Grain:** 1 baris per `(timestamp, sensor_id)`  
**Model dbt:** `dbt/models/facts/fact_sensor_calculations_5min.sql`  
**Peran pipeline:** menghitung MIT dengan prioritas sumber irradiance (GHI/POA/fallback).

---

## Tujuan bisnis

Menentukan apakah pada slot 5-menit sedang ada kondisi irradiance minimum (MIT), sehingga availability dihitung adil hanya saat ada sinar yang cukup.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `sensor_id` | ID sensor | kunci perangkat sensor |
| `site_id` | ID site | site sensor |
| `sensor_type` | Tipe sensor | GHI/POA |
| `irradiance_w_m2` | Irradiance tervalidasi | nilai sensor setelah validasi anomali |
| `is_fallback` | Flag fallback | `TRUE` jika memakai fallback site lain |
| `mit` | Flag MIT | `1 jika irradiance > 40 W/m2, else 0` |
| `mit_irradiance_source` | Sumber MIT | `GHI`, `POA`, atau `GHI_FALLBACK` |

---

## Catatan penting

- Ada validasi sensor stuck/outlier sebelum dipakai.
- Prioritas sumber irradiance berbeda untuk site MMKI vs non-MMKI.

---

## Koreksi jika hasil aneh

- Cek `seed_sensor_config` (sensor_type & mapping device).
- Cek `seed_sensor_site_mapping` (GHI_FALLBACK, POA_OVERRIDE, date efektif).
- Cek data 5-menit di `mart_sensor_measurements_5min`.
