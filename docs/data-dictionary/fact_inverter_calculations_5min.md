# Kamus Data: `mart.fact_inverter_calculations_5min`

**Grain:** 1 baris per `(timestamp, inverter_id)`  
**Model dbt:** `dbt/models/facts/fact_inverter_calculations_5min.sql`  
**Peran pipeline:** kalkulasi availability inverter 5-menit dengan konteks MIT site.

---

## Tujuan bisnis

Mengubah metrik inverter mentah menjadi status availability inverter, lalu dipakai agregasi site-level di `fact_site_calculations_5min`.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `inverter_id` | ID inverter | kunci perangkat inverter |
| `site_id` | ID site | site tempat inverter berada |
| `active_power_kw` | Daya inverter | dari `mart_inverter_performance_5min` (`inv_active_power`) |
| `mit` | Flag MIT | dari `fact_sensor_calculations_5min` (default 0 jika tidak ada) |
| `inverter_availability` | Status inverter aktif | `1 jika active_power_kw > 0, else 0` |
| `inverter_availability_with_mit` | Status inverter aktif saat MIT | `1 jika active_power_kw > 0 dan mit = 1` |

---

## Koreksi jika hasil aneh

- Cek `mart_inverter_performance_5min` (terutama `inv_active_power`).
- Cek `fact_sensor_calculations_5min` (nilai MIT dan fallback irradiance).
- Cek `dim_assets.total_inverters` jika coverage inverter terasa janggal.
