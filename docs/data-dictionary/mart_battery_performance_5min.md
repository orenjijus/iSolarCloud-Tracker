# Kamus Data: `mart.mart_battery_performance_5min`

**Grain:** 1 baris per `(timestamp, dev_id, metric_id)`  
**Model dbt:** `dbt/models/marts/mart_battery_performance_5min.sql`  
**Peran pipeline:** metrik battery 5-menit Hidden Valley.

---

## Tujuan bisnis

Menyediakan metrik battery yang sudah distandarkan (charge/discharge/power) untuk analisis operasional Hidden Valley.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `asset_id` | ID device battery | hasil mapping `FS_<dev_id>` |
| `site_name` | Nama site | site battery |
| `metric_id` | ID metrik mentah | dari FusionSolar battery unpivot |
| `metric_name` | Nama metrik standar | dari `seed_metric_mapper` (group battery) |
| `metric_unit` | Unit metrik | unit standar |
| `metric_value` | Nilai metrik | nilai numerik 5-menit |
| `dev_id` | Device ID raw | referensi ke raw source |

---

## Catatan penting

- Hanya metrik battery `used = yes` yang ikut (contoh: `charge_cap`, `discharge_cap`, `ch_discharge_power`).
- Sumber utama dari `stg_fusionsolar__perf_battery_unpivoted`.

---

## Koreksi jika hasil aneh

- Cek `seed_metric_mapper` untuk group `battery`.
- Cek staging battery unpivot (missing metric/record).
