# Kamus Data: `mart.mart_meter_performance_5min`

**Grain:** 1 baris per `(timestamp, asset_id, metric_id)`  
**Model dbt:** `dbt/models/marts/mart_meter_performance_5min.sql`  
**Peran pipeline:** tabel meter 5-menit utama (daily biasa + Hidden Valley).

---

## Tujuan bisnis

Menjadi sumber tunggal metrik meter 5-menit (termasuk Smart Assistant HV), yang dipakai untuk:

- perhitungan energi harian site,
- perhitungan beban villa Hidden Valley,
- turunan mart/fact meter lainnya.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `asset_id` | ID meter | hasil mapping ke `dim_assets` |
| `asset_name` | Nama meter | nama perangkat standar |
| `site_name` | Nama site | site meter |
| `metric_id` | ID metrik mentah | dari API sumber |
| `metric_name` | Nama metrik standar | dari `seed_metric_mapper` |
| `metric_unit` | Unit metrik | unit standar |
| `metric_value` | Nilai metrik | nilai numerik 5-menit |
| `meter_type` | Tipe meter | dari konfigurasi meter |
| `voltage_level` | Level tegangan | metadata dari konfigurasi meter |

---

## Catatan penting

- Hanya metrik group `meter` + `smart_assistant` yang `used = yes`.
- Nilai `nan/infinity` dibuang.
- Ada konsolidasi ID/name meter melalui `seed_meter_site_mapping`.

---

## Koreksi jika hasil aneh

- Cek `seed_metric_mapper` (mapping metric_id -> metric_name).
- Cek `meter_config_reference` / `seed_meter_config`.
- Cek `seed_meter_site_mapping` (ID consolidation, effective date).
