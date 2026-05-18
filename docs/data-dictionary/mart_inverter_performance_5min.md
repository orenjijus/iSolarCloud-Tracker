# Kamus Data: `mart.mart_inverter_performance_5min`

**Grain:** 1 baris per `(timestamp, asset_id, metric_id)`  
**Model dbt:** `dbt/models/marts/mart_inverter_performance_5min.sql`  
**Peran pipeline:** daily measurement 5-menit + dipakai jalur HV intraday.

---

## Tujuan bisnis

Menyediakan metrik inverter 5-menit yang sudah diseragamkan lintas platform (FusionSolar + iSolarCloud), supaya tabel fact bisa menghitung availability dan performa site.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `asset_id` | ID inverter | hasil mapping ke `dim_assets` |
| `asset_name` | Nama inverter | nama perangkat standar |
| `site_name` | Nama site | site inverter |
| `metric_id` | ID metrik mentah | ID dari platform sumber |
| `metric_name` | Nama metrik standar | dari `seed_metric_mapper` (contoh: `inv_active_power`) |
| `metric_unit` | Unit metrik | unit standar hasil mapping |
| `metric_value` | Nilai metrik | nilai numerik 5-menit |

---

## Catatan penting

- Hanya metrik inverter yang `used = yes` di `seed_metric_mapper` yang ikut.
- Nilai null dibuang.
- Ada mapping ID/name inverter untuk konsolidasi perangkat agar konsisten antar periode.

---

## Koreksi jika hasil aneh

- Cek `seed_metric_mapper` (mapping metrik, unit, used flag).
- Cek `seed_inverter_site_mapping` (ID consolidation / name override).
- Cek data staging (`stg_fusionsolar__perf_unpivoted`, `stg_isolarcloud__perf_unpivoted`).
