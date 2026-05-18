# Kamus Data: `mart.fact_site_calculations_5min`

**Grain:** 1 baris per `(timestamp, site_id)`  
**Model dbt:** `dbt/models/facts/fact_site_calculations_5min.sql`  
**Peran pipeline:** agregasi availability site 5-menit dari level inverter.

---

## Tujuan bisnis

Menghasilkan rasio ketersediaan site per slot 5-menit yang kemudian dijumlahkan menjadi jam availability/unavailability harian di `mart_site_performance_daily`.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `site_id` | ID site | kunci site |
| `total_inverters` | Jumlah inverter acuan | fixed dari konfigurasi (kecuali MMKI pakai dinamis) |
| `available_inverters` | Inverter aktif | count inverter dengan power > 0 |
| `mit` | Flag MIT site | dari fact inverter/sensor |
| `power_available_ratio` | Rasio inverter tersedia | `available_inverters / total_inverters` |
| `unavailability_ratio` | Rasio tidak tersedia saat MIT | `1 - power_available_ratio` jika `mit=1`, selain itu `0` |

---

## Catatan penting

- Site MMKI memakai total inverter dinamis (hanya inverter yang melapor).
- Site non-MMKI memakai total inverter fixed dari konfigurasi.

---

## Koreksi jika hasil aneh

- Cek `fact_inverter_calculations_5min`.
- Cek `dim_assets.total_inverters` / konfigurasi site.
- Cek coverage data inverter per timestamp.
