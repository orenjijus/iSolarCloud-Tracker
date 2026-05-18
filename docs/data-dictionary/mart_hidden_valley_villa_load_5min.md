# Kamus Data: `mart.mart_hidden_valley_villa_load_5min`

**Grain:** 1 baris per `(timestamp, site_id)`  
**Model dbt:** `dbt/models/marts/mart_hidden_valley_villa_load_5min.sql`  
**Peran pipeline:** kalkulasi beban villa 5-menit khusus Hidden Valley.

---

## Tujuan bisnis

Menghitung total beban villa per slot 5-menit dari kombinasi daya inverter + Smart Assistant.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `minute_key` | Index 5-menit harian | posisi slot per hari |
| `site_id` | ID site HV | dari `dim_assets` site Hidden Valley |
| `inv_active_power_kw` | Total daya inverter | agregasi inverter active power (dinormalisasi ke kW) |
| `sa_active_power_kw` | Total daya Smart Assistant | agregasi `sa_active_power` (dinormalisasi ke kW) |
| `beban_villa_kw` | Beban villa total | `inv_active_power_kw + sa_active_power_kw` |

---

## Koreksi jika hasil aneh

- Cek `mart_inverter_performance_5min` (`inv_active_power`).
- Cek `mart_meter_performance_5min` (`sa_active_power`).
- Cek konsistensi unit (`W` vs `kW`) di source.
