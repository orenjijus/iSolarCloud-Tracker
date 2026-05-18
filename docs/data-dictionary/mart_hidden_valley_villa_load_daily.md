# Kamus Data: `mart.mart_hidden_valley_villa_load_daily`

**Grain:** 1 baris per `(date_key, site_id)`  
**Model dbt:** `dbt/models/marts/mart_hidden_valley_villa_load_daily.sql`  
**Peran pipeline:** agregasi energi harian beban villa Hidden Valley.

---

## Tujuan bisnis

Mengubah daya 5-menit menjadi energi harian (kWh) untuk kebutuhan monitoring konsumsi villa.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `date_key` | Tanggal operasi | grain harian |
| `site_id` | ID site HV | kunci site |
| `interval_count_5min` | Jumlah slot 5-menit | indikator kelengkapan data |
| `daily_inv_energy_kwh` | Energi dari inverter | `SUM(inv_active_power_kw * 5/60)` |
| `daily_sa_energy_kwh` | Energi dari Smart Assistant | `SUM(sa_active_power_kw * 5/60)` |
| `daily_villa_load_kwh` | Total energi beban villa | `SUM(beban_villa_kw * 5/60)` |

---

## Catatan penting

- Rumus dasar energi: `kWh = kW × (5/60)` per interval.
- Sumber utama dari `mart_hidden_valley_villa_load_5min`.

---

## Koreksi jika hasil aneh

- Cek kelengkapan interval (`interval_count_5min`).
- Cek nilai sumber 5-menit di `mart_hidden_valley_villa_load_5min`.
