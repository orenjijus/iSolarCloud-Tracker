# Kamus Data: `mart.fact_meter_active_power_corrected_5min`

**Grain:** 1 baris per `(timestamp, asset_id)`  
**Model dbt:** `dbt/models/facts/fact_meter_active_power_corrected_5min.sql`  
**Peran pipeline:** koreksi active power meter Hidden Valley dari phase A/B/C.

---

## Tujuan bisnis

Menghasilkan daya meter 3-phase yang sudah dikoreksi untuk kasus phase C terbalik pada periode tertentu, agar kalkulasi beban HV akurat.

---

## Metrik inti

| Kolom | Arti | Logika ringkas |
|------|------|----------------|
| `timestamp` | Waktu 5-menit | waktu observasi |
| `date_key` | Tanggal operasi | turunan dari `timestamp` |
| `asset_id` | ID meter | `FS_<dev_id>` |
| `site_id` | ID plant/site | bisa hasil merge ke logical plant |
| `site_name` | Nama site | bisa hasil merge site |
| `pa_kw` | Phase A power | dari meter 5-menit |
| `pb_kw` | Phase B power | dari meter 5-menit |
| `pc_kw` | Phase C power | dari meter 5-menit |
| `active_power_corrected_kw` | Daya terkoreksi | normal: `PA+PB+PC`; jika phase C swapped pada window tertentu: `PA+PB-PC` |

---

## Koreksi jika hasil aneh

- Cek `seed_meter_config_override` (flag `phase_c_swapped`).
- Cek variabel tanggal koreksi (`meter_phase_correction_start_date/end_date`).
- Cek mapping site merge di `seed_site_merge`.
