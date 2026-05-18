# Investigasi Meter Samator Bali — Counter Stuck & Daily Pipeline

**Site:** Samator Bali (`ps_id` `1731512`, `site_id` `ISO_SITE_1731512`)  
**Tanggal laporan:** 17 Mei 2026  
**Status data mart (post-backfill 17 Mei):** Mar–Mei 2026 terisi; pola energi mencerminkan kondisi meter/inverter lapangan, bukan bug `REVENUE_PERIOD`.

**Dokumen terkait:**

- [2026-05-16_8Site_Energy_Availability_Report.md](./2026-05-16_8Site_Energy_Availability_Report.md)
- [2026-05-16_Implementation_Plan.md](./2026-05-16_Implementation_Plan.md)

---

## 1. Ringkasan eksekutif

| Temuan | Detail |
| ------ | ------ |
| **Konfirmasi lapangan** | Counter Forward Active Energy meter revenue **macet**, lalu **naik lagi ~16 Mei 2026** — selaras dengan data API/DB. |
| **Bukan bug dbt** | Setelah perbaikan seed `REVENUE_PERIOD` + refresh `stg_isolarcloud__devices`, mart menampilkan 0 energi pada periode counter flat — **benar secara data**. |
| **Gap Apr–Mei di mart** | **9 Apr – 15 Mei:** counter `p8030` tetap `8,056,857.91` Wh; inverter AC power = 0 → site tampak offline, bukan salah agregasi. |
| **Lonjakan 1 hari** | Saat counter hidup lagi, **~1,278 MWh** terhitung pada satu `date_key` = delta kumulatif selama stuck (~37 hari), **bukan** produksi harian. |
| **Refetch terjadwal** | Re-ingest iSolarCloud untuk tanggal **9, 14, 15, 16 Mei 2026** — gagal karena **daily pipeline rusak**; pipeline sudah diperbaiki, refetch **dijadwalkan** (belum dijalankan saat laporan ini ditulis). |

---

## 2. Perangkat & konfigurasi revenue

| Peran | `device_ps_key` | Nama portal | Periode `REVENUE_PERIOD` |
| ----- | ----------------- | ----------- | ------------------------ |
| Meter lama | `1731512_7_1_1` | MeterSamatorBali | 2025-12-16 s/d **2026-03-06** |
| Meter2 (revenue aktif) | `1731512_7_3_1` | Meter2 | **2026-03-07** s/d sekarang |
| Grid connection Meter2 | — | — | 2026-03-07 (portal) |

**Seed:**

- `seed_meter_config`: kedua meter `meter_type = Revenue`
- `seed_meter_site_mapping`: `mapping_type = REVENUE_PERIOD` (kolom `device_id` wajib quoted + `column_types: varchar` di `dbt_project.yml` agar `1731512_7_3_1` tidak terkorupsi menjadi `1731512731`)

---

## 3. Timeline counter & energi mart (Mar–Mei 2026)

Legenda metrik raw: `p8030` = Forward Active Energy (Wh), via `seed_metric_mapper` → `positive_active_energy`.

| Periode (kalender) | Counter Meter2 `1731512_7_3_1` | `daily_energy_mwh` (mart site) | Interpretasi |
| ------------------ | ------------------------------ | ------------------------------ | ------------ |
| 1–5 Mar | Meter lama flat di ~5,891,660 Wh | 0 | Revenue masih meter lama (sudah macet) |
| 6–7 Mar | Meter2 mulai ada data | ~0.001–0.002 | Transisi pergantian meter |
| 8 Mar – 7 Apr | Naik harian normal | ~0.02–0.10 / hari | ~25–50 kWh/hari — operasi normal |
| 8 Apr | Hanya +~183 Wh | ~0.0002 | Mulai macet |
| **9 Apr – 15 Mei** | **Flat 8,056,857.91 Wh** (288 sampel/5 menit/hari) | **0** | Counter stuck + inverter 0 kW |
| **~16 Mei** | Lompat ke ~9,334,976 Wh | **~1.28 MWh** pada satu `date_key`* | Counter hidup lagi; energi “tertuang” sekaligus |

\*Di DB, lonjakan dapat muncul pada `date_key` **15 Mei** (bucket WIB/UTC mart) — event lapangan **16 Mei**; selisih 1 hari = timezone, bukan dua kejadian berbeda.

### Verifikasi teknis (snapshot 17 Mei 2026)

**Raw `1731512_7_3_1` — hari counter naik lagi (WIB):**

| Tanggal | `p8030` min → max (Wh) | Δ kWh |
| ------- | ---------------------- | ----- |
| 13–14 Mei | 8,056,857.91 → 8,056,857.91 | 0 |
| 15 Mei* | 8,056,857.91 → 9,334,976.56 | **~1,278** |

**Inverter `1731512_1_2_1` — AC power (`p24` dll.):**

| Periode | Max power |
| ------- | --------- |
| s/d 7 Apr | ~22–50 kW |
| 8 Apr – 14 Mei | **0 kW** |

**Mart `mart_site_performance_daily` (Mar–16 Mei 2026):**

| Metrik | Nilai |
| ------ | ----- |
| Hari dengan `daily_energy_mwh` > 0 | **35 / 77** |
| Hari = 0 | **42** (termasuk gap 9 Apr–14 Mei + awal Mar) |
| Total MWh (Mar–16 Mei) | **~2.50** (termasuk lonjakan 1.28 MWh) |

---

## 4. Daily pipeline gagal — refetch terjadwal

**Konteks:** Ingest harian iSolarCloud untuk site ini (dan/atau global) **gagal** pada tanggal tertentu karena daily pipeline rusak. Pipeline **sudah diperbaiki**.

**Rencana re-ingest (belum dieksekusi saat laporan ini dibuat):**

| Tanggal | Alasan masuk daftar refetch |
| ------- | --------------------------- |
| **2026-05-09** | Gagal ingest harian (pipeline) |
| **2026-05-14** | Gagal ingest harian (pipeline) |
| **2026-05-15** | Gagal ingest harian (pipeline) |
| **2026-05-16** | Gagal ingest harian (pipeline); **counter meter naik lagi** di lapangan |

**Catatan:** Gap energi **9 Apr – 15 Mei** di mart utamanya karena **counter stuck / site offline**, bukan hanya kegagalan pipeline 9–16 Mei. Refetch tanggal di atas memastikan raw/staging **lengkap dan konsisten** setelah perbaikan pipeline; tidak mengisi otomatis hari-hari saat counter flat kecuali portal mengirim nilai baru.

### Perintah referensi (saat dijalankan)

```bash
cd isolarcloud
python isolarcloud_data_harvester.py --fetch-historical 2026-05-09 2026-05-09 --ps-ids 1731512 --device-types meter,inverter
python isolarcloud_data_harvester.py --fetch-historical 2026-05-14 2026-05-16 --ps-ids 1731512 --device-types meter,inverter
```

```bash
cd dbt
# Vars contoh — sesuaikan file atau gunakan vars\samator_reingest_20260301.yml dengan ps_ids hanya 1731512
dbt run --select stg_isolarcloud__devices stg_isolarcloud__perf_unpivoted --vars "reingest_start_date: 2026-05-09, reingest_end_date: 2026-05-16, reingest_ps_ids: '1731512'"
dbt run --select mart_meter_performance_5min mart_site_performance_daily --indirect-selection empty --vars "..."
```

---

## 5. Rekomendasi analitik & O&M

| # | Rekomendasi | Prioritas |
| - | ----------- | --------- |
| 1 | Jalankan refetch **9, 14–16 Mei** setelah pipeline stabil | P0 (terjadwal) |
| 2 | **Jangan** pakai `date_key` lonjakan (~1.28 MWh) untuk PR harian tanpa adjustment | P0 |
| 3 | Pertimbangkan `seed_issue_dates` / flag untuk rentang **9 Apr – 15 Mei 2026** (meter stuck + inverter 0) | P1 |
| 4 | O&M: dokumentasi outage Bali sejak ~8 Apr; verifikasi meter komunikasi pasca 16 Mei | P1 |
| 5 | Opsional: aturan DQ — counter flat > N hari berturut-turut → alert | P2 |

---

## 6. Perbandingan Solo / Semarang / Bali (post Meter2 + backfill)

| Site | Meter2 | Mar–Mei energi > 0 | Catatan |
| ---- | ------ | -------------------- | ------- |
| Solo `1730375` | `7_4_1` | 77/77 hari | Meter2 data kontinu setelah swap |
| Semarang `1723575` | `7_4_1` | 76/77 hari | Hampir penuh |
| **Bali `1731512`** | `7_3_1` | **35/77 hari** | Stuck 9 Apr–15 Mei + lonjakan 16 Mei |

---

## 7. Riwayat pekerjaan dbt (17 Mei 2026)

1. `dbt seed` — `seed_meter_config`, `seed_meter_site_mapping` (perbaikan `quote_columns` + `column_types`)
2. `stg_isolarcloud__devices` refresh (Meter2 masuk tabel devices)
3. `stg_isolarcloud__perf_unpivoted` — reingest Mar–16 Mei, `reingest_ps_ids: 1730375,1723575,1731512`
4. `dim_assets --indirect-selection empty`
5. `mart_meter_performance_5min` + `mart_site_performance_daily` — backfill sama

---

**Dibuat:** 17 Mei 2026  
**Konfirmasi lapangan:** counter stuck, naik lagi ~16 Mei 2026  
**Refetch pipeline dates:** 9, 14, 15, 16 Mei 2026 (terjadwal, pasca perbaikan daily pipeline)
