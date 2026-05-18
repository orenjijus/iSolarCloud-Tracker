# 18 Mei 2026 — Temuan Kritis: Tech Debt Hidden Valley (Pipeline & Model dbt Hilang)


| Field | Nilai |
| ----- | ----- |
| **Tanggal laporan** | 18 Mei 2026 |
| **Site** | Hidden Valley (`NE=60951882` — inverter/EMMA/battery; `NE=78317340` — revenue meter) |
| **Prioritas** | **P0 — Fatal** (laporan Power BI terputus; bukan sekadar data kosong di raw) |
| **Status remediasi** | **Selesai** (repo + cron + backfill + uji cron 18 Mei 00:33 WIB) |
| **Dokumen terkait** | [2026-05-16_Laporan_Masalah_Sistem_MMSR.md](./2026-05-16_Laporan_Masalah_Sistem_MMSR.md) · [2026-05-17_Laporan_Konsultasi_MMSR_2_Mandays.md](./2026-05-17_Laporan_Konsultasi_MMSR_2_Mandays.md) |

---

## 1. Ringkasan eksekutif

Dashboard **Hidden Valley** di Power BI tampak “rusak” sejak **~28 April 2026**: data **inverter** masih mengalir, tetapi **meter revenue, beban villa, EMMA (Smart Assistant), dan battery** berhenti di mart — padahal ingest harian global (`scripts/run_daily_pipeline.py`) tetap berjalan.

Investigasi menunjukkan ini **bukan satu bug query**, melainkan **kegagalan sistemik** karena:

1. **Folder `cron/` dan script pilot HV hilang dari repository**, sementara Windows Task Scheduler **masih aktif** dan gagal setiap jam.
2. **Puluhan artefak dbt / seed tidak ada di git** — model HV hanya hidup sebagai tabel di database; `dbt run` harian **tidak pernah** memperbarui rantai intraday HV.
3. **Mapping metrik Smart Assistant & battery** tidak ada di `seed_metric_mapper.csv` → `sa_active_power` dan metrik battery tidak terbentuk di mart.

Dampak bisnis: **salah keputusan operasional** jika user mengira plant offline padahal pipeline pelaporan yang mati. Severity setara atau lebih tinggi dari insiden `issue_date_remarks` (14–16 Mei) karena **silent failure** — task scheduler “jalan” tetapi exit error, dan mart membeku tanpa alarm terpusat.

---

## 2. Gejala di Power BI vs kenyataan di database

| Lapisan | Gejala (user) | Kenyataan teknis |
| ------- | ------------- | ---------------- |
| Power BI | Meter / villa load / EMMA kosong setelah ~28 Apr | Mart intraday HV **tidak di-refresh** sejak cron mati |
| Power BI | Inverter masih ada | Inverter ikut **daily pipeline global** (`mart_inverter_performance_5min` tag harian) |
| Raw DB | Data masih masuk (daily ingest) | FusionSolar historical **ter-update** untuk HV; transformasi HV **terputus** |
| Task Scheduler | Tidak terlihat di repo | Task **`MMSR HV Hourly Pilot`** enabled; **Last Result: file not found** |

**Catatan timezone:** `date_key` di Postgres untuk hari kalender **WIB** disimpan sebagai `YYYY-MM-DD 17:00:00 UTC` (midnight WIB). Filter PBI `date_key = 17 Mei` (UTC) **salah** — gunakan tanggal WIB atau kolom date yang sudah di-shift.

---

## 3. Akar masalah (berlapis)

### 3.1 — P0: Cron pilot HV hilang dari repo (fatal)

| Item | Detail |
| ---- | ------ |
| Task Windows | `MMSR HV Hourly Pilot` — setiap jam |
| Script yang diharapkan | `cron\run_hv_pilot_cron.bat` → `run_hv_pilot_cron.py` |
| Kondisi repo | Folder **`cron/` tidak ada** (tech debt / tidak pernah di-commit) |
| Dampak | Sejak ~28 Apr, **tidak ada** harvest terfokus HV + **tidak ada** `dbt run tag:intraday` untuk HV |
| Jendela skip | **Jam 1–4 WIB** (01:00–04:59) — exit code `3` = sukses untuk scheduler |

**Remediasi:** Dipulihkan `cron/run_hv_pilot_cron.py` dan `cron/run_hv_pilot_cron.bat`. Uji manual **18 Mei 2026 00:33 WIB**: harvest + dbt **9/9 PASS**.

### 3.2 — P0: Model dbt HV tidak ada di git (fatal)

Model berikut **ada di database** tetapi **hilang dari repository** — sehingga developer lain / clone baru / CI **tidak bisa** mereproduksi pipeline:

**Mart (intraday / HV):**

- `mart_hidden_valley_villa_load_5min.sql`
- `mart_hidden_valley_villa_load_daily.sql`
- `mart_meter_daily_hidden_valley.sql`
- `mart_meter_performance_5min.sql` (versi dengan CTE `smart_assistant`)
- `mart_battery_performance_5min.sql`
- `mart_site_performance_daily_hidden_valley.sql` (masih placeholder `WHERE FALSE`)

**Fact:**

- `fact_meter_active_power_corrected_5min.sql`

**Staging:**

- `stg_fusionsolar__perf_battery_unpivoted.sql` (baru dibuat saat remediasi)

**Dimension:**

- `dim_minute.sql`, `dim_site.sql`

**Dampak:** Daily pipeline (`dbt run` tanpa `tag:intraday`) **tidak memilih** model-model ini → tabel mart HV **membeku** meskipun raw bertambah.

### 3.3 — P0: Seed & dependensi dbt hilang (memblokir `dbt parse` / `run`)

Saat remediasi, `dbt parse` gagal karena seed tidak ada di repo (tabel masih ada di `staging.*`):

| Seed / model | Fungsi |
| ------------ | ------ |
| `seed_site_merge` | Gabung plant meter `78317340` → logical `60951882` |
| `seed_site_equipment_config` | Join `dim_inverter_string_layout` |
| `seed_inverter_model_master`, `seed_pv_module_model_master` | Master data inverter/modul |
| `seed_meter_config_override` | Phase swap meter HV di `fact_meter_active_power_corrected_5min` |
| `meter_config_reference` | Alias tabel `staging.meter_config` (sudah ada di repo) |

**Dampak:** Tanpa file CSV di git, **tidak ada reproducible build**; risiko hilang lagi saat migrasi server.

### 3.4 — P1: `seed_metric_mapper` tidak memetakan EMMA & battery

| Device | `dev_type_id` | Metrik kritis | `unified_name` di mart |
| ------ | ------------- | ------------- | ---------------------- |
| Smart Assistant (EMMA) | 23070 | `active_power` | `sa_active_power` |
| Smart Assistant | 23070 | `active_cap`, `reverse_active_cap`, dll. | `sa_grid_*`, `sa_positive_active_energy`, … |
| Battery | 39 | `ch_discharge_power`, `charge_cap`, `discharge_cap` | `battery_*` |

Harvester **sudah** mengambil `smart_assistant` dan `battery`; tanpa baris seed, mart **menyaring semua** metrik tersebut.

**Remediasi:** Baris ditambahkan ke `dbt/seeds/seed_metric_mapper.csv` + `dbt seed`.

### 3.5 — P2: Inkonsistensi arsitektur (risiko berulang)

- **Dua pipeline** untuk satu site logis: global harian vs HV hourly — keduanya wajib terdokumentasi dan ada di git.
- Model lama `stg_fusionsolar__perf_battery_unpivoted_reference` masih di cache/manifest (error numeric); digantikan `stg_fusionsolar__perf_battery_unpivoted`.
- `mart_site_performance_daily_hidden_valley` belum diimplementasi (placeholder).

---

## 4. Topologi FusionSolar Hidden Valley

| Plant code | Peran | Device contoh |
| ---------- | ----- | ------------- |
| `NE=60951882` | Site utama (inverter, EMMA, battery) | Inverter-13, EMMA `NS2451145483`, Battery |
| `NE=78317340` | Load / revenue meter | AM revenue meters |

`seed_site_merge`: meter plant **78317340** → logical **60951882** untuk dashboard tunggal `site_name = 'Hidden Valley'`.

**Rumus beban villa (5-min):** `beban_villa_kw = inv_active_power_kw + sa_active_power_kw` (sumber: `mart_inverter_performance_5min` + `mart_meter_performance_5min`).

---

## 5. Remediasi yang dilakukan (17–18 Mei 2026)

| # | Tindakan | Hasil |
| - | -------- | ----- |
| 1 | Restore `cron/run_hv_pilot_cron.py` + `.bat` | Task Scheduler dapat jalan lagi |
| 2 | Restore / tambah model dbt HV di `dbt/models/` | Rantai intraday terdefinisi di git |
| 3 | Tambah `stg_fusionsolar__perf_battery_unpivoted` + tag `intraday` pada staging/mart terkait | Battery masuk mart |
| 4 | Lengkapi `seed_metric_mapper` (23070, 39) | `sa_active_power` terbentuk |
| 5 | Export seed hilang dari DB → `dbt/seeds/*.csv` | `dbt parse` sukses |
| 6 | Backfill `2026-04-28` s/d `2026-05-17` (vars `reingest_plant_codes`) | Mart inverter/EMMA/battery terisi; meter AM **parsial** (lihat §10) |
| 7 | Re-ingest khusus gap `2026-05-14`–`2026-05-17` + dbt | API tidak mengembalikan meter AM pasca ~15 Mei 04:00 WIB |
| 8 | Perbaikan `mart_meter_daily_hidden_valley` (filter `asset_id` 5 meter) | PLN/Pump/Pool/Onsen kembali di PBI untuk hari yang ada datanya |
| 9 | Uji cron end-to-end | **PASS** (18 Mei 00:33 WIB, 9 model) |
| 8 | Lengkapi data-dictionary + Metric Catalog untuk tabel aktif daily/HV | User non-teknis bisa lihat flow & metrik lintas pipeline |

**Verifikasi data 17 Mei WIB (setelah backfill):**

- `mart_hidden_valley_villa_load_5min`: **288** interval (hari penuh)
- `mart_hidden_valley_villa_load_daily` (`date_key` = 16 Mei 17:00 UTC = **17 Mei WIB**): **288** interval, ~36,15 kWh
- Metric Catalog: kamus diperluas ke seluruh tabel `mart/fact` aktif (daily biasa + HV), hanya yang dipakai operasional.

---

## 6. Gap meter revenue AM — 14–16 Mei 2026 (konfirmasi portal FusionSolar)

> **Koreksi timeline:** Meter **tidak** mati pada malam **14 Mei ~21:00 WIB**. Hari **14 Mei penuh** di portal. Putusnya dimulai **dini hari 15 Mei** (sekitar **02:00–04:00 WIB**), selaras dengan Power BI dan grafik counter di FusionSolar.

### 6.1 Timeline per device (kalender WIB)

| Periode | Inverter / EMMA / Battery | Meter AM (PLN, Pump, Pool, Onsen) |
| ------- | ------------------------- | ----------------------------------- |
| **14 Mei** | 288 interval/hari (normal) | **288 interval** — hari penuh (portal: counter naik sepanjang hari) |
| **15 Mei 00:00 – ~04:00** | Normal | **~49 interval** — counter masih bergerak (portal: garis datar setelah ~04:00) |
| **15 Mei ~04:00+** s/d seterusnya | Normal | **Tidak ada** di raw DB maupun respons API historis |
| **16–17 Mei** | Normal di mart | Meter AM tetap kosong di sumber |

**Catatan timezone:** Timestamp di Postgres sering dibaca sebagai UTC; **15 Mei 04:00 WIB = 14 Mei 21:00 UTC** — jangan disamakan dengan “malam 14 Mei” di kalender WIB.

### 6.2 Verifikasi silang (18 Mei 2026)

| Sumber | Temuan |
| ------ | ------ |
| **Portal FusionSolar** | `[2026-05-14] Meter-AM00102555606343`: positive/negative energy penuh 00:00–23:55. `[2026-05-15]`: data hanya awal hari (~04:00), lalu flat |
| **Raw `fusionsolar_historical_data`** | PLN: max WIB pada hari kalender **15 Mei = 04:00**; tidak ada baris setelah itu |
| **Re-ingest API 14–17 Mei** | Inverter/battery/EMMA: **288 titik/hari** untuk 15–16 Mei. Meter AM: API `data: []` untuk window yang sama — **bukan kegagalan dbt** |
| **Power BI** | Gap chart terlihat sekitar **15 Mei 02:00** — konsisten dengan putusnya **meter**, bukan inverter |

### 6.3 Dua masalah berbeda (jangan dicampur)

| # | Masalah | Gejala | Status remediasi |
| - | ------- | ------ | ---------------- |
| **A** | Pipeline HV / repo (§3) | Semua metrik HV bekum di mart sejak ~28 Apr | **Selesai** — cron + model + seed + backfill |
| **B** | Outage meter di FusionSolar / lapangan | Hanya **4 meter AM** putus dari **~15 Mei 04:00 WIB** | **Terdokumentasi** — backfill API **tidak bisa** mengisi yang tidak dikembalikan Huawei |

### 6.4 Implikasi operasional

1. **14 Mei** aman untuk meter — laporan harian/bulanan Mei tetap valid untuk hari itu.
2. **15 Mei** hanya **~4 jam pertama** yang bisa diisi di mart; energi harian 15 Mei **tidak representatif** (partial day).
3. **16 Mei dst.** untuk PLN/Pump/Pool/Onsen: anggap **data tidak tersedia** sampai portal/API menunjukkan counter hidup lagi.
4. Cron HV yang sudah jalan akan mengisi **hari berjalan**; tidak menggantikan historis yang tidak ada di API.

---

## 7. Alur operasional setelah perbaikan

```text
[Setiap jam, kecuali 01:00–04:59 WIB]
  Windows Task: MMSR HV Hourly Pilot
    → cron/run_hv_pilot_cron.bat
      → Harvest FS: NE=60951882, NE=78317340 (inverter,meter,battery,smart_assistant) — hari ini (WIB)
      → dbt run: stg_fusionsolar__perf_unpivoted, stg_fusionsolar__perf_battery_unpivoted, tag:intraday, mart_meter_daily_hidden_valley

[Setiap malam 01:00 WIB]
  scripts/run_daily_pipeline.bat — semua site (termasuk ingest HV, tetapi BUKAN menggantikan mart intraday HV)
```

**Kritis:** Menonaktifkan atau menghapus task **`MMSR HV Hourly Pilot`** tanpa menggabungkan logika ke pipeline harian = **kematian mart HV** dalam hitungan jam.

---

## 8. Rekomendasi pencegahan (wajib)

| # | Rekomendasi | Prioritas |
| - | ----------- | --------- |
| 1 | **Commit semua** model HV, `cron/`, dan seed ke git; branch protection / review untuk folder `dbt/models/marts/*hidden*`, `cron/` | P0 |
| 2 | Inventarisasi berkala: `dbt ls` vs tabel `mart.*` / `staging.*` — flag tabel tanpa model | P0 |
| 3 | Monitor Task Scheduler: alert jika `Last Result ≠ 0` untuk `MMSR HV Hourly Pilot` | P0 |
| 4 | Dokumentasi runbook HV di `docs/reports/` (dokumen ini) + link dari README dbt | P1 |
| 5 | Implementasi `mart_site_performance_daily_hidden_valley` atau hapus dari PBI jika tidak dipakai | P2 |
| 6 | `dbt clean` + hapus model ghost `*_reference` yang duplikat | P2 |
| 7 | Power BI: dokumentasi filter **tanggal WIB** vs `date_key` UTC | P1 |

---

## 9. Checklist verifikasi operasional

```powershell
# 1. File cron ada
Test-Path "...\MMSR API - Server MA\cron\run_hv_pilot_cron.bat"

# 2. Task scheduler
schtasks /Query /TN "MMSR HV Hourly Pilot" /FO LIST /V

# 3. Log terbaru
Get-ChildItem "...\logs\hv_pilot_*.log" | Sort-Object LastWriteTime -Descending | Select-Object -First 1

# 4. Data hari ini (WIB) — contoh SQL
# SELECT COUNT(*) FROM mart.mart_hidden_valley_villa_load_5min
# WHERE timestamp >= (CURRENT_DATE AT TIME ZONE 'Asia/Jakarta')::timestamptz AT TIME ZONE 'Asia/Jakarta' ...
```

---

## 10. Kesimpulan untuk manajemen

Insiden ini mengklasifikasikan **tech debt repository** sebagai risiko **P0 operasional**, setara dengan kegagalan pipeline harian global:

- **Adalah** kehilangan kode otomasi + model transformasi dari sistem version control (§3).
- **Silent failure** selama berminggu-minggu karena task gagal tanpa integrasi ke alerting yang sama dengan daily pipeline.
- **Tambahan:** outage **meter AM** terpisah sejak **15 Mei ~02:00–04:00 WIB** (§6) — **bukan** kegagalan pipeline saja; inverter/EMMA/battery tetap hidup di FusionSolar.

Setelah remediasi, **cron HV telah diverifikasi sukses**; seluruh artefak kunci sudah masuk repo. Tindak lanjut utama: **disiplin git**, **monitoring task HV**, dan **eskalasi ke O&M/Huawei** jika counter meter tidak kembali di portal.

---

*Dibuat 18 Mei 2026. Revisi: §6 timeline meter AM (15 Mei dini hari, bukan 14 Mei malam); re-ingest 14–17 Mei & konfirmasi portal.*
