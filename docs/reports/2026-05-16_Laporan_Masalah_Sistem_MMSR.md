# 16 Mei 2026 — Laporan Masalah & Rencana Perbaikan Sistem MMSR


| Field                         | Nilai                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------- |
| **Tanggal laporan**           | 16 Mei 2026                                                                       |
| **Lingkup**                   | Daily pipeline (`scripts/run_daily_pipeline.py`), ingestion API, transformasi dbt |
| **Sumber data**               | `scripts/logs/daily_pipeline_*.log` (6–16 Mei 2026)                               |
| **Status sistem keseluruhan** | **Tidak sehat** — ingestion berjalan; transformasi mart gagal sejak 14 Mei        |
| **Pembaruan 17 Mei 2026**     | **Ditutup** — lihat [§10 Pembaruan penutupan](#10-pembaruan-penutupan--17-mei-2026) |
| **Pembaruan 18 Mei 2026**     | **Temuan P0 baru** — Hidden Valley: cron + model dbt hilang; lihat [§13](#13-temuan-p0-hidden-valley--tech-debt-repo-dan-pipeline-intraday) |
| **Pembaruan 18 Mei 2026 (b)** | **Temuan P1 — SPF energi anomali** — `REVENUE_PERIOD` diterapkan; lihat [§14](#14-temuan-p1--anomali-energi-sumatera-prima-fibreboard-spf) |
| **Pembaruan 18 Mei 2026 (c)** | **Kamus metrik diperluas** — seluruh tabel `mart/fact` aktif (daily + HV) masuk data-dictionary & Metric Catalog; lihat [§11](#11-lingkup-4--kamus-data-formula-dan-verifikasi-user) |


---

## 1. Ringkasan eksekutif

Daily pipeline MMSR dijadwalkan setiap hari pukul **01:00** (Windows Task Scheduler). Dari analisis log:

- **Ingestion (FusionSolar + iSolarCloud)** masih berjalan normal setiap malam (~39k baris FusionSolar/hari).
- **Transformasi dbt** gagal **3 hari berturut-turut** (14, 15, 16 Mei) pada model `mart_site_performance_daily`.
- Run terakhir yang **sukses end-to-end**: **13 Mei 2026** (~02:10 WIB, data tanggal 12 Mei).
- Ada **1 insiden data historis** (10 Mei): ingest FusionSolar parsial untuk tanggal 9 Mei, tetapi pipeline tetap dilaporkan sukses.

**Prioritas tertinggi:** perbaiki schema mismatch `issue_date_remarks`, lalu backfill transformasi untuk tanggal **13–15 Mei 2026**.

---

## 2. Status komponen sistem


| Komponen               | Status                  | Keterangan                                               |
| ---------------------- | ----------------------- | -------------------------------------------------------- |
| Scheduler (01:00)      | Berjalan                | Log baru tercipta setiap hari                            |
| iSolarCloud ingest     | **OK**                  | ~3 menit/run, status `success`                           |
| FusionSolar ingest     | **OK** (dengan catatan) | ~32 menit, ~38–40k baris/hari; 10 Mei ada anomali volume |
| dbt `run` (28 model)   | **GAGAL**               | 24 PASS, 1 ERROR, 3 SKIP (sejak 14 Mei)                  |
| Exit code pipeline     | **1** (gagal)           | `run_daily_pipeline.py` return 1 jika dbt gagal          |
| Database raw (staging) | **Ter-update**          | Data kemarin masuk setiap malam                          |
| Mart / reporting layer | **Stale**               | Site & string performance tidak ter-update sejak 13 Mei  |


---

## 3. Riwayat run terjadwal (01:00)


| Tanggal run | Data yang diproses | Ingest      | dbt       | Durasi total         | Log file                             |
| ----------- | ------------------ | ----------- | --------- | -------------------- | ------------------------------------ |
| 6 Mei       | 5 Mei              | OK          | OK        | ~82 menit            | `daily_pipeline_20260506_010002.log` |
| 7 Mei       | 6 Mei              | OK          | OK        | ~79 menit            | `daily_pipeline_20260507_010001.log` |
| 8 Mei       | 7 Mei              | OK          | OK        | ~79 menit            | `daily_pipeline_20260508_010001.log` |
| 9 Mei       | 8 Mei              | OK          | OK        | ~74 menit            | `daily_pipeline_20260509_010001.log` |
| 10 Mei      | 9 Mei              | **Parsial** | OK        | ~40 menit            | `daily_pipeline_20260510_010001.log` |
| 11 Mei      | 10 Mei             | OK          | OK        | ~76 menit            | `daily_pipeline_20260511_010001.log` |
| 12 Mei      | 11 Mei             | OK          | OK        | ~74 menit            | `daily_pipeline_20260512_010001.log` |
| 13 Mei      | 12 Mei             | OK          | OK        | ~71 menit            | `daily_pipeline_20260513_010001.log` |
| **14 Mei**  | 13 Mei             | OK          | **GAGAL** | ~63 menit (dbt fail) | `daily_pipeline_20260514_010001.log` |
| **15 Mei**  | 14 Mei             | OK          | **GAGAL** | ~63 menit            | `daily_pipeline_20260515_010002.log` |
| **16 Mei**  | 15 Mei             | OK          | **GAGAL** | ~64 menit            | `daily_pipeline_20260516_010001.log` |


**FusionSolar — volume baris (run 01:00):**


| Run         | Tanggal data           | Rows inserted | Catatan                                      |
| ----------- | ---------------------- | ------------- | -------------------------------------------- |
| Normal      | 6–9, 11–16 Mei         | 33k–40k       | Baseline sehat                               |
| **Anomali** | **9 Mei** (run 10 Mei) | **15.244**    | ~39% dari baseline; banyak ERROR DNS         |
| Recovery?   | 10 Mei (run 11 Mei)    | 39.912        | Normal; **tidak otomatis mengisi gap 9 Mei** |


---

## 4. Daftar masalah & perbaikan

### P1 — KRITIS (aktif, blokir pipeline)

#### P1.1 — dbt gagal: `mart_site_performance_daily` / kolom `issue_date_remarks`


| Item                           | Detail                                                                                                                                                                                                                                                                                                                                                                                                              |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Gejala**                     | `dbt run failed` setiap malam sejak 14 Mei                                                                                                                                                                                                                                                                                                                                                                          |
| **Error**                      | `kolom « issue_date_remarks » tidak ada` — kolom ada di tabel `mart.mart_site_performance_daily` tetapi tidak dapat direferensikan dari query incremental                                                                                                                                                                                                                                                           |
| **Model**                      | `dbt/models/marts/mart_site_performance_daily.sql`                                                                                                                                                                                                                                                                                                                                                                  |
| **Penyebab (diagnosis)**       | **Schema drift** pada model incremental: query SELECT saat ini hanya memuat `is_issue_date` (baris ~641), **tidak** memuat `issue_date_remarks`. Tabel fisik di PostgreSQL masih menyimpan kolom lama; dbt incremental merge mencoba INSERT kolom tersebut dari subquery yang tidak menghasilkan kolom itu.                                                                                                         |
| **Model downstream yang SKIP** | `mart_site_performance_monthly`, `mart_string_performance_daily`, `mart_string_performance_monthly`                                                                                                                                                                                                                                                                                                                 |
| **Dampak**                     | KPI site/string harian & bulanan di Power BI / mart tidak ter-update untuk **13, 14, 15 Mei** (dan seterusnya sampai diperbaiki). Fact/staging upstream (24 model) tetap jalan.                                                                                                                                                                                                                                     |
| **Perbaikan yang disarankan**  | **Opsi A (disarankan):** Tambahkan kembali `issue_date_remarks` di SELECT final (mis. `NULL::text` atau join ke seed jika kolom remarks ditambahkan ke `seed_issue_dates`). **Opsi B:** Drop kolom dari tabel + `dbt run --full-refresh --select mart_site_performance_daily` jika kolom memang tidak dipakai lagi. **Opsi C:** Set `on_schema_change: sync_all_columns` di config model (hati-hati di production). |
| **Setelah fix**                | Jalankan backfill: `dbt run --select mart_site_performance_daily+` dengan vars window `reingest_start_date` / `reingest_end_date` untuk **2026-05-13** s/d **2026-05-15** (atau full-refresh model jika aman).                                                                                                                                                                                                      |
| **Verifikasi**                 | Log harus menampilkan `MMSR Daily Pipeline completed successfully!` dan `Done. PASS=28 ERROR=0`.                                                                                                                                                                                                                                                                                                                    |


**Cuplikan error (16 Mei 2026):**

```
Failure in model mart_site_performance_daily
kolom « issue_date_remarks » tidak ada
Done. PASS=24 WARN=0 ERROR=1 SKIP=3 NO-OP=0 TOTAL=28
```

**Referensi log:** `scripts/logs/daily_pipeline_20260514_010001.log`, `...20260515_010002.log`, `...20260516_010001.log`

---

### P2 — TINGGI (kualitas data, tidak memblokir scheduler saat ini)

#### P2.1 — Ingest FusionSolar parsial 9 Mei 2026


| Item           | Detail                                                                                                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Gejala**     | Run 10 Mei 01:00: hanya **15.244** baris FusionSolar (baseline ~39k)                                                                                                      |
| **Penyebab**   | Ratusan ERROR `NameResolutionError` — gagal resolve `sg5.fusionsolar.huawei.com`                                                                                          |
| **Dampak**     | Raw FusionSolar untuk **9 Mei** kemungkinan tidak lengkap; mart 9 Mei sudah ter-build saat pipeline masih sukses (10 Mei), sehingga **data mart 9 Mei bisa tidak akurat** |
| **Perbaikan**  | Re-ingest manual FusionSolar untuk `2026-05-09` via Streamlit / script reingest, lalu partial dbt refresh untuk site/string performance tanggal tersebut                  |
| **Verifikasi** | Bandingkan row count per site vs hari adjacent; cek log reingest tanpa ERROR DNS                                                                                          |


**Referensi log:** `scripts/logs/daily_pipeline_20260510_010001.log`

---

### P3 — SEDANG (intermiten, terutama run manual)

#### P3.1 — Kegagalan DNS / jaringan ke API vendor


| Item                    | Detail                                                                                                                 |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **Host yang terdampak** | `sg5.fusionsolar.huawei.com`, `gateway.isolarcloud.com.hk`                                                             |
| **Error**               | `getaddrinfo failed` / `NameResolutionError`                                                                           |
| **Kejadian di log**     | 5 Mei (run manual), 7 Mei (run manual), **10 Mei (run terjadwal)**                                                     |
| **Perbaikan**           | Cek DNS server, firewall, proxy, stabilitas jaringan server MA; pertimbangkan retry dengan backoff di layer API client |
| **Pencegahan**          | Alert jika `rows_inserted` FusionSolar < threshold (mis. < 30.000)                                                     |


#### P3.2 — Session FusionSolar expired


| Item          | Detail                                                                                                                                   |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Gejala**    | `USER_MUST_RELOGIN (Code: 305)`, `FusionSolar login failed: Unknown error`                                                               |
| **Kejadian**  | 5 Mei run manual (`daily_pipeline_20260505_161327.log`) — ingest tetap `success` dengan volume sangat rendah (3.160 baris)               |
| **Perbaikan** | Pastikan kredensial/env valid; perbaiki logic re-login di `fusionsolar` client; jangan laporkan `success` jika volume di bawah threshold |


---

### P4 — RENDAH (debt operasional / observability)

#### P4.1 — Pipeline melaporkan sukses meski ada ERROR API di tengah proses


| Item          | Detail                                                                                                           |
| ------------- | ---------------------------------------------------------------------------------------------------------------- |
| **Gejala**    | Banyak `ERROR` / `WARNING` di log ingest, tetapi `status: success` dan pada 10 Mei pipeline end-to-end sukses    |
| **Risiko**    | Data tidak lengkap tanpa terdeteksi oleh operator                                                                |
| **Perbaikan** | Tambah validasi post-ingest: row count vs baseline, % device dengan data, gagal pipeline jika di bawah threshold |


#### P4.2 — iSolarCloud `rows_inserted: None`


| Item          | Detail                                                                            |
| ------------- | --------------------------------------------------------------------------------- |
| **Gejala**    | Semua run menampilkan `rows_inserted: None` untuk iSolarCloud                     |
| **Risiko**    | Sulit memantau volume ingest iSolar dari log saja                                 |
| **Perbaikan** | Kembalikan hitungan baris di `isolarcloud.tasks` (opsional, tidak blokir operasi) |


---

## 5. Alur pipeline & titik kegagalan saat ini

```
[01:00 Task Scheduler]
        │
        ▼
┌───────────────────┐
│ Ingest parallel   │  ← OK (FS ~32 min, ISO ~3 min)
│ FS + iSolarCloud  │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ dbt run (28 model)│
│  24 PASS          │
│  1 ERROR ◄────────┼── mart_site_performance_daily (issue_date_remarks)
│  3 SKIP           │     ├── mart_site_performance_monthly
└─────────┬─────────┘     ├── mart_string_performance_daily
          │               └── mart_string_performance_monthly
          ▼
   Pipeline exit code 1
   (TIDAK "completed successfully")
```

---

## 6. Rencana tindakan (urutan disarankan)


| #   | Prioritas | Tindakan                                                                                     | Estimasi | Owner            |
| --- | --------- | -------------------------------------------------------------------------------------------- | -------- | ---------------- |
| 1   | P1        | Perbaiki `mart_site_performance_daily` (kolom `issue_date_remarks` atau full-refresh schema) | 1–2 jam  | Data engineering |
| 2   | P1        | Test manual: `dbt run --select mart_site_performance_daily+`                                 | 30 menit | Data engineering |
| 3   | P1        | Backfill mart 13–15 Mei 2026 (vars reingest atau full-refresh terkontrol)                    | 1–2 jam  | Data engineering |
| 4   | P1        | Verifikasi run scheduler malam berikutnya (17 Mei 01:00)                                     | —        | Ops              |
| 5   | P2        | Re-ingest FusionSolar `2026-05-09` + refresh mart tanggal tersebut                           | 2–4 jam  | Ops / DE         |
| 6   | P3        | Review DNS/jaringan server; dokumentasi runbook                                              | 1 hari   | Infra            |
| 7   | P4        | Implementasi alert row-count & gagal pipeline pada partial ingest                            | Backlog  | DE               |


---

## 7. Checklist verifikasi setelah perbaikan

- [x] `dbt run` menghasilkan **28/28 PASS**, 0 ERROR, 0 SKIP (13–16 Mei, 9 Mei, 26 Apr — 17 Mei malam)
- [x] Log menampilkan `MMSR Daily Pipeline completed successfully!`
- [x] `mart.mart_site_performance_daily` memiliki baris untuk **13, 14, 15, 16 Mei** 2026
- [x] Model monthly & string performance ter-update (tidak SKIP)
- [x] Backfill **9 Mei** (dbt-only) + re-ingest FS siang 17 Mei
- [x] Backfill **26 Apr 2026** — staging FS 9/9 plant, mart 26 site, issue date Pusan OK
- [ ] Power BI / laporan internal — validasi oleh pengguna bisnis

---

## 10. Pembaruan penutupan — 17 Mei 2026

**Status:** Isu P1 pipeline (dbt gagal 14–16 Mei) dan gap data terkait **ditutup**.

### Perbaikan kode

- Model `mart_site_performance_daily`: kolom `issue_date_remarks` = `CAST(NULL AS TEXT)`; join issue dates diperkuat (`asset_id` + `site_name`).

### Backfill dbt-only (17 Mei 2026, WIB)


| Tanggal | Hasil | Log |
| ------- | ----- | --- |
| 13–16 Mei | PASS 28/28 masing-masing | `scripts/logs/daily_pipeline_20260517_171626.log` |
| 9 Mei | PASS 28/28 | `scripts/logs/daily_pipeline_dbtonly_20260509_20260517_210301.log` |
| 26 Apr 2026 | PASS 28/28 (tanpa API; raw FS sudah lengkap) | `scripts/logs/daily_pipeline_dbtonly_20260426_20260517_222926.log` |


### Catatan per isu asli


| Isu laporan | Penutupan |
| ----------- | --------- |
| dbt gagal 14–16 Mei | Backfill dbt-only; ingest malam sudah OK |
| Mart stale setelah 13 Mei | Mart ter-update sampai **16 Mei** + backfill **9 Mei** |
| Ingest parsial 9 Mei | Re-ingest 17 Mei siang + dbt-only malam |
| Staging FS partial 26 Apr | dbt rebuild; Pusan & 4 plant lain kembali di staging/mart |

**Scheduler 17 Mei 01:00:** run untuk data **16 Mei** sukses end-to-end (post-fix).

Detail teknis: [Implementation Plan](./2026-05-16_Implementation_Plan.md#penyelesaian-backfill-manual-17-mei-2026--malam).

---

## 8. Lampiran — file log utama


| Periode / kejadian                | File log                                                                    |
| --------------------------------- | --------------------------------------------------------------------------- |
| Sukses terakhir (13 Mei)          | `scripts/logs/daily_pipeline_20260513_010001.log`                           |
| Gagal dbt mulai 14 Mei            | `scripts/logs/daily_pipeline_20260514_010001.log`                           |
| Gagal dbt 15 Mei                  | `scripts/logs/daily_pipeline_20260515_010002.log`                           |
| Gagal dbt 16 Mei                  | `scripts/logs/daily_pipeline_20260516_010001.log`                           |
| **Backfill 13–16 Mei (17 Mei)**   | `scripts/logs/daily_pipeline_20260517_171626.log`                           |
| **Backfill 9 Mei (17 Mei)**       | `scripts/logs/daily_pipeline_dbtonly_20260509_20260517_210301.log`          |
| Re-ingest 9 Mei (17 Mei siang)    | `scripts/logs/daily_pipeline_20260517_154524.log`                           |
| **Backfill 26 Apr (17 Mei)**      | `scripts/logs/daily_pipeline_dbtonly_20260426_20260517_222926.log`          |
| Ingest parsial 9 Mei (historis)   | `scripts/logs/daily_pipeline_20260510_010001.log`                           |
| Run manual bermasalah (referensi) | `scripts/logs/daily_pipeline_20260505_161327.log`, `...20260507_151201.log` |


**Script pipeline:** `scripts/run_daily_pipeline.py`  
**Batch scheduler:** `scripts/run_daily_pipeline.bat`

---

## 9. Lingkup 2 — Site performance daily: 8 site tidak terisi benar

**Gejala:** Site-site berikut **ada baris** di `mart.mart_site_performance_daily` (Apr–Mei 2026), tetapi `**daily_energy_mwh = 0`**, `**daily_ghi_kwh_m2` / `daily_poa_weighted_kwh_m2` = NULL**, availability ~0. Data **bukan** hilang di ingest; terputus di **seed / konfigurasi** dan filter mart.


| Site                                   | `site_id`             | Baris mart (Apr+) | Energy > 0 | GHI/POA > 0 |
| -------------------------------------- | --------------------- | ----------------- | ---------- | ----------- |
| PLTS Rooftop Weiss Tech                | `ISO_SITE_1789614`    | 42                | 0          | 0           |
| PT Gelora Djaja 1 MWp                  | `FS_SITE_NE=61847068` | 29                | 0          | 0           |
| Samator Gas Industri Malang 6,82 kWp   | `ISO_SITE_1734912`    | 40                | 0          | 0           |
| Samator Bali                           | `ISO_SITE_1731512`    | 9 (stops ~8 Apr)  | 0          | 0           |
| Samator Gas Industri Solo 6,82 kWp     | `ISO_SITE_1730375`    | 42                | 0          | 0           |
| Samator Gas Industri Sidoarjo 6,82 kWP | `ISO_SITE_1728142`    | 42                | 0          | 0           |
| Samator Indo Gas Semarang 9,92 kWp     | `ISO_SITE_1723575`    | 27                | 0          | 0           |
| Samator Ame Krian 6,82 kWp             | `ISO_SITE_1722752`    | 42                | 0          | 0           |


**Run terakhir mart ter-update:** 12–13 Mei 2026 (konsisten dengan kegagalan dbt Lingkup 1).

### 9.1 Alur data & titik putus

```
[API iSolar / FusionSolar]  →  staging  →  mart 5-min (meter/sensor/inverter)  →  facts  →  mart_site_performance_daily
                                    ✅              ✅ (raw ada)                    ⚠️/❌         ❌ (metrik 0/NULL)
```


| Lapisan                               | Samator (7 site)                         | Weiss Tech                              | Gelora Djaja                              |
| ------------------------------------- | ---------------------------------------- | --------------------------------------- | ----------------------------------------- |
| Staging ingest                        | ✅ Inverter + meter                       | ✅ + meteo (type 5)                      | ✅ FS meter + POA                          |
| `mart_meter_performance_5min`         | ✅ ~53–83k baris/site                     | ✅ 2 meter                               | ✅ 4 meter                                 |
| `seed_meter_config` (Revenue)         | ❌ **0 baris**                            | ❌ **0 baris**                           | ❌ **0 baris**                             |
| Energy di site mart                   | ❌ 0 (JOIN seed gagal)                    | ❌ 0                                     | ❌ 0                                       |
| `mart_sensor_measurements_5min`       | ❌ **0 baris** (no meteo)                 | ✅ ada, `sensor_type` NULL               | ✅ POA ter-tag                             |
| `seed_sensor_config`                  | ❌ 0 baris                                | ❌ 0 baris                               | ❌ 0 baris                                 |
| `mart_sensor_daily`                   | ❌ kosong                                 | ❌ tidak terbentuk                       | ✅ POA ada, `**sensor_capacity_kwp` NULL** |
| GHI/POA di site mart                  | ❌ NULL                                   | ❌ NULL                                  | ❌ NULL (filter kapasitas)                 |
| `fact_inverter_calculations_5min`     | ✅ ada                                    | ✅ ada                                   | ✅ ada                                     |
| `seed_site_config` (`totalinverters`) | ❌ tidak ada                              | ❌ tidak ada                             | ✅ 6 inverter                              |
| `seed_inverter_config`                | ❌ 0 baris DB                             | ⚠️ 4 baris, `**site_name` = `1789614`** | ✅ 6 baris                                 |
| `fact_site_calculations_5min`         | ⚠️ ada, `total_inverters` NULL → ratio 0 | ⚠️ sama                                 | ✅ MIT/availability jalan                  |
| `mart_string_performance_daily`       | ✅ baris ada, metrik site 0               | ✅                                       | ✅                                         |


### 9.2 Energy (meter revenue)

**Penyebab:** `mart_site_performance_daily` hanya menghitung energi dari meter yang join ke `staging.seed_meter_config` dengan `meter_type = 'Revenue'`:

```sql
JOIN seed_meter_config mc ON m.asset_id = CONCAT('ISO_' | 'FS_', mc.esn_code)
WHERE mc.meter_type = 'Revenue'
```

- Di DB: **tidak ada satu pun** baris `seed_meter_config` untuk Samator / Weiss / Gelora (`COUNT` Samator = 0).
- Data meter **sudah ada**. Contoh Samator Malang, `ISO_1734912_7_2_1`: delta harian ~**22–59 kWh** (Mei 2026) jika dihitung manual dari `positive_active_energy`.
- **Esn yang perlu di-seed** (format join): `1734912_7_2_1`, `1722752_7_2_1`, `1728142_7_2_1`, `1730375_7_2_1`, `1723575_7_2_1`, `1731512_7_1_1` (Bali), Weiss `1789614_7_6_1` / `1789614_7_7_1` (pilih meter revenue + polarity), Gelora: meter FS revenue (mis. `AM`* / `EM`* dari `dim_assets`).

**Perbaikan:** Tambah baris di `dbt/seeds/seed_meter_config.csv` → `dbt seed --select seed_meter_config` → backfill `mart_site_performance_daily` (setelah fix P1).

### 9.3 Irradiasi (sensor GHI / POA)

**Kelompok A — Samator (semua):**

- Di iSolarCloud: hanya **inverter (type 1)**, **meter (type 7)**, **logger/komunikasi (type 9, 0 perf row)**.
- **Tidak ada meteo station (device_type 5)** → tidak masuk `mart_sensor_measurements_5min` → tidak ada MIT/GHI/POA.
- Bukan bug ingest; **hardware/logger tidak mengirim irradiance** (atau perlu sumber lain).

**Opsi perbaikan Samator irradiasi:**

1. Pasang / aktifkan sensor irradiance di portal + pastikan type 5 ter-ingest; atau
2. `seed_sensor_site_mapping` (GHI_FALLBACK / GHI_COPY_FROM_SITE) dari site referensi; atau
3. Input manual `seed_ghi_adjustment_daily` jika hanya untuk analisis tertentu.

**Kelompok B — Weiss Tech:**

- Meteo **ada** (`1789614_5_8_1`, `1789614_5_9_1`, type 5, metric `irradiance` / `daily_irradiance`).
- Putus karena `**seed_sensor_config` kosong** → join gagal → `sensor_type` NULL → tidak masuk `mart_sensor_daily` / `fact_sensor_calculations_5min`.

**Perbaikan Weiss:** Seed sensor (mis. `device_id = 1789614_5_8_1`, `sensor_type = GHI`) + `seed_site_config` (capacity, `totalinverters = 4`) + meter revenue; perbaiki `seed_inverter_config.site_name` menjadi `**PLTS Rooftop Weiss Tech`** (bukan `1789614`).

**Kelompok C — Gelora Djaja:**

- POA di `mart_sensor_daily` **ada** (4 sensor EM*), nilai irradiance ~5–6 kWh/m²/hari.
- Putus di mart site karena filter:

```sql
WHERE sensor_type = 'POA'
  AND daily_irradiance_kwh_m2 IS NOT NULL
  AND sensor_capacity_kwp IS NOT NULL   -- ← semua NULL saat ini
```

- `seed_poa_sensor_config` / `seed_sensor_config` untuk FS `EM0010254C346758` dll. **belum terisi** → `sensor_capacity_kwp` NULL.

**Perbaikan Gelora:** Isi `seed_sensor_config` + `seed_poa_sensor_config` (capacity per orientasi) + meter revenue FS; optional GHI fallback jika tidak ada pyranometer GHI.

### 9.4 Inverter & availability


| Isu                                    | Dampak                                                               | Perbaikan                                                |
| -------------------------------------- | -------------------------------------------------------------------- | -------------------------------------------------------- |
| `seed_site_config` tanpa Samator/Weiss | `dim_assets.total_inverters` NULL → `power_available_ratio` selalu 0 | Tambah site + `totalinverters` (+ capacity kWp)          |
| Samator tanpa sensor MIT               | `mit = 0` → availability / unavailability tidak bermakna             | Selesaikan irradiasi (9.3) atau terima MIT=0             |
| `seed_inverter_config` tanpa Samator   | String mart: DC capacity 0, PR string kosong                         | Tambah baris inverter per `dim_assets` / SN dari staging |
| Weiss: `site_name` seed = `1789614`    | Risiko join string/site salah nama                                   | Samakan `site_name` dengan `dim_assets.site_name`        |
| Samator Bali                           | Mart berhenti ~8 Apr; staging inverter masih ada                     | Cek gap ingest / incremental; reingest jika perlu        |


Inverter **5-min ada** di `mart.fact_inverter_calculations_5min` untuk Samator & Weiss; yang hilang adalah agregasi site yang benar (seed + irradiasi).

### 9.5 Prioritas perbaikan (Lingkup 2)


| #   | Prioritas        | Tindakan                                                      | Site                                |
| --- | ---------------- | ------------------------------------------------------------- | ----------------------------------- |
| 1   | Tinggi           | Tambah `seed_meter_config` (Revenue) + `dbt seed`             | Semua 8                             |
| 2   | Tinggi           | Tambah `seed_site_config` (capacity, total inverters, tariff) | Samator + Weiss (+ capacity Gelora) |
| 3   | Tinggi           | Tambah `seed_sensor_config` (+ POA capacity Gelora)           | Weiss + Gelora                      |
| 4   | Sedang           | Samator: keputusan irradiasi (sensor fisik vs fallback)       | 7 Samator                           |
| 5   | Sedang           | Tambah `seed_inverter_config` + perbaiki nama Weiss           | Samator + Weiss                     |
| 6   | Rendah           | Reingest / validasi Samator Bali pasca 8 Apr                  | Bali                                |
| 7   | Blocker upstream | Selesaikan P1 `issue_date_remarks` lalu backfill mart         | Semua                               |


**Verifikasi setelah fix:**

```sql
-- Contoh: Samator Malang harus energy > 0 di hari cerah
SELECT date_key, daily_energy_mwh, daily_ghi_kwh_m2, daily_poa_weighted_kwh_m2, power_available_hours
FROM mart.mart_site_performance_daily
WHERE site_name = 'Samator Gas Industri Malang 6,82 kWp'
  AND date_key >= '2026-05-01'
ORDER BY date_key;
```

### 9.6 Referensi audit internal

- `docs/audit-tables/string_mart_site_fields_20260401_20260506.csv` — pola sama: energy ADA di string path mentah, GHI/POA TIDAK.
- `docs/audit-tables/19_string_capacity_source_vs_mart.csv` — Samator/Weiss: `in_seed_config = 0`.

---

## 10. Lingkup 3 — Trace perubahan meter / sensor / inverter per site

### 10.1 Masalah operasional

Konfigurasi device **tidak berada di satu tabel**, melainkan tersebar di banyak seed + `dim_assets`. Saat ada pergantian sensor (mis. PYR → WST), operator harus membuka beberapa file dan membaca logika dbt untuk memahami **kondisi existing**, **apa yang diganti**, **sejak kapan**, dan **kenapa**.

Tanpa ringkasan terpusat, kasus seperti **Shoetown Ligung Indonesia** dan **Charoen Pokphand Majalengka** (keduanya memakai WST Shoetown sejak Mei 2026) sulit diaudit.

### 10.2 Tiga lapisan konfigurasi (model mental)


| Lapisan                  | Sumber                                                                                | Arti                                                  | Contoh kolom kunci                      |
| ------------------------ | ------------------------------------------------------------------------------------- | ----------------------------------------------------- | --------------------------------------- |
| **A. Live (API)**        | `dimensions.dim_assets` + staging devices                                             | Device yang terlihat di portal **hari ini**           | `asset_id`, `device_type`               |
| **B. Baseline (seed)**   | `seed_meter_config`, `seed_sensor_config`, `seed_inverter_config`, `seed_site_config` | Identitas & tipe device yang **dikenali pipeline**    | `esn_code`, `sensor_type`, `meter_type` |
| **C. Override temporal** | `seed_sensor_site_mapping`, `seed_meter_site_mapping`, `seed_inverter_site_mapping`   | **Perubahan aturan** dengan rentang tanggal + `notes` | `mapping_type`, `effective_date_`*      |
| **D. Koreksi poin**      | `seed_ghi_adjustment_daily`, `seed_energy_adjustment_daily`                           | Override nilai **per hari** (bukan pergantian device) | `date_key`, `*_adjusted_value`          |


**Normal** = baseline (B) cocok dengan live (A), tanpa override aktif (C).  
**Berubah** = ada baris C (atau D) dengan `effective_date_start` / `notes` yang menjelaskan pergantian.

### 10.3 Peta seed → dampak di mart


| Kebutuhan bisnis              | Seed / tabel                               | `mapping_type` / field               | Dampak di mart                    |
| ----------------------------- | ------------------------------------------ | ------------------------------------ | --------------------------------- |
| Meter revenue mana            | `seed_meter_config`                        | `meter_type = Revenue`               | `daily_energy_mwh`                |
| Tipe & kapasitas sensor       | `seed_sensor_config`                       | `sensor_type`, `sensor_capacity`     | `mart_sensor_daily`               |
| GHI pakai sensor lain         | `seed_sensor_site_mapping`                 | `GHI_ACTUAL_OVERRIDE`                | `ghi_actual` / `daily_ghi_kwh_m2` |
| GHI adjusted / PR             | `seed_sensor_site_mapping`                 | `GHI_ADJUSTED`                       | `ghi_adjusted`, `pr_`*            |
| GHI dari site lain            | `seed_sensor_site_mapping`                 | `GHI_FALLBACK`, `GHI_COPY_FROM_SITE` | GHI saat site tidak punya PYR     |
| POA sensor fisik di site lain | `seed_sensor_site_mapping`                 | `POA_OVERRIDE`                       | POA & MIT di `fact_sensor_`*      |
| Inverter / availability       | `seed_inverter_config`, `seed_site_config` | `totalinverters`                     | `availability_percent`            |
| Override atribut device       | `seed_*_config_override` (Streamlit)       | polarity, sensor_type, dll.          | Join ke 5-min marts               |


**Catatan:** `seed_*_config_override` terdaftar di `dbt_project.yml` / Seed Manager; pastikan CSV ada & sudah `dbt seed` jika dipakai.

### 10.4 Contoh kasus: Shoetown & CP Majalengka (WST)

#### Baseline sensor (seed_sensor_config)


| Site          | Device           | Nama             | Tipe seed | Peran fisik                  |
| ------------- | ---------------- | ---------------- | --------- | ---------------------------- |
| Shoetown      | `1479456_5_26_1` | SLI-PYR          | GHI       | Pyranometer lokal            |
| Shoetown      | `1479456_5_10_2` | SLI-WST-01-A     | Weather   | **WST** (weather station)    |
| CP Majalengka | `1614122_5_16_1` | CPMJL-IRR-GHI-01 | GHI       | Pyranometer lokal Majalengka |


#### Override temporal (seed_sensor_site_mapping) — **ini yang menjelaskan “diganti jadi apa”**


| Periode (kalendar) | Site target       | `mapping_type`        | Sumber (`logical_site_id`)           | Kenapa (dari `notes`)                                                                            |
| ------------------ | ----------------- | --------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------ |
| Apr 2026 saja      | **CP Majalengka** | `GHI_FALLBACK`        | **Shoetown Ligung Indonesia** (site) | GHI sementara dari Shoetown (PYR); ditutup 30 Apr agar tidak bentrok saat Shoetown pindah ke WST |
| ≥ 1 Mei 2026       | **Shoetown**      | `GHI_ACTUAL_OVERRIDE` | `ISO_1479456_5_10_2`                 | `ghi_actual` pakai WST SLI-WST-01-A                                                              |
| ≥ 1 Mei 2026       | **Shoetown**      | `GHI_ADJUSTED`        | `ISO_1479456_5_10_2`                 | `ghi_adjusted` & PR dari WST yang sama                                                           |
| ≥ 1 Mei 2026       | **CP Majalengka** | `GHI_ACTUAL_OVERRIDE` | `ISO_1479456_5_10_2`                 | **Sensor fisik di plant Shoetown**, dipakai juga untuk Majalengka                                |
| ≥ 1 Mei 2026       | **CP Majalengka** | `GHI_ADJUSTED`        | `ISO_1479456_5_10_2`                 | Sama seperti Shoetown                                                                            |


**Timeline ringkas:**

```
< Apr 2026        Shoetown: GHI default dari PYR (seed)
                  Majalengka: GHI dari PYR lokal (1614122_5_16_1)

Apr 2026          Majalengka: GHI_FALLBACK ← Shoetown (site-level, PYR era)

≥ Mei 2026        Shoetown + Majalengka: ghi_actual & ghi_adjusted ← WST ISO_1479456_5_10_2
                  (perangkat di lokasi Shoetown, dipakai bersama)
```

**Cara baca di mart:** Bukan berarti Majalengka “punya” WST di `dim_assets` Majalengka — override mengarahkan perhitungan ke asset `**ISO_1479456_5_10_2`** milik Shoetown.

### 10.5 Checklist audit per site (5 menit)

1. **Live:** `SELECT * FROM dimensions.dim_assets WHERE site_name = '...' AND asset_level = 'Device'`
2. **Baseline:** baris di `seed_meter_config` / `seed_sensor_config` / `seed_inverter_config` untuk `site_id` / nama site
3. **Override aktif hari ini:**

```sql
SELECT * FROM staging.seed_sensor_site_mapping
WHERE device_id = 'Shoetown Ligung Indonesia'
  AND (effective_date_end IS NULL OR effective_date_end >= CURRENT_DATE)
ORDER BY effective_date_start;
```

1. **Registry gabungan:** jalankan `[dbt/analyses/site_device_configuration_registry.sql](../../dbt/analyses/site_device_configuration_registry.sql)` — set `site_name_filter` di CTE `params`
2. **Bandingkan ingest:** device di `stg_isolarcloud__devices` vs baseline — device baru di API tanpa seed = gap konfigurasi

### 10.6 Rekomendasi perbaikan proses (bukan hanya teknis)


| #   | Rekomendasi                                                                                            | Manfaat                                                 |
| --- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------- |
| 1   | **Wajib isi `notes`** di setiap baris `seed_sensor_site_mapping` / mapping baru                        | Audit trail “kenapa diganti”                            |
| 2   | **Satu baris per perubahan** + `effective_date_end` pada aturan lama                                   | Timeline jelas (lihat contoh Apr vs Mei Majalengka)     |
| 3   | Jalankan **registry SQL** setelah ubah seed & sebelum `dbt run`                                        | Cek `validity_status = active_today`                    |
| 4   | Pertimbangkan tabel baru `seed_device_change_log` (site, category, old, new, from, to, reason, ticket) | Change log terstruktur; seed mapping tetap untuk engine |
| 5   | Halaman Streamlit **“Site config summary”** (baca registry + filter site)                              | User tidak buka 6 CSV                                   |
| 6   | Saat ganti sensor fisik: update **B** (`seed_sensor_config`) **dan** tutup **C** lama                  | Baseline & override selaras                             |


### 10.7 Artefak yang disediakan

- Query analisis: `[dbt/analyses/site_device_configuration_registry.sql](../../dbt/analyses/site_device_configuration_registry.sql)`
- Seed Manager (Streamlit): tab per seed di `app/config/seed_config.py` — gunakan untuk edit, registry untuk **baca ringkas**

---

## 11. Lingkup 4 — Kamus data, formula, dan verifikasi user

Parameter mart/fact (terutama **a_KPI** / `energy_a_kpi_daily_mwh`) sebelumnya dominan dipahami data engineer. Per 18 Mei 2026, kamus diperluas agar user non-teknis bisa **membaca, mengecek, dan mengoreksi** metrik lintas pipeline.

### 11.1 Cakupan baru (aktif, dipakai pipeline)

| Jalur | Tabel aktif yang sudah masuk kamus + Metric Catalog |
| ----- | ---------------------------------------------------- |
| **Daily biasa** | `mart_inverter_performance_5min`, `mart_meter_performance_5min`, `mart_sensor_measurements_5min`, `fact_inverter_calculations_5min`, `fact_sensor_calculations_5min`, `fact_site_calculations_5min`, `mart_sensor_daily`, `mart_site_performance_daily` |
| **Hidden Valley (intraday)** | `fact_meter_active_power_corrected_5min`, `mart_battery_performance_5min`, `mart_hidden_valley_villa_load_5min`, `mart_hidden_valley_villa_load_daily`, `mart_meter_daily_hidden_valley` |

**Total katalog CSV aktif:** 13 tabel (masing-masing punya file `.md` + `.csv`).

### 11.2 Artefak

| Artefak | Lokasi |
| ------- | ------ |
| Panduan utama data-dictionary | `[docs/data-dictionary/README.md](../data-dictionary/README.md)` |
| Folder kamus per tabel (`.md`) | `[docs/data-dictionary/](../data-dictionary/)` |
| Folder katalog mesin-baca (`.csv`) | `[docs/data-dictionary/catalog/](../data-dictionary/catalog/)` |
| SQL verifikasi 1 site × 1 hari (site performance) | `[dbt/analyses/verify_site_performance_daily_one_day.sql](../../dbt/analyses/verify_site_performance_daily_one_day.sql)` |
| UI user | `app/pages/5_Metric_Catalog.py` |

### 11.3 Batasan eksplisit (sesuai keputusan bisnis)

- Fokus hanya tabel `mart/fact` yang **aktif dipakai pipeline/laporan**.
- Tabel yang belum dipakai operasional (contoh keluarga string/monthly tertentu) **tidak dimasukkan** ke katalog saat ini.

**a_KPI dalam satu kalimat:** `target_harian × (KPI_bulan / simulasi_bulan)` — bukan energi aktual, bukan availability.

---

## 12. Catatan untuk audit berikutnya

- **Lingkup 1:** Daily pipeline + dbt (§1–8).
- **Lingkup 2:** 8 site site performance (§9).
- **Lingkup 3:** Trace device / seed override (§10).
- **Lingkup 4:** Kamus data & verifikasi user (§11).
- **Lingkup 5:** Hidden Valley intraday & tech debt repo (§13).
- **Lingkup 6:** Anomali energi SPF / lesson learned meter revenue (§14).
- **Belum diaudit:** Airflow, Power BI gateway; halaman Streamlit "Metric Catalog" (rencana).

---

## 14. Temuan P1 — Anomali energi Sumatera Prima Fibreboard (SPF)

> **Laporan lengkap:** [2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md](./2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md)

### 14.1 Ringkasan (untuk pelaporan ke user)

| Aspek | Temuan |
| ----- | ------ |
| **Gejala bisnis** | Dashboard: energi aktual **~700 MWh/hari** dan PR GHI **>3.000%** (Apr–Mei 2026); GHI tetap normal |
| **Penyebab** | Tiga meter `Revenue` di seed; meter lama (`1680199_7_18_1`) counter **macet** + data lama ikut stream meter baru → algoritma `max−min` menghasilkan **~670 MWh palsu/hari** |
| **Perbaikan** | `REVENUE_PERIOD` di `seed_meter_site_mapping` (pola Samator) + reingest `mart_site_performance_daily` Apr–Mei 2026 |
| **Status** | **Selesai** — energi kembali **~8–13 MWh/hari**; refresh Power BI disarankan |

### 14.2 Lesson learned (singkat)

1. Pergantian meter revenue → **wajib** `REVENUE_PERIOD`, tidak cukup `ID_CONSOLIDATION`.
2. PR ekstrem + GHI normal → investigasi **meter revenue** dulu.
3. Site dengan 2 meter revenue paralel → beberapa baris `REVENUE_PERIOD` dengan rentang tanggal eksplisit.

---

## 13. Temuan P0 — Hidden Valley: tech debt repo dan pipeline intraday

> **Laporan lengkap:** [2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md](./2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md)

### 13.1 Ringkasan (sangat krusial)

| Aspek | Temuan |
| ----- | ------ |
| **Gejala bisnis** | Power BI Hidden Valley: meter, beban villa, EMMA, battery **kosong** sejak ~28 Apr; inverter masih tampil |
| **Penyebab utama** | Bukan API mati — **pipeline intraday HV mati** karena artefak hilang dari git |
| **Mekanisme** | (1) Task `MMSR HV Hourly Pilot` masih jalan tetapi script `cron\` **tidak ada** → gagal tiap jam; (2) model dbt HV **hanya di DB**, tidak di repo → `dbt run` harian tidak refresh mart HV; (3) `seed_metric_mapper` tanpa device 23070/39 → `sa_active_power` tidak terbentuk |
| **Severity** | **Fatal / silent failure** — raw ter-update, mart membeku, tidak ada alarm terpusat |
| **Status 18 Mei** | Cron + model + seed dipulihkan; backfill Apr–Mei; **uji cron sukses** |

### 13.2 Artefak yang hilang (daftar singkat)

| Kategori | Contoh path / objek |
| -------- | ------------------- |
| **Cron** | `cron/run_hv_pilot_cron.py`, `cron/run_hv_pilot_cron.bat` |
| **Mart dbt** | `mart_hidden_valley_villa_load_5min`, `mart_meter_performance_5min`, `mart_battery_performance_5min`, `mart_meter_daily_hidden_valley`, … |
| **Fact / dim** | `fact_meter_active_power_corrected_5min`, `dim_site`, `dim_minute` |
| **Staging** | `stg_fusionsolar__perf_battery_unpivoted` |
| **Seed (repo)** | `seed_site_merge`, `seed_metric_mapper` (baris EMMA/battery), beberapa master seed (di-export ulang dari DB) |

### 13.3 Dua pipeline — jangan dicampur

| Pipeline | Jadwal | Cakupan HV |
| -------- | ------ | ---------- |
| `scripts/run_daily_pipeline.bat` | 01:00 WIB | Ingest semua site + dbt **harian global** — **tidak** menggantikan mart intraday HV |
| `cron/run_hv_pilot_cron.bat` | Setiap jam (skip **jam 1–4 WIB**) | Harvest 2 plant HV + `dbt run tag:intraday` |

**Menghapus task HV atau tidak commit `cron/` = risiko kegagalan berulang.**

### 13.4 Gap meter AM 14–16 Mei 2026 (konfirmasi portal)

| Aspek | Temuan |
| ----- | ------ |
| **Bukan putus 14 Mei malam** | **14 Mei WIB penuh** (288 interval/meter) — sesuai grafik counter portal `[2026-05-14]` |
| **Putus dini hari 15 Mei** | Meter AM (PLN, Pump, Pool, Onsen) berhenti **~02:00–04:00 WIB** 15 Mei; Power BI terlihat “hilang” sekitar jam 2 |
| **Inverter / EMMA / battery** | Tetap normal 15–16 Mei di raw & mart |
| **Re-ingest 14–17 Mei** | Tidak menambah meter pasca ~04:00 WIB — API Huawei `data: []` (sama seperti portal) |
| **Detail** | [§6 laporan HV](./2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md#6-gap-meter-revenue-am--1416-mei-2026-konfirmasi-portal-fusionsolar) |

### 13.5 Rekomendasi wajib

1. Semua artefak §13.2 **wajib ada di git** sebelum dianggap “production”.
2. Monitor `MMSR HV Hourly Pilot` — `Last Result` harus 0 (exit 3 = skip jam 1–4 = OK).
3. Power BI: filter tanggal **WIB**, bukan `date_key` UTC mentah.
4. Inventarisasi berkala: tabel mart di DB vs `dbt ls`.
5. Outage meter AM: eskalasi **O&M / Huawei** jika counter tidak kembali di portal — backfill MMSR tidak bisa mengisi data yang tidak ada di API.

---

*Dibuat dari analisis log pipeline + query DB per 16 Mei 2026. Revisi setelah seed diperbarui dan P1 dbt diselesaikan. §13 ditambahkan 18 Mei 2026 (Hidden Valley). §13.4 timeline meter AM 18 Mei 2026. §14 ditambahkan 18 Mei 2026 (SPF energi / REVENUE_PERIOD).*