# Laporan Keterlibatan Konsultasi IT — Audit & Remediasi Sistem MMSR


| Field                     | Nilai                                                                  |
| ------------------------- | ---------------------------------------------------------------------- |
| **Penyedia Jasa**         | Kreasi Koleksi Kreatif                                                 |
| **Klien**                 | Solar Radiance — Departemen O&M                                        |
| **Periode Pelaksanaan**   | 16–17 Mei 2026 (2 Mandays)                                             |
| **Initial Brief**         | 15 Mei 2026                                                            |
| **Clarification Meeting** | 16 Mei 2026                                                            |
| **Tanggal Laporan**       | 17 Mei 2026                                                            |
| **Pembaruan**             | 18 Mei 2026 — tech debt repo (§3.9), HV, Gelora, SPF (§3.5–3.8) |
| **Status Engagement**     | **Selesai** (scope 2 mandays) · **Tech debt repo: teridentifikasi, belum ditutup penuh** |


**Dokumen terkait (lampiran teknis):**

- [2026-05-16_Laporan_Masalah_Sistem_MMSR.md](./2026-05-16_Laporan_Masalah_Sistem_MMSR.md)
- [2026-05-16_Clarification_Meeting_Brief.md](./2026-05-16_Clarification_Meeting_Brief.md)
- [2026-05-16_Implementation_Plan.md](./2026-05-16_Implementation_Plan.md)
- [2026-05-16_8Site_Energy_Availability_Report.md](./2026-05-16_8Site_Energy_Availability_Report.md)
- [2026-05-17_Config_Correction_Weiss_Gelora.md](./2026-05-17_Config_Correction_Weiss_Gelora.md)
- [2026-05-17_Samator_Bali_Meter_Investigation.md](./2026-05-17_Samator_Bali_Meter_Investigation.md)
- [2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md](./2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md) — **temuan P0 pasca-engagement (18 Mei)**
- [2026-05-18_Gelora_Calculation_Start_Date_Update.md](./2026-05-18_Gelora_Calculation_Start_Date_Update.md) — **penyelarasan tanggal operasional resmi Gelora (18 Mei)**
- [2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md](./2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md) — **remediasi anomali energi SPF (18 Mei)**

---

## 1. Ringkasan Eksekutif

Kreasi Koleksi Kreatif dikontrak selama **2 mandays (16–17 Mei 2026)** untuk mengaudit, mendiagnosis, dan memperbaiki sistem pelaporan performa PLTS milik **Solar Radiance** yang berjalan di platform **MMSR**.

Sistem ini setiap malam secara otomatis:

1. Mengambil data dari portal vendor (**Huawei FusionSolar** dan **iSolarCloud**)
2. Menyimpan data ke database
3. Memproses data menjadi laporan harian site performance
4. Menyajikan hasil ke tim O&M melalui **dashboard Power BI**

### Kondisi saat brief diterima (15 Mei 2026)

- Laporan harian site performance **berhenti ter-update sejak 14 Mei** (3 malam berturut-turut gagal)
- **Delapan site** menampilkan energi = 0 dan irradiance kosong sepanjang April–Mei, padahal data mentah dari portal sudah masuk ke database
- Aturan sensor, meter, dan KPI tersebar di banyak file konfigurasi — sulit diaudit tanpa bantuan tim teknis

### Hasil setelah 2 mandays


| Aspek                                                  | Hasil                                                                                                            |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| Pipeline harian (transformasi data)                    | **Diperbaiki** — berjalan normal kembali                                                                         |
| Backfill data 13–16 Mei 2026                           | **Selesai**                                                                                                      |
| Konfigurasi 8 site audit                               | **Selesai** — meter revenue & kapasitas terdaftar                                                                |
| Backfill historis (Des 2025 – Mei 2026)                | **Selesai**                                                                                                      |
| Koreksi Weiss Tech & Gelora Djaja                      | **Selesai** (17 Mei)                                                                                             |
| Investigasi Samator Bali                               | **Terdokumentasi**                                                                                               |
| Backfill dbt **9 Mei**, **13–16 Mei**, **26 Apr 2026** | **Selesai** (17 Mei malam, PASS 28/28)                                                                           |
| **Hidden Valley — gejala pipeline intraday**           | **Remediasi insiden selesai 18 Mei** — lihat §3.5 (satu manifestasi tech debt) |
| **Gelora — calculation start date**                    | **Diperbarui 18 Mei** (`2025-12-01` → `2026-04-07`) — lihat §3.7 |
| **SPF — anomali energi ~700 MWh**                      | **Diperbaiki 18 Mei** dengan `REVENUE_PERIOD` + reingest — lihat §3.8 |
| **Tech debt repository — skala PR besar**              | **Teridentifikasi P0 fatal** — ~**1/3 artefak hilang** dari git; lihat **§3.9** |


**Kesimpulan:** Seluruh isu kritikal dalam **scope engagement 16–17 Mei** telah diselesaikan (pipeline, 8 site, backfill). **Temuan fatal tambahan 18 Mei** bukan hanya insiden HV: audit menunjukkan **technical debt repository** — banyak file dan bahkan **folder** tidak pernah ada di git, sehingga risiko kegagalan berulang dan kebutuhan **PR besar** (~sepertiga basis kode/fitur tidak ter-version-control). Insiden Hidden Valley, Gelora calc start, dan SPF adalah **gejala yang sudah ditangani**; **penutupan penuh tech debt** (dashboard operasional, pipeline HV, seluruh model dbt major/minor, seed, fitur tambahan) **belum selesai** dan memerlukan sprint/PR tersendiri.

**Catatan:** Item minor engagement awal (GHI Samator, kapasitas POA) tetap ditunda. **Tech debt repo (§3.9) memblokir reproducible build** dan harus diprioritaskan setelah mandays.

---

## 2. Timeline Keterlibatan


| Tanggal                       | Fase                          | Kegiatan                                                                                        | Output                                        |
| ----------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **15 Mei 2026**               | Initial Brief                 | Penerimaan brief dari Departemen O&M Solar Radiance                                             | Pemahaman scope awal                          |
| **16 Mei 2026 (pagi)**        | Clarification Meeting         | Rapat klarifikasi: validasi temuan, prioritas perbaikan, konfirmasi definisi KPI & pemilik data | Scope dan urutan kerja disepakati             |
| **16 Mei 2026 (siang–malam)** | Hari 1 — Implementasi         | Perbaikan pipeline, konfigurasi 8 site, backfill Apr–Mei, investigasi meter Samator             | Pipeline pulih; 5 site aktif penuh di laporan |
| **17 Mei 2026**               | Hari 2 — Koreksi & Finalisasi | Koreksi sensor Weiss & Gelora, investigasi Bali, backfill historis, dokumentasi                 | Semua item P0/P1 selesai; laporan diserahkan  |


---

## 3. Temuan & Tindakan Perbaikan

### 3.1 Pipeline harian berhenti sejak 14 Mei 2026 — **SELESAI**


| Item              | Keterangan                                                                                                                             |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| **Prioritas**     | P0 — Kritis                                                                                                                            |
| **Gejala**        | Dashboard Power BI tidak ter-update setelah 13 Mei; proses malam keluar dengan status gagal                                            |
| **Penyebab**      | Ketidaksesuaian struktur database pada tahap transformasi data (kolom lama masih direferensikan, padahal sudah tidak dipakai di query) |
| **Dampak bisnis** | KPI site dan string untuk tanggal **13, 14, 15 Mei** tidak tersedia di laporan                                                         |
| **Tindakan**      | Perbaikan query transformasi, uji manual, backfill data 13–16 Mei                                                                      |
| **Status**        | **Selesai** — pipeline berjalan normal; data 13–16 Mei tersedia di laporan                                                             |


**Catatan tambahan — 9 Mei:** Ingest FS parsial pada run **10 Mei** (~39% volume). **Ditutup 17 Mei:** re-ingest FS+iSolar siang + dbt-only malam (PASS 28/28).

**Catatan tambahan — 26 April 2026:** Staging FusionSolar hanya 4/9 plant di mart; raw lengkap. **Ditutup 17 Mei malam** dengan dbt-only (tanpa API); 26 site di mart, Pusan + issue dates ter-flag.

---

### 3.2 Delapan site dengan angka nol di dashboard — **SELESAI (konfigurasi)**

Delapan site berikut muncul di dashboard tetapi menampilkan **energi = 0** dan **irradiance kosong** sepanjang April–Mei:


| No  | Site                          | Portal      |
| --- | ----------------------------- | ----------- |
| 1   | PLTS Rooftop Weiss Tech       | iSolarCloud |
| 2   | PT Gelora Djaja 1 MWp         | FusionSolar |
| 3   | Samator Gas Industri Malang   | iSolarCloud |
| 4   | Samator Gas Industri Sidoarjo | iSolarCloud |
| 5   | Samator Ame Krian             | iSolarCloud |
| 6   | Samator Gas Industri Solo     | iSolarCloud |
| 7   | Samator Indo Gas Semarang     | iSolarCloud |
| 8   | Samator Bali                  | iSolarCloud |


**Akar masalah (bukan masalah API):** Data mentah dari portal **sudah masuk** ke database. Yang belum lengkap adalah **konfigurasi device** — sistem belum tahu meter mana yang dianggap "meter revenue" dan sensor mana yang dipakai untuk irradiance.

**Tindakan yang dilakukan:**

- Pendaftaran meter revenue untuk semua 8 site
- Koreksi kapasitas site (Weiss 238,08 kWp; Bali 9,92 kWp; Gelora 1000,4 kWp)
- Koreksi sensor Weiss (tipe POA, bukan GHI) dan Gelora (GHI + POA)
- Backfill historis Des 2025 – Mei 2026
- Konfigurasi pergantian meter (Meter2) untuk Solo, Semarang, Bali

---

### 3.3 Samator Bali — outage hardware ~37 hari — **TERDOKUMENTASI**


| Item                    | Keterangan                                                                                                                                                 |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Prioritas**           | P1 — Tinggi                                                                                                                                                |
| **Periode**             | ~9 April – 15 Mei 2026                                                                                                                                     |
| **Gejala**              | Counter meter berhenti naik; inverter menunjukkan daya 0 kW                                                                                                |
| **Penyebab**            | Kondisi lapangan (meter/inverter offline), **bukan kegagalan sistem MMSR**                                                                                 |
| **Konfirmasi lapangan** | Counter naik kembali per ~16 Mei 2026 — selaras dengan data di database                                                                                    |
| **Dampak di laporan**   | 42 hari energi = 0; saat meter hidup kembali, satu hari menampilkan lonjakan ~1,28 MWh (akumulasi selama periode stuck — **bukan produksi harian normal**) |
| **Tindakan**            | Dokumentasi outage; re-ingest data 9 & 14–16 Mei; rekomendasi flagging hari anomali                                                                        |
| **Status**              | **Terdokumentasi** — re-ingest selesai; tindak lanjut hardware O&M direkomendasikan                                                                        |


---

### 3.4 Pergantian meter Solo / Semarang / Bali — **SELESAI**

Ketiga site menggunakan **meter baru (Meter2)** sejak awal Maret 2026. Konfigurasi dua periode meter (lama dan baru) telah diselesaikan.


| Site             | Meter lama (s/d) | Meter baru (mulai) | Status laporan Mar–Mei 2026                  |
| ---------------- | ---------------- | ------------------ | -------------------------------------------- |
| Samator Solo     | 28 Feb 2026      | 1 Mar 2026         | **77/77 hari** energi > 0                    |
| Samator Semarang | 26 Feb 2026      | 2 Mar 2026         | **76/77 hari** energi > 0                    |
| Samator Bali     | 6 Mar 2026       | 7 Mar 2026         | **35/77 hari** (terpengaruh outage hardware) |


---

### 3.5 Hidden Valley — gejala pipeline intraday (manifestasi tech debt) — **insiden ditutup 18 Mei**

> **Konteks:** HV adalah **kasus paling terlihat** dari masalah lebih luas di **§3.9** (artefak hilang dari git). Remediasi di bawah ini **menutup insiden operasional HV**, bukan seluruh audit tech debt repository.


| Item               | Keterangan                                                                                                                                                                                                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Prioritas**      | **P0 — Fatal** (silent failure; laporan PBI salah)                                                                                                                                                                                                                   |
| **Gejala**         | Dashboard HV: meter, beban villa, EMMA, battery **kosong** sejak ~28 Apr 2026; inverter masih ada                                                                                                                                                                    |
| **Bukan penyebab** | Bukan outage plant; raw FusionSolar **masih ter-ingest** oleh pipeline harian global                                                                                                                                                                                 |
| **Penyebab langsung** | (1) Folder `cron/` hilang — task `MMSR HV Hourly Pilot` gagal tiap jam; (2) model dbt HV tidak di-commit; (3) `seed_metric_mapper` tanpa EMMA (23070) & battery (39) |
| **Akar masalah**   | **Tech debt repository** — lihat **§3.9**                                                                                                                                                                                                                          |
| **Dampak bisnis**  | Keputusan O&M berdasarkan PBI **misleading** — terlihat seperti data plant mati                                                                                                                                                                                      |
| **Tindakan**       | Restore cron + subset model/seed HV; backfill 28 Apr–17 Mei; uji cron sukses (18 Mei 00:33 WIB)                                                                                                                                                                        |
| **Status insiden** | **Remediasi HV selesai** — detail: [2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md](./2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md)                                                                                                                                  |
| **Status tech debt** | **Belum ditutup penuh** — masih ada model/seed/fitur di DB atau server yang belum masuk git (§3.9)                                                                                                                                                                  |

---

### 3.6 Hidden Valley — gap meter revenue AM 14–16 Mei 2026 (konfirmasi portal)

> **Koreksi timeline:** Meter **tidak** mati malam **14 Mei ~21:00 WIB**. Hari **14 Mei penuh** di portal FusionSolar. Putusnya dimulai **dini hari 15 Mei** (~**02:00–04:00 WIB**).


| Tanggal (WIB)             | Inverter / EMMA / battery  | Meter AM (PLN, Pump, Pool, Onsen)           |
| ------------------------- | -------------------------- | ------------------------------------------- |
| **14 Mei**                | Normal (288 interval/hari) | **Penuh** — counter naik sepanjang hari     |
| **15 Mei 00:00 – ~04:00** | Normal                     | **~49 interval** — partial day              |
| **15 Mei ~04:00+**        | Normal                     | **Tidak ada** di raw DB maupun API historis |
| **16–17 Mei**             | Normal di mart             | Meter AM tetap kosong di sumber             |


- Re-ingest **14–17 Mei** tidak menambah meter pasca ~04:00 WIB — respons API Huawei `data: []` (selaras portal).
- **Bukan kegagalan dbt** setelah remediasi §3.5; ini **outage meter di FusionSolar / lapangan** — eskalasi **O&M / Huawei** jika counter tidak kembali.
- Detail lengkap: [§6 laporan HV](./2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md#6-gap-meter-revenue-am--1416-mei-2026-konfirmasi-portal-fusionsolar).

---

### 3.7 Gelora — penyesuaian calculation start date — **SELESAI (18 Mei)**


| Item              | Keterangan                                                                                                                                                                  |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Prioritas**     | P1 — Tinggi                                                                                                                                                                 |
| **Gejala**        | Gelora masih memiliki baris historis sebelum tanggal operasional resmi                                                                                                      |
| **Penyebab**      | `CalculationStartDate` di seed site masih `2025-12-01`, belum selaras dengan tanggal operasi bisnis                                                                         |
| **Tindakan**      | Update `seed_site_config` menjadi `2026-04-07`, refresh `dim_assets`, re-run mart, lalu cleanup baris stale sebelum calc start                                              |
| **Dampak bisnis** | Laporan Gelora menjadi selaras dengan kebijakan tanggal operasional resmi; data Feb–Mar tidak lagi ikut agregasi                                                            |
| **Status**        | **Selesai** — 32 baris stale sebelum calc start dihapus; detail: [2026-05-18_Gelora_Calculation_Start_Date_Update.md](./2026-05-18_Gelora_Calculation_Start_Date_Update.md) |


---

### 3.8 SPF — anomali energi dan PR tinggi — **SELESAI (18 Mei)**


| Item              | Keterangan                                                                                                                                                                      |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Prioritas**     | P1 — Tinggi                                                                                                                                                                     |
| **Gejala**        | Energi SPF melonjak ~700–760 MWh/hari dan PR GHI >3.000% pada Apr–Mei                                                                                                           |
| **Penyebab**      | Meter lama + meter baru sama-sama aktif sebagai `Revenue`; data stale meter lama ikut stream meter baru                                                                         |
| **Tindakan**      | Tambah mapping `REVENUE_PERIOD` di `seed_meter_site_mapping` dan reingest `mart_site_performance_daily` Apr–Mei                                                                 |
| **Dampak bisnis** | Nilai energi kembali realistis (~8–13 MWh/hari), anomali PR hilang, dashboard kembali dapat dipakai untuk evaluasi operasional                                                  |
| **Status**        | **Selesai** — tidak ada lagi hari SPF >50 MWh di window perbaikan; detail: [2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md](./2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md) |


---

### 3.9 Technical debt repository — artefak hilang dari git — **P0 FATAL / BELUM DITUTUP PENUH**

Ini adalah **temuan paling fatal** yang terungkap pasca-engagement (18 Mei): basis kode di server produksi **tidak selaras** dengan repository git. Estimasi kasar: **~sepertiga** artefak yang dipakai operasional (pipeline, transformasi, UI, konfigurasi) **tidak pernah di-commit** atau folder utamanya **hilang** — sehingga remediasi HV hanya memulihkan **bagian yang terlihat rusak**, sementara risiko kehilangan ulang tetap tinggi tanpa **PR besar** dan inventarisasi penuh.

#### Mengapa fatal

| Risiko | Dampak |
| ------ | ------ |
| Clone / migrasi server baru | Build tidak bisa direproduksi; `dbt parse` / pipeline gagal |
| Silent failure | Ingest jalan, mart/fitur mati — seperti HV ~28 Apr |
| PR recovery besar | Banyak file sekaligus; review & regresi sulit |
| Fitur “ada di DB” tapi tidak di git | Hanya satu server yang “tahu” cara jalan |

#### Kategori artefak yang hilang atau tidak lengkap di git

| Kategori | Contoh / dampak | Blokir pipeline? | Blokir fitur tambahan? |
| -------- | ----------------- | ---------------- | ---------------------- |
| **1. Dashboard operasional (terbesar)** | Folder `app/` (Streamlit): Re-ingest, Seed Manager, dbt Commands, Run History, Metric Catalog — **seluruh modul UI** pada awal audit **belum ter-track** di git | Tidak langsung | **Ya** — operator tidak punya sumber kebenaran di repo |
| **2. Pipeline Hidden Valley** | Folder `cron/` (`run_hv_pilot_cron.py`, `.bat`); task `MMSR HV Hourly Pilot` | **Ya** (mart intraday HV) | — |
| **3. Model dbt — major (HV & inti)** | `mart_hidden_valley_villa_load_*`, `mart_meter_performance_5min` (versi SA), `mart_battery_performance_5min`, `fact_meter_active_power_corrected_5min`, `stg_fusionsolar__perf_battery_unpivoted`, `dim_minute`, `dim_site` | **Ya** (rantai intraday) | — |
| **4. Model dbt — minor / placeholder** | `mart_site_performance_daily_hidden_valley` (placeholder), model ghost `*_reference`, simulasi/KPI yang ada di DB tapi tidak di `dbt ls` | Sebagian | **Ya** |
| **5. Seed & dependensi** | `seed_site_merge`, `seed_site_equipment_config`, `seed_inverter_model_master`, `seed_pv_module_model_master`, `seed_meter_config_override`, baris EMMA/battery di `seed_metric_mapper` | **Ya** (`dbt parse` / run) | **Ya** (string layout, phase swap meter) |
| **6. Lainnya** | Airflow (`airflow/`), skrip cron harian tambahan, dokumentasi kamus metrik, override seed Streamlit | Tergantung fitur | **Ya** |

#### Status remediasi vs sisa pekerjaan

| Lapisan | Status 18 Mei | Sisa pekerjaan |
| ------- | ------------- | -------------- |
| Insiden HV (gejala) | **Ditutup** — cron + subset model/seed + backfill | Monitoring task; meter AM outage lapangan (§3.6) |
| Subset dbt HV + cron | **Dipulihkan ke repo** | Inventarisasi **semua** tabel mart/staging di DB vs `dbt ls` |
| Dashboard `app/` | **Belum audit commit penuh** | Commit seluruh modul; CI/smoke test Streamlit |
| Seed lengkap | **Sebagian** (export dari DB saat HV) | Semua CSV seed yang dipakai production masuk git |
| PR konsolidasi | **Belum** | Satu atau beberapa PR terstruktur; branch protection `cron/`, `dbt/models/**`, `app/` |

**Rekomendasi wajib (pasca-mandays):**

1. **Inventarisasi:** bandingkan objek di PostgreSQL (`mart.*`, `staging.*`) dengan `dbt ls` + daftar file di server.
2. **Prioritas commit:** `cron/` → seed yang memblokir `dbt parse` → model HV → `app/` (dashboard).
3. **Satu PR besar atau PR berurutan** per domain (pipeline / dbt / UI) dengan checklist regresi.
4. **Kebijakan:** tidak ada perubahan production-only; semua artefak wajib di git sebelum dianggap “production”.

Detail insiden HV (subset dari tech debt ini): [2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md](./2026-05-18_Hidden_Valley_Pipeline_Tech_Debt.md).

---

## 4. Status Akhir per Site (per 17 Mei 2026)


| Site                    | Energi di laporan                 | GHI / POA                            | Catatan                                        |
| ----------------------- | --------------------------------- | ------------------------------------ | ---------------------------------------------- |
| PLTS Rooftop Weiss Tech | Aktif (Mar–Mei)                   | POA: menunggu kapasitas sensor       | GHI: perlu keputusan sumber                    |
| PT Gelora Djaja 1 MWp   | Aktif (7 Apr–Mei)                 | GHI ~38 hari; POA menunggu kapasitas | Revenue meter = EM-POI; calc start resmi 7 Apr |
| Samator Malang          | Aktif (Jan–Mei)                   | —                                    | Tidak ada sensor irradiance di portal          |
| Samator Sidoarjo        | Aktif (Jan–Mei)                   | —                                    | Tidak ada sensor irradiance di portal          |
| Samator Ame Krian       | Aktif (Jan–Mei)                   | —                                    | Tidak ada sensor irradiance di portal          |
| Samator Solo            | Aktif penuh Mar–Mei               | —                                    | Historis Jan–Feb juga terisi                   |
| Samator Semarang        | Aktif penuh Mar–Mei               | —                                    | Historis Des–Feb juga terisi                   |
| Samator Bali            | Aktif Jan–8 Apr; gap 9 Apr–15 Mei | —                                    | Outage hardware; re-ingest selesai             |


**Ringkasan:** **5 dari 8 site** menampilkan data energi penuh dan kontinu. **3 site** (Solo, Semarang, Bali pada periode tertentu) memiliki keterbatasan yang berasal dari kondisi meter/inverter lapangan, bukan dari sistem pelaporan.

---

## 5. Deliverables yang Diserahkan

### 5.1 Perbaikan teknis


| No  | Item                                                              | Tanggal |
| --- | ----------------------------------------------------------------- | ------- |
| 1   | Perbaikan pipeline transformasi harian                            | 16 Mei  |
| 2   | Backfill data 13–16 Mei 2026                                      | 16 Mei  |
| 3   | Konfigurasi meter revenue 8 site                                  | 16 Mei  |
| 4   | Konfigurasi kapasitas & inverter                                  | 16 Mei  |
| 5   | Backfill historis Des 2025 – Mei 2026                             | 16 Mei  |
| 6   | Koreksi sensor Weiss Tech (POA)                                   | 17 Mei  |
| 7   | Koreksi meter revenue & sensor Gelora Djaja                       | 17 Mei  |
| 8   | Konfigurasi meter swap Solo / Semarang / Bali                     | 17 Mei  |
| 9   | Investigasi & dokumentasi Samator Bali                            | 17 Mei  |
| 10  | Backfill re-ingest 9, 14–16 Mei 2026                              | 17 Mei  |
| 11  | Remediasi **insiden** Hidden Valley (cron + subset dbt/seed + backfill) | 18 Mei  |
| 12  | Penyesuaian calculation start date Gelora + cleanup data historis | 18 Mei  |
| 13  | Remediasi anomali energi SPF (`REVENUE_PERIOD` + reingest)        | 18 Mei  |
| 14  | **Dokumentasi tech debt repo** (§3.9) — inventarisasi & rekomendasi PR | 18 Mei  |


### 5.2 Dokumen


| Dokumen                                   | Isi                                                                |
| ----------------------------------------- | ------------------------------------------------------------------ |
| Laporan Masalah Sistem MMSR               | Audit lengkap: pipeline, 8 site, konfigurasi device, kamus data    |
| Clarification Meeting Brief               | Ringkasan rapat klarifikasi & agenda keputusan bisnis              |
| Implementation Plan                       | Rencana & catatan eksekusi task                                    |
| Laporan Ketersediaan Energi 8 Site        | Timeline meter vs laporan per site                                 |
| Laporan Koreksi Weiss & Gelora            | Koreksi sensor & meter revenue final                               |
| Investigasi Samator Bali                  | Kronologi counter stuck & re-ingest                                |
| **Hidden Valley — Tech Debt & Pipeline**  | Insiden HV + **indikasi tech debt repo** (~1/3 artefak); gap meter AM 15 Mei |
| **Gelora — Calculation Start Date**       | Penyesuaian tanggal operasional + cleanup baris historis stale     |
| **SPF — Energy Anomaly (REVENUE_PERIOD)** | Root cause energi spike ~700 MWh + perbaikan konfigurasi revenue   |
| **Laporan ini**                           | Ringkasan konsolidasi untuk pembaca non-teknis                     |


---

## 6. Item yang Ditunda & temuan fatal di luar scope mandays


| Item                                | Alasan                                                         | Rekomendasi                                           | Prioritas |
| ----------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------- | --------- |
| **Tech debt repository (§3.9)**     | ~**1/3** artefak hilang dari git; butuh PR besar & inventarisasi penuh | Commit `app/`, `cron/`, seluruh model dbt & seed; DB vs `dbt ls`; branch protection | **P0 — Fatal** |
| Sumber GHI untuk 7 site Samator     | Tidak ada sensor irradiance di portal — perlu keputusan bisnis | Pasang sensor fisik, atau gunakan referensi site lain | P2        |
| Kapasitas POA Weiss & Gelora        | Nilai kWp per orientasi belum tersedia dari klien              | Konfirmasi nilai, input ke konfigurasi                | P2        |
| ~~Re-ingest / backfill 9 Mei 2026~~ | —                                                              | ✅ Selesai 17 Mei (ingest + dbt PASS 28/28)            | —         |
| ~~Backfill staging FS 26 Apr 2026~~ | Raw lengkap; staging partial                                   | ✅ Selesai 17 Mei malam (dbt-only)                     | —         |
| Alert volume ingest harian          | Peningkatan monitoring — tidak memblokir operasi               | Masukkan ke backlog engineering                       | P4        |


---

## 7. Rekomendasi Operasional

### 7.1 Untuk tim O&M lapangan


| Site                        | Tindakan                                                                     |
| --------------------------- | ---------------------------------------------------------------------------- |
| **Samator Bali**            | Dokumentasikan outage 9 Apr–15 Mei; verifikasi meter & inverter pasca 16 Mei |
| **Samator Solo & Semarang** | Pantau counter meter di portal iSolarCloud                                   |
| **Samator Malang**          | Pantau meter setelah 12 Mei (meter berhenti naik sejak tanggal tersebut)     |


### 7.2 Untuk menjaga stabilitas sistem

1. **Setiap malam (01:00 WIB):** pastikan log pipeline menampilkan status sukses (28 model berhasil, 0 error)
2. **Hidden Valley — setiap jam (kecuali jam 1–4 WIB):** pastikan task `**MMSR HV Hourly Pilot`** sukses dan file `cron\run_hv_pilot_cron.bat` **ada di repo/server** — tanpa ini mart meter/villa/EMMA/battery **beku** meskipun ingest harian jalan
3. **Setelah ubah konfigurasi device:** ikuti urutan — update seed → refresh dimensi asset → refresh laporan mart
4. **Setiap pergantian sensor/meter:** isi catatan (`notes`) di file mapping agar mudah diaudit di masa depan
5. **Tech debt repo (§3.9) — prioritas tertinggi pasca-mandays:** dashboard `app/`, pipeline HV, model dbt major/minor, seed, dan fitur tambahan **wajib** masuk git sebelum dianggap production
6. **Hindari** rebuild data staging FusionSolar kecuali memang dijadwalkan re-ingest — dapat memperlambat proses dan mengunci database

### 7.3 Keputusan bisnis yang masih terbuka (dari Clarification Meeting)


| Topik                        | Pertanyaan untuk klien                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------------------- |
| **a_KPI**                    | Apakah definisi "target harian × (KPI bulan ÷ simulasi bulan)" sesuai pemahaman Finance/Ops? |
| **Shoetown & CP Majalengka** | Setuju aturan GHI dari WST Shoetown sejak Mei 2026?                                          |
| **8 site Samator**           | Apakah irradiance wajib di dashboard, atau cukup energi saja?                                |
| **Kamus metrik**             | Apakah perlu dokumen resmi definisi KPI sebelum rollout ke user luas?                        |


---

## 8. Penutup & Pernyataan Penyelesaian

Kreasi Koleksi Kreatif dengan ini menyatakan bahwa seluruh item dalam **scope 2 mandays (16–17 Mei 2026)** telah diselesaikan sesuai kesepakatan **Clarification Meeting 16 Mei 2026**.

- Sistem pipeline data MMSR beroperasi normal
- Delapan site audit memiliki konfigurasi yang benar di database
- Data historis telah diisi ulang sejak tanggal operasional masing-masing site
- Item yang ditunda telah didokumentasikan beserta rekomendasi tindak lanjut

Item *deferred* bisnis (GHI Samator, POA) **tidak memblokir operasional harian**. Sebaliknya, **tech debt repository (§3.9)** adalah temuan **fatal** yang dapat mematikan pipeline atau fitur kapan saja — seperti yang sudah terjadi pada Hidden Valley.

**Pembaruan 18 Mei 2026:**

- **Insiden operasional ditutup:** HV (§3.5–3.6), Gelora calc start (§3.7), SPF (§3.8).
- **Temuan struktural belum ditutup:** **Technical debt repository** — estimasi **~1/3** artefak tidak ada di git; pemulihan membutuhkan **PR besar** (dashboard `app/` sebagai blok terbesar, lalu `cron/`, model dbt major/minor, seed, dan komponen lain yang menghalangi pipeline atau fitur tambahan).
- **Yang sudah dilakukan dalam mandays:** diagnosa, remediasi subset HV, dokumentasi, dan rekomendasi inventarisasi — **bukan** commit penuh seluruh basis kode.

---


|                               |                                     |
| ----------------------------- | ----------------------------------- |
| **Kreasi Koleksi Kreatif**    | **Solar Radiance — Departemen O&M** |
| Penyedia Jasa IT Consulting   | Klien / Penerima Layanan            |
|                               |                                     |
| Tanggal: 17 Mei 2026          | Tanggal: _______________            |
|                               |                                     |
| Tanda tangan: _______________ | Tanda tangan: _______________       |


---

*Dokumen ini dapat diedit langsung. Untuk detail teknis, lihat lampiran di folder `docs/reports/`. Revisi 18 Mei 2026: §3.9 tech debt repo (fatal, PR besar), §3.5–3.6 HV, §3.7 Gelora, §3.8 SPF.*