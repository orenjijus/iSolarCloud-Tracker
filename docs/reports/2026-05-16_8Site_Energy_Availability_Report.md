# Laporan Ketersediaan Energi — 8 Site Audit (16 Mei 2026)

**Tujuan:** Memverifikasi apakah energi (`daily_energy_mwh`) di `mart_site_performance_daily` tersedia **sejak site beroperasi** (sesuai data meter di raw/staging), bukan hanya sejak perbaikan seed 16 Mei 2026.

**Sumber data:** `staging.stg_isolarcloud__`*, `mart.mart_meter_performance_5min`, `mart.mart_site_performance_daily`, `mart.fact_inverter_calculations_5min`, `staging.seed_*` (post-revisi).

**Tanggal snapshot DB:** data terakhir di mart umumnya **14 Mei 2026**.

---

## 1. Ringkasan eksekutif


| Kategori                    | Site                                       | Masalah utama                                                                                                       |
| --------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| ✅ OK (meter + mart Apr–Mei) | Weiss, Gelora, Malang, Sidoarjo, Ame Krian | Mart energi aktif setelah seed; ada gap historis sebelum backfill Mar–Apr                                           |
| ⚠️ Mart belum pernah terisi | Solo, Semarang, Bali                       | Meter pernah naik (Jan–Feb 2026) tapi **mart selalu 0** (seed Revenue baru 16 Mei); meter **stuck sejak akhir Feb** |
| ⚠️ Gap historis mart        | Semua Samator + Weiss                      | Banyak hari meter OK **tidak** terhitung di mart karena `seed_meter_config` kosong sampai 16 Mei                    |
| ⚠️ Inverter putus           | Bali                                       | Inverter fact berhenti **~8 Apr 2026** (meter masih ada baris)                                                      |


**Kesimpulan utama:** Raw/staging **tidak kosong dari awal** untuk ketiga site bermasalah (Solo/Semarang/Bali). Yang hilang adalah (1) **agregasi mart** untuk periode ketika meter masih hidup, dan (2) **pembacaan meter** yang berhenti naik sejak **akhir Februari 2026**.

---

## 2. Konfigurasi site (setelah revisi seed)


| site_id     | Site                          | System      | Capacity (kWp) | Calc start (seed) | Revenue meter                 |
| ----------- | ----------------------------- | ----------- | -------------- | ----------------- | ----------------------------- |
| 1789614     | PLTS Rooftop Weiss Tech       | iSolarCloud | 238.08         | 2025-01-01        | `1789614_7_7_1` (WT-RM-01)    |
| NE=61847068 | PT Gelora Djaja 1 MWp         | FusionSolar | 1000.4         | 2025-12-01        | `AM0610254C346758` (EM-POI) |
| 1722752     | Samator Ame Krian 6,82 kWp    | iSolarCloud | 6.82           | 2025-01-01        | `1722752_7_2_1`               |
| 1731512     | Samator Bali                  | iSolarCloud | 9.92           | 2025-01-01        | `1731512_7_1_1`               |
| 1734912     | Samator Gas Industri Malang   | iSolarCloud | 6.82           | 2025-01-01        | `1734912_7_2_1`               |
| 1728142     | Samator Gas Industri Sidoarjo | iSolarCloud | 6.82           | 2025-01-01        | `1728142_7_2_1`               |
| 1730375     | Samator Gas Industri Solo     | iSolarCloud | 6.82           | 2025-01-01        | `1730375_7_2_1`               |
| 1723575     | Samator Indo Gas Semarang     | iSolarCloud | 9.92           | 2025-01-01        | `1723575_7_2_1`               |


**Sensor (koreksi 17 Mei):** Weiss Meteo 1 = **POA** (`1789614_5_8_1`); Meteo 2 tidak dipakai; Gelora EM001=GHI, EM011/021/031=POA. Lihat [2026-05-17_Config_Correction_Weiss_Gelora.md](./2026-05-17_Config_Correction_Weiss_Gelora.md).

---

## 3. Timeline per site

Legenda:

- **Ingest meter:** rentang baris `positive_active_energy` di mart meter 5-min.
- **Meter hidup:** hari dengan delta kumulatif meter > 100 Wh.
- **Mart energi > 0:** hari `daily_energy_mwh > 0` di site mart.
- **Inverter fact:** rentang `fact_inverter_calculations_5min`.


| Site          | Ingest meter          | Meter hidup (pertama → terakhir) | Hari meter hidup | Mart energi > 0 (pertama → terakhir) | Hari mart energi | Inverter fact             |
| ------------- | --------------------- | -------------------------------- | ---------------- | ------------------------------------ | ---------------- | ------------------------- |
| **Weiss**     | 4 Mar → 14 Mei 26     | 4 Mar → 14 Mei 26                | 72               | **31 Mar** → 14 Mei 26               | **45**           | 4 Mar → 14 Mei (70 h)     |
| **Gelora**    | 27 Agu 25 → 14 Mei 26 | 29 Agu 25 → 14 Mei 26            | 54*              | **6 Apr** → 14 Mei 26                | **32**           | 28 Agu 25 → 13 Mei (53 h) |
| **Ame Krian** | 16 Des 25 → 14 Mei 26 | 26 Jan → 14 Mei 26               | 107              | **31 Mar** → 14 Mei 26               | **45**           | 26 Jan → 14 Mei (106 h)   |
| **Malang**    | 16 Des 25 → 14 Mei 26 | 25 Jan → **12 Mei** 26           | 107              | **31 Mar** → 12 Mei 26               | **42**           | 25 Jan → 12 Mei (105 h)   |
| **Sidoarjo**  | 16 Des 25 → 14 Mei 26 | 26 Jan → 14 Mei 26               | 109              | **31 Mar** → 14 Mei 26               | **45**           | 26 Jan → 14 Mei (106 h)   |
| **Solo**      | 16 Des 25 → 14 Mei 26 | 25 Jan → **28 Feb** 26           | **35**           | **tidak pernah**                     | **0**            | 25 Jan → 14 Mei (107 h)†  |
| **Semarang**  | 16 Des 25 → 14 Mei 26 | 16 Des 25 → **26 Feb** 26        | **58**           | **tidak pernah**                     | **0**            | 16 Des → 14 Mei (109 h)†  |
| **Bali**      | 16 Des 25 → 14 Mei 26 | 27 Jan → **26 Feb** 26           | **31**           | **tidak pernah**                     | **0**            | 27 Jan → **8 Apr** (65 h) |


Gelora: delta meter jarang/sporadis di 2025; padat sejak Feb 2026.  
†Inverter masih ada baris setelah meter stuck — produksi inverter tidak otomatis masuk site mart energi (energi site = meter revenue).

### Tanggal meter “macet” (counter tidak naik lagi)


| Site     | Terakhir hari delta > 0                             | Nilai kumulatif terakhir (Wh) |
| -------- | --------------------------------------------------- | ----------------------------- |
| Solo     | **28 Feb 2026** (delta kecil 29 kWh); **1 Mar** = 0 | 3.870.145                     |
| Semarang | **26 Feb 2026**                                     | 11.072.735                    |
| Bali     | **26 Feb 2026**                                     | 5.891.660                     |


Setelah tanggal ini, ingest masih jalan tetapi **delta harian = 0** → `daily_energy_mwh` = 0.

---

## 4. Analisis: “energi harus ada dari awal nyala”

### 4.1 Apakah raw kosong dari awal?

**Tidak**, untuk semua 8 site ada data di pipeline:

- **Samator (7):** ingest meter dari **16 Des 2025** (~160k–180k baris 5-min per site).
- **Weiss:** ingest meter dari **4 Mar 2026**.
- **Gelora:** ingest meter dari **Agu 2025** (terbatas), padat **Feb 2026+**.

### 4.2 Dua lapisan masalah

```text
[Portal meter] → [Staging/raw] → [Mart meter 5-min] → [Site mart daily_energy_mwh]
                      ✅              ✅                    ⚠️ / ❌
```


| Lapisan                     | Solo / Semarang / Bali            | Site lain (Samator 4 + Weiss + Gelora) |
| --------------------------- | --------------------------------- | -------------------------------------- |
| Raw ada?                    | ✅                                 | ✅                                      |
| Meter pernah naik?          | ✅ Jan–Feb 2026                    | ✅ (Weiss dari Mar 2026)                |
| Meter masih naik (Mar–Mei)? | ❌ stuck                           | ✅ kecuali Malang stop 12 Mei           |
| Mart energi pernah > 0?     | ❌ **tidak pernah**                | ✅ dari ~31 Mar / 6 Apr 2026            |
| Penyebab mart 0 historis    | Seed Revenue kosong sampai 16 Mei | Seed + backfill baru Apr 2026          |


### 4.3 Gap: hari meter hidup vs hari mart energi

Periode ketika **meter OK di raw** tetapi **mart = 0** (perlu **backfill mart** setelah seed fix):


| Site      | Perkiraan hari meter OK tanpa mart | Rentang                                  |
| --------- | ---------------------------------- | ---------------------------------------- |
| Solo      | ~35                                | 25 Jan – 28 Feb 2026                     |
| Semarang  | ~58                                | 16 Des 2025 – 26 Feb 2026                |
| Bali      | ~31                                | 27 Jan – 26 Feb 2026                     |
| Ame Krian | ~64                                | 26 Jan – 30 Mar 2026                     |
| Malang    | ~65                                | 25 Jan – 30 Mar 2026                     |
| Sidoarjo  | ~64                                | 26 Jan – 30 Mar 2026                     |
| Weiss     | ~27                                | 4 Mar – 30 Mar 2026                      |
| Gelora    | ~48+                               | Feb 2026 – 5 Apr 2026 (tergantung meter) |


Contoh **Solo** — meter vs mart (Feb 2026):


| Tanggal | Delta meter (kWh) | `daily_energy_mwh` mart |
| ------- | ----------------- | ----------------------- |
| 26 Feb  | 170.7             | 0                       |
| 28 Feb  | 28.9              | 0                       |
| 1 Mar   | **0** (stuck)     | 0                       |


→ Data produksi **ada di meter**, belum pernah di-roll-up ke site mart.

---

## 5. Status per site (checklist)


| Site         | Seed OK | Meter revenue aktif | Meter hidup sekarang | Mart energi      | GHI/POA          | Rekomendasi                                                     |
| ------------ | ------- | ------------------- | -------------------- | ---------------- | ---------------- | --------------------------------------------------------------- |
| Weiss        | ✅       | ✅ `7_7_1`           | ✅                    | ✅ 45 h (Mar–Mei) | GHI ✅            | Backfill Mar–Mei penuh; historis sebelum 4 Mar tidak ada ingest |
| Gelora       | ✅       | ✅ `AM011`           | ✅                    | ✅ 32 h           | POA legacy 37 h‡ | Backfill Des 2025–Mei; bersihkan POA lama; konfirmasi EM-POI    |
| Ame Krian    | ✅       | ✅                   | ✅                    | ✅ 45 h           | —                | Backfill Jan–Mar 2026                                           |
| Malang       | ✅       | ✅                   | ⚠️ stop 12 Mei       | ✅ 42 h           | —                | Cek meter pasca 12 Mei; backfill Jan–Mar                        |
| Sidoarjo     | ✅       | ✅                   | ✅                    | ✅ 45 h           | —                | Backfill Jan–Mar 2026                                           |
| **Solo**     | ✅       | ✅ (tag)             | ❌ stuck 1 Mar        | ❌                | —                | **Portal/meter lapangan** + backfill Jan–Feb                    |
| **Semarang** | ✅       | ✅ (tag)             | ❌ stuck 27 Feb       | ❌                | —                | **Portal/meter lapangan** + backfill Des–Feb                    |
| **Bali**     | ✅       | ✅ (tag)             | ❌ stuck 27 Feb       | ❌                | —                | **Meter + re-ingest inverter** + backfill Jan–Feb               |


‡POA dari baris lama `mart_sensor_daily`; seed sensor Gelora sudah dikosongkan.

---

## 6. Irradiance


| Site       | Meteo di portal     | Seed sensor | Hari GHI di mart | Hari POA di mart      |
| ---------- | ------------------- | ----------- | ---------------- | --------------------- |
| Weiss      | Meteo 1 = POA; Meteo 2 tidak dipakai | 1× POA | 0 GHI / POA weighted† | 0 |
| Gelora     | EM001 GHI + 3× POA  | EM001=GHI   | ~38              | 0 (kapasitas POA TBD) |
| Samator ×7 | tidak ada           | kosong      | 0                | 0                     |


---

## 7. Tindakan yang disarankan

### Prioritas 1 — Backfill energi historis (seed sudah benar)

Jalankan ulang `mart_site_performance_daily` (dan dependency meter) dari **tanggal pertama meter hidup** per site, bukan hanya Apr 2026:

```bash
cd dbt
# Contoh per site — sesuaikan tanggal first_meter_ok dari tabel §3
dbt run --select mart_meter_performance_5min mart_site_performance_daily \
  --indirect-selection empty \
  --vars '{"reingest_start_date": "2025-12-16", "reingest_end_date": "2026-05-16"}'
```

Site-spesifik `reingest_start_date` yang disarankan:


| Site                                    | `reingest_start_date`                                 |
| --------------------------------------- | ----------------------------------------------------- |
| Semarang                                | 2025-12-16                                            |
| Solo, Bali, Malang, Sidoarjo, Ame Krian | 2026-01-25                                            |
| Weiss                                   | 2026-03-04                                            |
| Gelora                                  | 2025-12-01 (calc start) atau 2026-02-22 (meter padat) |


Setelah backfill, verifikasi Solo/Semarang/Bali **Jan–Feb 2026** harus punya `daily_energy_mwh > 0` jika meter delta > 0.

### Prioritas 2 — Perbaikan lapangan (meter stuck)


| Site                 | Aksi                                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Solo, Semarang, Bali | Cek di iSolarCloud apakah counter `positive_active_energy` masih naik; perbaiki komunikasi meter / ganti meter jika perlu |
| Bali                 | Re-ingest inverter setelah 2026-04-08                                                                                     |
| Malang               | Investigasi meter setelah 2026-05-12                                                                                      |


### Prioritas 3 — Data quality

- Hapus/rebuild POA Gelora di `mart_sensor_daily` untuk rentang historis.
- Dokumentasikan keputusan final meter revenue Gelora (EM-MVSWG vs EM-POI).

---

## 8. SQL verifikasi

```sql
-- Timeline meter vs mart (8 site)
WITH daily AS (
  SELECT site_name, date_key::date AS dt,
         MAX(metric_value) - MIN(metric_value) AS delta_wh
  FROM mart.mart_meter_performance_5min
  WHERE metric_name = 'positive_active_energy'
    AND site_name IN (
      'PLTS Rooftop Weiss Tech','PT Gelora Djaja 1 MWp',
      'Samator Ame Krian 6,82 kWp','Samator Bali',
      'Samator Gas Industri Malang 6,82 kWp',
      'Samator Gas Industri Sidoarjo 6,82 kWP',
      'Samator Gas Industri Solo 6,82 kWp',
      'Samator Indo Gas Semarang 9,92 kWp'
    )
  GROUP BY 1, 2
)
SELECT d.site_name,
       MIN(d.dt) FILTER (WHERE d.delta_wh > 100) AS first_meter_ok,
       MAX(d.dt) FILTER (WHERE d.delta_wh > 100) AS last_meter_ok,
       COUNT(*) FILTER (WHERE d.delta_wh > 100) AS days_meter_ok,
       MIN(sp.date_key) FILTER (WHERE sp.daily_energy_mwh > 0) AS first_mart_energy,
       COUNT(*) FILTER (WHERE sp.daily_energy_mwh > 0) AS days_mart_energy
FROM daily d
LEFT JOIN mart.mart_site_performance_daily sp
  ON sp.site_name = d.site_name AND sp.date_key = d.dt
GROUP BY d.site_name
ORDER BY d.site_name;
```

---

## 9. Referensi

- [2026-05-16_Implementation_Plan.md](./2026-05-16_Implementation_Plan.md) — revisi seed & pelaksanaan dbt
- [2026-05-16_Laporan_Masalah_Sistem_MMSR.md](./2026-05-16_Laporan_Masalah_Sistem_MMSR.md) — konteks audit awal

---

## 10. Hasil backfill historis (16 Mei 2026)

**Perintah:** `dbt run` — `mart_meter_performance_5min`, `mart_sensor_daily`, `mart_site_performance_daily`  
**Rentang:** `2025-12-16` → `2026-05-16` (`--indirect-selection empty`)  
**Durasi:** ~1 jam 30 menit — **PASS 3/3**

### `daily_energy_mwh > 0` setelah backfill


| Site         | Hari energi | Pertama         | Terakhir        | Catatan                                      |
| ------------ | ----------- | --------------- | --------------- | -------------------------------------------- |
| Weiss        | **72**      | 4 Mar 2026      | 14 Mei 2026     | Selaras periode meter hidup                  |
| Gelora       | **48**      | 22 Feb 2026     | 14 Mei 2026     | Selaras meter padat                          |
| Ame Krian    | **107**     | 26 Jan 2026     | 14 Mei 2026     | Penuh sejak meter hidup                      |
| Malang       | **107**     | 25 Jan 2026     | 12 Mei 2026     | Stop selaras meter 12 Mei                    |
| Sidoarjo     | **109**     | 26 Jan 2026     | 14 Mei 2026     | Penuh                                        |
| **Solo**     | **35**      | 25 Jan 2026     | **28 Feb 2026** | ✅ Historis terisi; Mar–Mei = 0 (meter stuck) |
| **Semarang** | **58**      | **16 Des 2025** | **26 Feb 2026** | ✅ Historis terisi; Mar–Mei = 0 (meter stuck) |
| **Bali**     | **31**      | 27 Jan 2026     | **26 Feb 2026** | ✅ Historis terisi; Mar–Mei = 0 (meter stuck) |


**Kesimpulan backfill:** Energi site mart sekarang ada **sejak meter mulai naik** untuk semua 8 site. Site dengan meter macet (Solo/Semarang/Bali) tetap **0 energi setelah ~1 Mar / 27 Feb** pada meter **lama** — itu benar secara data meter lama.

---

## 11. Pergantian meter Samator (sync devices 16 Mei 2026)

**Temuan sync iSolarCloud:** muncul **Meter2** (device baru) di ketiga site — cocok dengan portal.

| Site | Meter lama (Revenue periode 1) | Meter baru Meter2 (Revenue periode 2) | Grid connection meter baru |
| ---- | ------------------------------ | ------------------------------------- | -------------------------- |
| Solo `1730375` | `1730375_7_2_1` MeterSamatorSolo | `1730375_7_4_1` Meter2 | 2026-03-01 |
| Semarang `1723575` | `1723575_7_2_1` MeterSamatorSemarang | `1723575_7_4_1` Meter2 | 2026-03-02 |
| Bali `1731512` | `1731512_7_1_1` MeterSamatorBali | `1731512_7_3_1` Meter2 | 2026-03-07 |

**Model konfigurasi (bukan ganti meter di seed):**

| Lapisan | File | Isi |
| ------- | ---- | --- |
| Baseline | `seed_meter_config` | **Keduanya** `meter_type = Revenue` |
| Override temporal | `seed_meter_site_mapping` | `mapping_type = REVENUE_PERIOD` + `effective_date_start/end` + `logical_device_id` = `site_id` |
| Mart | `mart_site_performance_daily` | Hanya hitung revenue meter yang **aktif** pada `date_key` |

Contoh baris `seed_meter_site_mapping`:

```text
REVENUE_PERIOD;1730375_7_2_1;1730375;;2025-12-16;2026-02-28;Solo meter lama
REVENUE_PERIOD;1730375_7_4_1;1730375;;2026-03-01;;Solo Meter2
```

Site lain **tanpa** baris `REVENUE_PERIOD` → perilaku lama (semua meter Revenue di seed dipakai).

**Kasus SPF (18 Mei 2026):** PLTS Rooftop Sumatera Prima Fibreboard (`1680199`) — energi spike ~700 MWh karena meter lama + baru sama-sama `Revenue`; diperbaiki dengan `REVENUE_PERIOD`. Lihat [2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md](./2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md).

**Langkah berikutnya:** ingest perf untuk meter baru (`--fetch-historical` dari 2026-03-01) → `dbt seed` → backfill `mart_site_performance_daily`.

---

**Dibuat:** 16 Mei 2026  
**Diperbarui:** 16 Mei 2026 (post-backfill + meter swap)  
**Oleh:** pipeline audit / crosscheck MMSR