# Implementation Plan — Audit Findings (16 Mei 2026)

**Ringkasan:** Revisi seed 8 site selesai; mart Apr–Mei 2026 di-backfill. **17 Mei (malam):** backfill dbt-only **9 Mei**, **13–16 Mei**, dan **26 Apr 2026** — semua **PASS 28/28**. Energi mart aktif untuk 5/8 site audit (Weiss, Gelora, Malang, Sidoarjo, Ame Krian). **Solo / Semarang / Bali:** raw tidak kosong — meter pernah hidup Jan–Feb 2026 lalu stuck; mart energi belum pernah terisi penuh → tindak lanjut lapangan.

**Koreksi terbaru (Weiss & Gelora):** [2026-05-17_Config_Correction_Weiss_Gelora.md](./2026-05-17_Config_Correction_Weiss_Gelora.md) — Weiss Meteo 1 = **POA** (bukan GHI); Meteo 2 **tidak dipakai**; Gelora revenue = **EM-POI**, sensor EM001=GHI / EM011–031=POA (kapasitas POA ditunda).

Detail 8 site: [2026-05-16_8Site_Energy_Availability_Report.md](./2026-05-16_8Site_Energy_Availability_Report.md).

## Status


| #   | Task                                                      | Priority | Status                                                                        |
| --- | --------------------------------------------------------- | -------- | ----------------------------------------------------------------------------- |
| 1   | Fix `issue_date_remarks` in `mart_site_performance_daily` | P0       | Done                                                                          |
| 2   | Backfill mart 13–16 Mei 2026 (+ Apr refresh)              | P0       | Done (dbt-only 17 Mei, lihat bawah)                                           |
| 2c  | Backfill dbt **9 Mei** + **26 Apr 2026** (FS staging)     | P0       | Done (17 Mei malam, tanpa API re-ingest 26 Apr)                               |
| 2b  | Backfill historis energi 8 site (2025-12-16 → 2026-05-16) | P1       | Done (~1,5 jam, PASS 3/3)                                                     |
| 3   | `seed_meter_config` Revenue — 8 site                      | P1       | Done (revised + mart re-run)                                                  |
| 4   | `seed_site_config` capacity + inverters                   | P1       | Done (revised + `dim_assets` OK)                                              |
| 5   | `seed_sensor_config` Weiss POA + Gelora GHI/POA           | P1       | Done — lihat [koreksi 17 Mei](./2026-05-17_Config_Correction_Weiss_Gelora.md) |
| 6   | Fix Weiss `seed_inverter_config.site_name`                | P2       | Done                                                                          |
| 7   | Majalengka `daily_ghi` COALESCE with `ghi_adjusted`       | P1       | Done                                                                          |
| 8   | Samator irradiance (no meteo hardware)                    | P2       | Deferred — needs business decision                                            |
| 9   | Re-ingest / backfill **9 Mei 2026**                       | P2       | Done — ingest FS 17 Mei siang + dbt-only 21:44 PASS 28/28                     |
| 10  | Pipeline row-count alerts                                 | P4       | Deferred                                                                      |


## Revisi seed config (koreksi crosscheck — ulangi seed + mart)

Setelah crosscheck manual, nilai di `dbt/seeds/*.csv` diperbaiki. **Run sebelumnya (task 3–5) tidak valid** — perlu `dbt seed` + backfill mart ulang.

### 1. `seed_site_config.csv` — capacity (kWp)


| Slug seed                 | Site                    | Sebelum | **Sesudah** |
| ------------------------- | ----------------------- | ------- | ----------- |
| `plts_rooftop_weiss_tech` | PLTS Rooftop Weiss Tech | 51.42   | **238.08**  |
| `samator_bali`            | Samator Bali            | 6.82    | **9.92**    |
| `pt_gelora_djaja_1_mwp`   | PT Gelora Djaja 1 MWp   | 1000    | **1000.4**  |


Samator lainnya (6.82 / 9.92 kWp) tidak berubah.

### 2. `seed_meter_config.csv` — revenue meter


| Site         | Sebelum (salah)               | **Sesudah (benar)**                                                                | `asset_id` join                                                                      |
| ------------ | ----------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Weiss Tech   | `1789614_7_6_1` (Meter1)      | `**1789614_7_7_1`** (WT-RM-01)                                                     | `ISO_1789614_7_7_1`                                                                  |
| Gelora Djaja | `AM0110254C346758` (EM-MVSWG) | **`AM0610254C346758` (EM-POI)** saja — uji dual Revenue dibatalkan | `FS_AM0610254C346758` — [detail](./2026-05-17_Config_Correction_Weiss_Gelora.md) |
| Samator ×7   | —                             | unchanged                                                                          | `1734912_7_2_1`, `1731512_7_1_1`, dll.                                               |


- Gelora: revenue = **EM-POI (`AM0610254C346758`)**; EM-MVSWG bukan revenue.
- Meter Gelora non-revenue (AM001, AM021, AM031) tetap di seed tanpa `meter_type = Revenue`.

### 3. `seed_sensor_config.csv` — irradiance

**→ Diganti oleh koreksi 17 Mei:** [2026-05-17_Config_Correction_Weiss_Gelora.md](./2026-05-17_Config_Correction_Weiss_Gelora.md)


| Site         | Konfigurasi final (17 Mei)                                                                   |
| ------------ | -------------------------------------------------------------------------------------------- |
| Weiss Tech   | Meteo Station 1 `1789614_5_8_1` = **POA**; Meteo Station 2 **tidak di-seed** (device kosong) |
| Gelora Djaja | EM001 = **GHI**; EM011 / EM021 / EM031 = **POA** (kapasitas POA ditunda)                     |
| Samator ×7   | kosong — task #8 deferred                                                                    |


### 4. Inverter — tidak berubah

- Weiss / Gelora: `seed_inverter_config` tetap (Weiss `site_name` sudah benar).
- Samator: tanpa baris inverter seed — cukup dari `dim_assets`.

---

## Execution order (setelah revisi seed)

```bash
cd dbt
dbt seed --select seed_site_config seed_meter_config seed_sensor_config

# PENTING: --indirect-selection empty → TIDAK rebuild staging FusionSolar
# Tanpa flag ini, dbt ikut menjalankan stg_fusionsolar__perf_unpivoted (jam + lock DB)
dbt run --select dim_assets --indirect-selection empty

dbt run --select mart_meter_performance_5min mart_sensor_measurements_5min mart_sensor_daily mart_site_performance_daily \
  --indirect-selection empty \
  --vars '{"reingest_start_date": "2026-04-01", "reingest_end_date": "2026-05-16"}'

# Jika mart_site terputus, ulangi hanya:
dbt run --select mart_site_performance_daily --indirect-selection empty \
  --vars '{"reingest_start_date": "2026-04-01", "reingest_end_date": "2026-05-16"}'
```

## Pelaksanaan (16 Mei 2026)


| Langkah | Perintah                                                       | Hasil                                     | Durasi                 |
| ------- | -------------------------------------------------------------- | ----------------------------------------- | ---------------------- |
| 1       | `dbt seed` (site, meter, sensor)                               | PASS 3/3 — 144 meter, 126 sensor, 25 site | ~1 s                   |
| 2       | `dim_assets` (run pertama, tanpa flag)                         | **Gagal macet** — lock DB (lihat bawah)   | >20 menit (dibatalkan) |
| 3       | Terminate backend PG + `dim_assets --indirect-selection empty` | PASS — 612 baris, capacity ter-update     | **0,3 s**              |
| 4       | Mart reingest Apr–16 Mei (4 model)                             | PASS 4/4                                  | ~50 menit              |
| 5       | `mart_site_performance_daily` (ulang)                          | PASS — INSERT 1159 baris                  | ~2,7 menit             |


### Insiden: `dim_assets` tampak “gagal” / sangat lama

Bukan karena model berat, melainkan **deadlock** saat `dbt run --select dim_assets` tanpa `--indirect-selection empty`:

- `stg_fusionsolar__perf_unpivoted` DELETE berjalan >1 jam (IO)
- `stg_fusionsolar__devices` ALTER menunggu lock ~39 menit
- `dim_assets` CREATE TABLE menunggu lock ~21 menit

**Solusi:** selalu pakai `--indirect-selection empty` untuk `dim_assets` dan mart refresh setelah seed; jangan trigger rebuild staging perf FusionSolar kecuali memang diperlukan.

---

## Penyelesaian backfill manual (17 Mei 2026 — malam)

Setelah fix `issue_date_remarks`, scheduler **17 Mei 01:00** sukses untuk data **16 Mei**. Sisa gap di-backfill berurutan (satu job dbt pada satu waktu, hindari lock DB).

### Ringkasan eksekusi


| Tanggal data    | Mode                     | Mulai (WIB) | Selesai (WIB) | dbt        | Durasi (approx) | Log utama                                                          |
| --------------- | ------------------------ | ----------- | ------------- | ---------- | --------------- | ------------------------------------------------------------------ |
| **13 Mei**      | dbt-only                 | 17:16       | 18:16         | PASS 28/28 | ~59 menit       | `scripts/logs/daily_pipeline_20260517_171626.log`                  |
| **14 Mei**      | dbt-only                 | 18:16       | 19:00         | PASS 28/28 | ~44 menit       | idem                                                               |
| **15 Mei**      | dbt-only                 | 19:00       | 19:40         | PASS 28/28 | ~40 menit       | idem                                                               |
| **16 Mei**      | dbt-only                 | 19:40       | 20:14         | PASS 28/28 | ~35 menit       | idem                                                               |
| **9 Mei**       | dbt-only                 | 21:03       | 21:44         | PASS 28/28 | ~41 menit       | `scripts/logs/daily_pipeline_dbtonly_20260509_20260517_210301.log` |
| **26 Apr 2026** | dbt-only (**tanpa API**) | 22:29       | 23:14         | PASS 28/28 | ~45 menit       | `scripts/logs/daily_pipeline_dbtonly_20260426_20260517_222926.log` |


**Perintah:** `python scripts/run_daily_pipeline.py --dbt-only --date YYYY-MM-DD` (vars `reingest_start_date` / `reingest_end_date` otomatis).

### 9 Mei 2026


| Aspek            | Detail                                                                                                      |
| ---------------- | ----------------------------------------------------------------------------------------------------------- |
| **Masalah awal** | Ingest FS parsial (~39% volume) pada run 10 Mei; dbt 9 Mei terputus saat backfill gabungan 17 Mei siang     |
| **Raw**          | Re-ingest FS + iSolar **17 Mei ~15:45–16:26** (~47.792 titik FS) — log `daily_pipeline_20260517_154524.log` |
| **Mart**         | dbt-only malam — **26 site** di `mart_site_performance_daily` untuk hari tersebut                           |


### 13–16 Mei 2026


| Aspek            | Detail                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------- |
| **Penyebab gap** | dbt gagal 14–16 Mei (kolom `issue_date_remarks`); **ingest malam tetap OK** — raw sudah ada |
| **Tindakan**     | dbt-only berurutan; tidak perlu API re-ingest                                               |
| **Hasil**        | Mart + string performance ter-update; pipeline total 13–16 ~3 jam                           |


### 26 April 2026 (FusionSolar staging partial)


| Aspek                   | Detail                                                                                                   |
| ----------------------- | -------------------------------------------------------------------------------------------------------- |
| **Gejala**              | Hanya **4 dari 9** plant FS di `staging.stg_fusionsolar__perf_unpivoted`; **Pusan** tidak masuk mart     |
| **Raw**                 | **Lengkap** — Pusan 6.750 titik / 30 device (setara 25 Apr)                                              |
| **Perbaikan**           | dbt rebuild full chain (28 model) — **bukan** re-ingest API                                              |
| **Staging FS (26 Apr)** | **9/9 plant**, termasuk Pusan (~219k baris)                                                              |
| **Mart (26 Apr WIB)**   | **26/26** site operasional (3 site di `dim_assets` tidak pernah masuk mart: Load Meter HV, Klinik 1 & 2) |
| **Issue date**          | 5 site di seed → semua `is_issue_date = true` di mart: Pusan, MMKI Ph 2 & 3, Mall Panakkukang, STBC      |


---

## Hasil verifikasi (setelah revisi + re-run)

Rentang mart: **2026-04-01 s.d. 2026-05-16** (+ backfill **9 Mei** & **26 Apr** 17 Mei malam). `mart_site_performance_daily` terisi sampai **16 Mei 2026** untuk run harian; **9 Mei** dan **26 Apr** divalidasi pasca backfill.

### Ringkasan 8 site audit


| Site                          | Capacity mart (kWp) | Hari `daily_energy_mwh > 0` | Hari GHI | Hari POA               | Revenue meter (ter-tag di mart)        |
| ----------------------------- | ------------------- | --------------------------- | -------- | ---------------------- | -------------------------------------- |
| PLTS Rooftop Weiss Tech       | **238.08**          | **45**                      | **45**   | 0                      | `ISO_1789614_7_7_1` ✅                  |
| PT Gelora Djaja 1 MWp         | **1000.4**          | **32**                      | 0        | ⚠️ 37 (sisa data lama) | `FS_AM0110254C346758` ✅                |
| Samator Gas Industri Malang   | 6.82                | **42**                      | 0        | 0                      | `ISO_1734912_7_2_1` ✅                  |
| Samator Gas Industri Sidoarjo | 6.82                | **45**                      | 0        | 0                      | `ISO_1728142_7_2_1` ✅                  |
| Samator Ame Krian             | 6.82                | **45**                      | 0        | 0                      | `ISO_1722752_7_2_1` ✅                  |
| Samator Gas Industri Solo     | 6.82                | **0**                       | 0        | 0                      | seed OK — raw meter datar              |
| Samator Indo Gas Semarang     | 9.92                | **0**                       | 0        | 0                      | seed OK — raw meter datar              |
| Samator Bali                  | **9.92**            | **0**                       | 0        | 0                      | seed OK — raw meter datar / gap ingest |


### Contoh harian (12–14 Mei 2026)


| Site   | Tanggal | Energy (MWh) | GHI  | POA                 |
| ------ | ------- | ------------ | ---- | ------------------- |
| Weiss  | 14 Mei  | 0.783        | 4.09 | —                   |
| Weiss  | 13 Mei  | 0.948        | 5.42 | —                   |
| Gelora | 14 Mei  | 2.514        | —    | —                   |
| Gelora | 13 Mei  | 4.502        | —    | 5.53 *(baris lama)* |


### Energi 13–16 Mei (8 site)


| Site                               | Hari dengan energi > 0 (13–16 Mei) |
| ---------------------------------- | ---------------------------------- |
| Weiss, Gelora, Sidoarjo, Ame Krian | 3/4                                |
| Samator Malang                     | 1/4                                |
| Solo, Semarang, Bali               | 0/4                                |


---

## Kesimpulan

### Berhasil

1. **Seed revisi** (capacity, revenue meter Weiss/Gelora, sensor Weiss 1× GHI, Gelora sensor dikosongkan) sudah di-load ke `staging.`*.
2. `**dim_assets`** mencerminkan capacity baru: Weiss 238.08, Bali 9.92, Gelora 1000.4 kWp.
3. **Revenue meter** di mart: Weiss `7_7_1`, Gelora `AM011` (EM-MVSWG) — join `meter_type = Revenue` aktif.
4. **Energi site mart** untuk Weiss, Gelora, dan **4 dari 7** Samator (Malang, Sidoarjo, Ame Krian + sebagian hari lain) sudah **> 0** di Apr–Mei; bukan lagi 0 karena seed kosong.
5. **Weiss GHI** terisi dari satu sensor `1789614_5_8_1` (45 hari).
6. **P0/P1 audit** (issue_date, backfill, Majalengka GHI) tetap Done dari run sebelumnya.
7. **Backfill 17 Mei malam:** **9 Mei**, **13–16 Mei**, **26 Apr 2026** — semua dbt **PASS 28/28**; 26 Apr tanpa API re-ingest (staging FS 9/9 plant, mart 26 site).

### Belum selesai / tindak lanjut


| Isu                                         | Penyebab                                                                                            | Tindakan                                                                                                                                                                 |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Gelora POA** masih ada ~37 hari           | Baris lama di `mart_sensor_daily` (incremental) dari seed POA sebelumnya; seed baru sudah kosong    | Hapus/rebuild `mart_sensor_daily` untuk Gelora di rentang Apr–Mei, atau `full-refresh` model tersebut                                                                    |
| **Samator Solo, Semarang, Bali** energi = 0 | Raw ada; meter OK Jan–Feb lalu **stuck akhir Feb**; mart **belum pernah** terisi (seed baru 16 Mei) | Backfill mart Jan–Feb + perbaikan meter di portal; Bali re-ingest inverter pasca 8 Apr — lihat [laporan energi 8 site](./2026-05-16_8Site_Energy_Availability_Report.md) |
| ~~Gap historis mart~~                       | —                                                                                                   | ✅ Selesai 16 Mei (rentang 2025-12-16 → 2026-05-16)                                                                                                                       |
| **Samator meter swap** (Solo/Semarang/Bali) | Meter2 baru di API; keduanya Revenue via `REVENUE_PERIOD`                                           | ✅ Seed + logika mart; ⬜ fetch perf meter baru + re-backfill Maret–Mei                                                                                                    |
| **SPF energi spike** (~700 MWh, Apr–Mei 2026) | Meter lama + baru sama-sama `Revenue`; counter lama macet + duplikat di mart 5-min | ✅ `REVENUE_PERIOD` + reingest Apr–Mei 2026 — lihat [laporan SPF](./2026-05-18_SPF_Energy_Anomaly_REVENUE_PERIOD.md) |
| **Samator GHI/POA**                         | Tidak ada meteo hardware                                                                            | Task #8 deferred — fallback GHI atau keputusan bisnis                                                                                                                    |
| **Gelora revenue**                          | Hanya **EM-POI**; uji POI+MVSWG → energi kebesaran (dibatalkan)                                     | Tindak lanjut: ingest/interval **EM-POI** pasca ~30 Apr (Mei 0 = data POI hilang di mart, bukan seed)                                                                   |
| ~~Gap mart 9 / 13–16 / 26 Apr Mei~~         | —                                                                                                   | ✅ Selesai 17 Mei malam (dbt-only, PASS 28/28)                                                                                                                            |
| **Mart tanggal**                            | Sampai **16 Mei** (+ 9 Mei, 26 Apr verified)                                                        | Lanjutkan ingest + dbt harian scheduler 01:00                                                                                                                            |


### Rekomendasi operasional

- Setelah ubah seed: `**dbt seed` → `dim_assets --indirect-selection empty` → mart dengan flag yang sama** — jangan `dbt run --select dim_assets` polos.
- Pipeline nightly: cukup **mart-only refresh** setelah ingest; hindari full rebuild `stg_fusionsolar__perf_unpivoted` kecuali re-ingest FS memang dijadwalkan.

---

## Verification SQL

```sql
-- Capacity + energi per site (8 site audit)
SELECT site_name,
       MAX(actual_capacity_kw) AS capacity_kw,
       COUNT(*) FILTER (WHERE daily_energy_mwh > 0) AS days_energy,
       COUNT(*) FILTER (WHERE daily_ghi_kwh_m2 IS NOT NULL) AS days_ghi,
       COUNT(*) FILTER (WHERE daily_poa_weighted_kwh_m2 IS NOT NULL) AS days_poa
FROM mart.mart_site_performance_daily
WHERE (site_name ILIKE '%Samator%' OR site_name ILIKE '%Weiss%' OR site_name ILIKE '%Gelora%')
  AND date_key >= '2026-04-01' AND date_key <= '2026-05-16'
GROUP BY site_name ORDER BY site_name;

-- Revenue meter ter-tag
SELECT site_name, asset_id, meter_type
FROM mart.mart_meter_performance_5min
WHERE site_name IN ('PLTS Rooftop Weiss Tech','PT Gelora Djaja 1 MWp')
  AND date_key >= '2026-05-01'
GROUP BY 1,2,3 ORDER BY 1,2;
```

