# Ringkasan EDA Site PLTS (SQL) — Persiapan ML

Temuan dari query EDA di PostgreSQL terhadap **site daily** (`mart.mart_site_performance_daily`), **device 5min** (meter, sensor, inverter), dan `dimensions.dim_assets`. Digunakan untuk keputusan filter site/tanggal dan feature/target dalam pipeline ML.

**Exclude untuk analisis device 5min dan rekomendasi ML**: site **Samator** dan **Klinik** di-skip (filter: `site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'`). Site daily tetap mencakup semua site; untuk konsistensi bisa pakai filter yang sama.

---

## 1. Cakupan Data (Site Daily)

| Metrik | Nilai |
|--------|--------|
| **Total baris** | 5.268 (site × hari) |
| **Jumlah site** | 21 |
| **Rentang tanggal** | 2024-04-30 s/d 2026-01-28 |
| **Calendar span** | 639 hari |

- **Site dengan data panjang (≥ 380 hari)**: PT. MMKI 1.75 MWp (626), PT. MMKI 5.7 MWp Phase 2 (582), Garuda Metalindo IKP (453), Garuda Metalindo 1 & 2 (447), Garuda MPF (443), Pusan Manis Mulia (429), Mall Panakkukang (406), Shoetown Ligung (384).
- **Site dengan data sangat pendek (1–2 hari)**: 4 site Samator (Ame Krian, Gas Industri Sidoarjo, Malang, Solo). Disarankan **exclude dari training** atau hanya dipakai untuk inference setelah model stabil.
- **Sistem**: FusionSolar dan iSolarCloud; keduanya tercakup di mart.

---

## 2. Missing Values (Kolom Penting)

| Kolom | Non-null | Null | % Null |
|-------|----------|------|--------|
| daily_energy_mwh | 5.268 | 0 | 0% |
| daily_ghi_kwh_m2 | 5.082 | 186 | 3.5% |
| availability_percent | 5.131 | 137 | 2.6% |
| pr_ghi_actual | 4.997 | 271 | 5.1% |
| pr_poa_actual | 4.603 | 665 | 12.6% |
| energy_target_mwh | 4.019 | 1.249 | 23.7% |

**Rekomendasi ML**:
- **Target/feature energy**: Lengkap; aman dipakai.
- **GHI / PR GHI**: Missing kecil; bisa impute (mis. forward-fill per site) atau drop baris.
- **PR POA**: Missing lebih besar (12.6%); jika dipakai sebagai target, pertimbangkan filter “hanya baris dengan PR POA” atau model terpisah.
- **energy_target_mwh**: Banyak missing (23.7%); cocok sebagai feature/target opsional atau untuk subset data yang punya target simulasi.

---

## 3. Statistik Numerik (Global)

| Metrik | Avg | Min | Max | Stddev |
|--------|-----|-----|-----|--------|
| daily_energy_mwh | 6.40 | 0 | 52.90 | 6.15 |
| daily_ghi_kwh_m2 | 4.56 | 0 | 12.12 | — |
| availability_percent | 0.987 | 0 | 1.0 | — |
| pr_ghi_actual | 0.787 | 0.0013 | 10.0 | — |
| pr_poa_actual | 0.828 | 0.0014 | 2.0 | — |
| energy_target_mwh | 6.86 | 0.16 | 35.05 | — |

- **Outlier kasar**: PR GHI/POA ada nilai ekstrem (max 10 dan 2); layak dicek di Python (IQR/Z-score) dan mungkin di-cap atau di-drop untuk training.
- **Energy 0**: Min energy = 0; bisa hari tanpa produksi atau data hilang; flag `is_issue_date` membantu interpretasi.

---

## 4. Issue Dates

| is_issue_date | N | % |
|----------------|---|---|
| false | 5.064 | 96.13% |
| true | 204 | 3.87% |

**Dampak pada metrik (rata-rata)**:
- **Non-issue**: avg energy 6.30 MWh, avg PR GHI 0.79, avg availability 0.99.
- **Issue**: avg energy 8.85 MWh, avg PR GHI 0.67, avg availability 0.91.

Issue days punya energy rata-rata lebih tinggi tapi PR dan availability lebih rendah (konsisten dengan maintenance/partial operation). Untuk ML: **flag `is_issue_date` sebagai feature** atau latih hanya pada `is_issue_date = false` untuk prediksi “normal”.

---

## 5. Dimensi Site (dim_assets)

- **Total site di dim_assets (asset_level = 'Site')**: 25.
- **Yang punya performance daily**: 21 (4 site belum ada di mart).
- **Kelengkapan atribut**: has_capacity 16, has_lat 24, has_lon 24, has_tariff 15.

Untuk ML: **latitude/longitude** hampir lengkap; **capacity** dan **tariff** hanya sebagian; gunakan sebagai feature opsional dan handle missing (impute atau mask).

---

## 6. Rekomendasi Filter untuk ML

1. **Minimum hari per site**: Filter site dengan ≥ 30 hari (atau 90 hari untuk time-series) agar cukup untuk train/val; exclude 4 site Samator dengan 1–2 hari jika tidak dipakai untuk inference only.
2. **Tanggal**: Gunakan time-based split (mis. train sampai 2025-09, val 2025-10–11, test 2025-12 onward) untuk menghindari leakage.
3. **Target**: Pilih satu atau kombinasi: `daily_energy_mwh`, `pr_ghi_actual`, atau `pr_poa_actual`; untuk “vs target” bisa pakai `energy_target_mwh` pada subset yang non-null.
4. **Missing**: Untuk regresi PR/energy, impute GHI/POA per site (e.g. interpolate) atau drop baris dengan missing target; untuk target `energy_target_mwh`, latih hanya pada baris yang punya target.
5. **Kategorik**: Encode `site_id` dan `system` (e.g. one-hot atau embedding); `is_issue_date` sebagai binary feature atau filter.

---

## 7. Device 5min (Meter, Sensor, Inverter) — Exclude Samator & Klinik

EDA device-level memakai data 5 menit per device (meter, sensor, inverter) per site. Site **Samator** dan **Klinik** di-exclude.

| Device  | Total baris (5min) | Jumlah site | Jumlah asset |
|---------|--------------------|-------------|--------------|
| Meter   | ~48,1 jt           | 17          | 129          |
| Sensor  | ~30,3 jt           | 17          | 126          |
| Inverter| ~52,1 jt           | 17          | 213          |

- **Meter**: metrik utama `positive_active_energy`, `negative_active_energy`, `meter_active_power`, `meter_power_factor`, dll.
- **Sensor**: `daily_irradiance`, `irradiance`, `ambient_temp`, `pv_temp`, `rainfall`, dll.
- **Inverter**: `inv_yield`, `inv_active_power`, `inv_dc_power`, `inv_reactive_power`, `inv_power_factor`, dll.

Untuk ML: gunakan **daily aggregates** (query `eda_device_5min_daily_aggregates.sql`) agar ukuran data manageable; atau sample 5min per site/bulan untuk model resolusi tinggi. Query EDA device: `eda_device_5min_overview.sql`, `eda_device_5min_missing.sql`, `eda_device_5min_numeric_summary.sql`, `eda_device_5min_daily_aggregates.sql`.

---

**Query SQL EDA** (folder `queries/`):

- **Site daily**: `eda_site_daily_overview.sql`, `eda_site_missing_stats.sql`, `eda_site_numeric_summary.sql`, `eda_site_issue_dates.sql`, `eda_site_dim_assets_join.sql`.
- **Device 5min** (excl. Samator/Klinik): `eda_device_5min_overview.sql`, `eda_device_5min_missing.sql`, `eda_device_5min_numeric_summary.sql`, `eda_device_5min_daily_aggregates.sql`.
