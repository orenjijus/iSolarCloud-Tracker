# Investigasi Anomali Energi — PLTS Rooftop Sumatera Prima Fibreboard (SPF)

**Site:** PLTS Rooftop Sumatera Prima Fibreboard (`ps_id` `1680199`, `site_id` `ISO_SITE_1680199`)  
**Tanggal laporan:** 18 Mei 2026  
**Status:** **Selesai diperbaiki (revisi 2)** — `REVENUE_PERIOD` tetap aktif, ditambah deduplikasi pembacaan meter per timestamp di `mart_site_performance_daily` setelah anomali berulang pada 16 Mei 2026.

**Dokumen terkait:**

- [2026-05-16_8Site_Energy_Availability_Report.md](./2026-05-16_8Site_Energy_Availability_Report.md) — pola `REVENUE_PERIOD` (Samator)
- [2026-05-17_Samator_Bali_Meter_Investigation.md](./2026-05-17_Samator_Bali_Meter_Investigation.md) — kasus meter stuck / pergantian
- [2026-05-16_Laporan_Masalah_Sistem_MMSR.md](./2026-05-16_Laporan_Masalah_Sistem_MMSR.md) — §14 ringkasan temuan ini

---

## 1. Ringkasan eksekutif (untuk user / stakeholder)

| Aspek | Keterangan |
| ----- | ---------- |
| **Gejala di dashboard** | Energi aktual melonjak ke **~700–760 MWh/hari** (Apr–Mei 2026), PR GHI **>3.000%**, padahal GHI normal (~4–6 kWh/m²/hari). |
| **Bukan masalah panel/inverter** | Irradiasi (GHI) dan target simulasi masih masuk akal; hanya **perhitungan energi dari meter revenue** yang salah. |
| **Penyebab** | Tiga meter ditandai `Revenue` di seed, termasuk **meter lama yang sudah diganti** (nilai counter **macet**). Data meter lama ikut masuk ke stream meter baru → algoritma mengira meter “reset” → energi harian **~670 MWh palsu** ditambah meter kedua yang benar. |
| **Perbaikan** | Konfigurasi **`REVENUE_PERIOD`** (sama seperti Samator): hanya meter yang **aktif per tanggal** yang dihitung. |
| **Hasil setelah fix** | Tidak ada lagi hari >50 MWh; energi harian kembali **~8–13 MWh**; PR GHI **~60–70%**. |

---

## 2. Gejala yang dilaporkan

### 2.1 Tampilan Power BI / dashboard

- **April 2026:** spike energi pada tanggal tertentu (mis. 28 Apr: **675 MWh** vs target **~6 MWh**).
- **Mei 2026:** banyak hari dengan energi **700+ MWh**, PR GHI **~3.000–5.800%**, sementara GHI harian tetap rendah (~3–5).
- **Energy vs GHI** dan **Energy Actual/Target** jauh di atas 1.000% — secara fisik tidak mungkin untuk PLTS rooftop ~3 MWp class.

### 2.2 Contoh angka sebelum perbaikan (mart)

| `date_key` | `daily_energy_mwh` (salah) | `ghi_actual` | `pr_ghi_actual` |
| ---------- | -------------------------- | ------------ | ----------------- |
| 2026-04-27 | 11,73 | 5,89 | 0,65 |
| **2026-04-28** | **676,37** | 5,97 | 10,0 (cap) |
| 2026-04-29 | 10,06 | 5,66 | 0,58 |
| **2026-05-04** | **703,60** | 3,84 | 10,0 (cap) |
| **2026-05-12** | **739,43** | 0,47 | 10,0 (cap) |
| 2026-05-15 | 8,23 | 4,22 | 0,64 |

Hari “normal” dan hari “spike” **bergantian** meskipun kondisi cuaca serupa — indikasi **bug perhitungan**, bukan produksi lapangan.

---

## 3. Akar masalah (teknis)

### 3.1 Konfigurasi meter revenue

Site SPF memiliki **tiga** meter bertipe `Revenue` di `seed_meter_config`:

| Nama | `esn_code` | Keterangan |
| ---- | ---------- | ---------- |
| Meter-kWh-01 | `1680199_7_18_1` | Meter **lama** — diganti 11 Okt 2025 |
| Meter-kWh-01(new) | `1680199_7_14_1` | Meter **aktif** sejak pergantian |
| Meter-kWh-02 | `1680199_7_13_2` | Meter export paralel (~6 MWh/hari) |

Tanpa `REVENUE_PERIOD`, model `mart_site_performance_daily` **menjumlahkan ketiganya**.

### 3.2 Meter lama macet + ID consolidation

- Counter meter lama (`1680199_7_18_1`) **beku** di **347.302,3 kWh** (tidak naik lagi).
- `seed_meter_site_mapping` sudah punya `ID_CONSOLIDATION` (lama → baru sejak 2025-10-11), sehingga di `mart_meter_performance_5min` nilai beku meter lama **juga muncul** di bawah asset meter baru (`ISO_1680199_7_14_1`).
- Per interval 5 menit, meter baru punya **dua nilai**: ~347k (sisa lama) dan ~1,0 juta kWh (kumulatif benar).

### 3.3 Logika energi harian yang memicu spike

Di `mart_site_performance_daily`:

1. Jika `first_value` hari ini ≈ `max` kemarin → meter **kumulatif** → energi = `max_hari_ini - max_kemarin`.
2. Jika tidak → meter dianggap **reset** → energi = `max - min` **dalam hari**.

Pada hari spike, `first_value` terambil dari nilai **347.302** (stale), `max` ~**1.017.000** → mode **resetting** → delta **~670.000 kWh ≈ 670 MWh** hanya dari Meter-kWh-01(new), ditambah ~6 MWh dari Meter-kWh-02 → total **~676–760 MWh**.

Pada hari normal, `first_value` mengikuti nilai kumulatif benar → mode **cumulative** → delta **~5–6 MWh** per meter utama.

**Kesimpulan teknis:** Bukan konversi Wh/kWh salah, melainkan **meter lama tidak di-exclude** dari perhitungan revenue + **duplikasi nilai** setelah konsolidasi ID.

---

## 4. Perbaikan yang dilakukan

### 4.1 Seed `REVENUE_PERIOD` (pola Samator)

Baris ditambahkan di `dbt/seeds/seed_meter_site_mapping.csv`:

```text
REVENUE_PERIOD;"1680199_7_18_1";"1680199";;;2025-10-10;"SPF - revenue Meter-kWh-01 lama sampai pergantian 10 Okt 2025"
REVENUE_PERIOD;"1680199_7_14_1";"1680199";;2025-10-11;;"SPF - revenue Meter-kWh-01(new) sejak pergantian 11 Okt 2025"
REVENUE_PERIOD;"1680199_7_13_2";"1680199";;;;"SPF - revenue Meter-kWh-02 (meter export paralel)"
```

| Meter | Aktif pada `date_key` |
| ----- | --------------------- |
| `1680199_7_18_1` | s/d **10 Okt 2025** (inklusif) |
| `1680199_7_14_1` | dari **11 Okt 2025** |
| `1680199_7_13_2` | **selalu** (tanpa batas tanggal) |

Logika di `mart_site_performance_daily`: jika site punya baris `REVENUE_PERIOD`, hanya meter yang **match `device_id` + rentang tanggal** yang dipakai untuk energi (sama seperti Samator Solo/Semarang/Bali).

### 4.2 Deploy dbt

```bash
cd dbt
dbt seed --select seed_meter_site_mapping
dbt run --select mart_site_performance_daily \
  --vars '{"reingest_start_date": "2026-04-01", "reingest_end_date": "2026-05-31"}'
```

**Catatan PowerShell:** gunakan quoting JSON yang valid untuk `--vars` (lihat log run 17–18 Mei 2026).

### 4.3 Perbaikan lanjutan (insiden berulang 16 Mei 2026)

Setelah fix awal, anomali muncul lagi pada **16 Mei 2026**. Investigasi ulang menunjukkan:

- Data `mart_meter_performance_5min` untuk asset aktif `ISO_1680199_7_14_1` masih dapat berisi **duplikasi point per timestamp** (counter stale + counter valid).
- Pada kondisi tertentu, `first_value` harian dapat mengambil nilai stale (lebih kecil) sehingga algoritma menganggap meter reset dan menghitung delta palsu.

Perbaikan lanjutan di model `dbt/models/marts/mart_site_performance_daily.sql`:

- Tambah tahap deduplikasi pembacaan meter per `(date_key, asset_id, metric_name, timestamp)`.
- Untuk duplikasi pada bucket 5-menit yang sama, dipilih `MAX(metric_value_kwh)` agar counter valid menang atas counter stale.
- `min_value`, `max_value`, `first_value`, `last_value` dihitung dari hasil deduplikasi (bukan raw langsung).

Deploy:

```bash
cd dbt
dbt run --full-refresh --select mart_site_performance_daily
```

---

## 5. Hasil verifikasi (pasca perbaikan)

### 5.1 Energi harian — tidak ada lagi spike

| `date_key` | Sebelum (MWh) | Sesudah (MWh) |
| ---------- | ------------- | ------------- |
| 2026-04-28 | **676,37** | **12,17** |
| 2026-05-04 | **703,60** | **7,84** |
| 2026-05-12 | **739,43** | **6,47** |
| 2026-05-13 | **744,15** | **10,76** |

- **0 hari** dengan `daily_energy_mwh` > 50 MWh pada rentang Apr–Mei 2026 (SPF).
- PR GHI kembali **~0,6–0,7** pada hari cerah (kecuali hari GHI bermasalah, mis. 12 Mei GHI 0,47).

Verifikasi ulang setelah perbaikan lanjutan (run 18 Mei 2026):

| `date_key` | Nilai sebelum revisi 2 | Nilai sesudah revisi 2 |
| ---------- | ---------------------- | ---------------------- |
| **2026-05-16** | **~753,68 MWh** (anomali) | **8,35 MWh** (normal) |
| **2026-05-17** | **~760,37 MWh** (anomali) | **10,97 MWh** (normal) |

### 5.2 Query verifikasi (opsional)

```sql
SELECT date_key, daily_energy_mwh, ghi_actual, pr_ghi_actual
FROM mart.mart_site_performance_daily
WHERE site_id = 'ISO_SITE_1680199'
  AND date_key BETWEEN '2026-04-01' AND '2026-05-31'
ORDER BY date_key;
```

```sql
SELECT mapping_type, device_id, effective_date_start, effective_date_end
FROM staging.seed_meter_site_mapping
WHERE logical_device_id = '1680199' AND mapping_type = 'REVENUE_PERIOD';
```

---

## 6. Lesson learned

| # | Pelajaran | Tindakan pencegahan |
| - | --------- | ------------------- |
| 1 | **Setiap pergantian meter revenue wajib `REVENUE_PERIOD`**, tidak cukup `ID_CONSOLIDATION` saja. Konsolidasi ID mengarahkan device key di mart 5-min, tetapi **tidak** menghentikan penjumlahan ganda di agregasi harian jika meter lama masih `Revenue` di seed. | Checklist O&M: saat meter diganti → update `seed_meter_site_mapping` + `dbt seed` + reingest mart. |
| 2 | **Gejala “PR >1.000%” dengan GHI normal** hampir selalu masalah **energi meter**, bukan sensor irradiasi. | Urutan investigasi: meter revenue → seed → raw counter → baru GHI/inverter. |
| 3 | **Spike intermiten** (hari normal / hari anomali bergantian) mengarah ke **logika kumulatif vs reset** (`first_value` vs `prev_day_max`), sering dipicu data duplikat atau nilai stale. | Saat audit: bandingkan `min`/`max`/`first_value` per `asset_id` di `mart_meter_performance_5min`. |
| 4 | Site dengan **>1 meter revenue aktif** (mis. POI + export) perlu **beberapa baris `REVENUE_PERIOD`** dengan rentang yang benar, bukan menonaktifkan semua kecuali satu tanpa analisis. | SPF: meter 01(new) + meter 02 keduanya tetap aktif setelah Okt 2025. |
| 5 | **Dashboard user:** jelaskan bahwa angka lama di cache semantic model perlu **refresh** setelah backfill dbt. | Komunikasi ke user: refresh dataset Power BI pasca 18 Mei 2026. |

---

## 7. Rekomendasi lanjutan

| # | Rekomendasi | Prioritas |
| - | ----------- | --------- |
| 1 | Reingest `mart_site_performance_daily` dari **2025-10-11** jika laporan historis pasca pergantian meter perlu diaudit | P2 |
| 2 | Pertimbangkan **hapus `Revenue`** dari `1680199_7_18_1` di `seed_meter_config` (redundan dengan `REVENUE_PERIOD`) | P3 |
| 3 | **Sudah diterapkan di `mart_site_performance_daily`**: dedupe pembacaan meter per bucket waktu (ambil `MAX`) untuk mencegah stale value memicu false reset | P1 (closed) |
| 4 | Data quality alert: `daily_energy_mwh` > 3× `energy_target_mwh` atau PR GHI > 150% → flag | P3 |

---

## 8. Riwayat pekerjaan

| Tanggal | Aktivitas |
| ------- | --------- |
| 18 Mei 2026 | Investigasi gejala dashboard; root cause meter revenue ganda + stale consolidation |
| 18 Mei 2026 | Tambah 3 baris `REVENUE_PERIOD` SPF; `dbt seed`; reingest Apr–Mei 2026 |
| 18 Mei 2026 | Verifikasi DB: spike hilang; dokumentasi laporan ini |
| 18 Mei 2026 | Insiden berulang (16 Mei) terdeteksi; root cause tambahan: duplikasi point 5-min dalam asset aktif |
| 18 Mei 2026 | Patch `mart_site_performance_daily` (dedupe per timestamp), `dbt run --full-refresh`, verifikasi ulang normal |

---

**Dibuat:** 18 Mei 2026  
**Diperbaiki oleh:** konfigurasi `REVENUE_PERIOD` + reingest `mart_site_performance_daily`  
**File seed:** `dbt/seeds/seed_meter_site_mapping.csv`  
**Model terdampak:** `mart_site_performance_daily` (energi aktual, PR GHI)
