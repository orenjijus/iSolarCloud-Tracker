# Laporan — Perubahan Calculation Start Date PT Gelora Djaja 1 MWp

| Field | Nilai |
| ----- | ----- |
| **Tanggal** | 18 Mei 2026 |
| **Site** | PT Gelora Djaja 1 MWp (`pt_gelora_djaja_1_mwp`, `FS_SITE_NE=61847068`) |
| **Referensi** | [Koreksi Weiss & Gelora (17 Mei)](./2026-05-17_Config_Correction_Weiss_Gelora.md) · [8 Site Energy](./2026-05-16_8Site_Energy_Availability_Report.md) |

---

## 1. Ringkasan

**Calculation start date** site Gelora Djaja disesuaikan dari **1 Desember 2025** menjadi **7 April 2026** (tanggal operasional resmi). Perubahan diterapkan di seed, `dim_assets`, mart performa harian, dan baris historis sebelum tanggal tersebut dihapus dari mart.

| Item | Sebelum | Sesudah |
| ---- | ------- | ------- |
| `seed_site_config.CalculationStartDate` | `2025-12-01` | **`2026-04-07`** |
| `seed_site_config.Tariff` (Gelora) | *(kosong)* | **`776.84`** |
| `seed_site_config.Tariff` (Weiss Tech) | *(kosong)* | **`683.61`** |
| `dimensions.dim_assets.calculation_start_date` | 2025-12-01 (WIB) | **2026-04-07** (WIB) |
| Baris `mart_site_performance_daily` | 67 hari (Feb–Mei 2026) | **35 hari** (≥ calc start) |
| Baris sebelum calc start di mart | 32 | **0** (dibersihkan) |

---

## 2. Perubahan konfigurasi

**File:** `dbt/seeds/seed_site_config.csv`

```text
pt_gelora_djaja_1_mwp;1000.4;;16;2026-04-07;6
```

**Load & dimensi:**

```bash
cd dbt
dbt seed --select seed_site_config
dbt run --select dim_assets
```

---

## 3. Re-run mart (18 Mei 2026)

```bash
cd dbt
dbt run --select mart_site_performance_daily+ \
  --vars '{"reingest_start_date": "2026-04-01", "reingest_end_date": "2026-05-18"}'
```

| Model | Hasil | Durasi |
| ----- | ----- | ------ |
| `mart_site_performance_daily` | PASS — INSERT 1.210 baris (semua site, window reingest) | ~80 d |
| `mart_site_performance_monthly` | PASS — view di-refresh | <1 d |
| `mart_string_performance_daily` | PASS — INSERT 26 baris | ~578 d |
| `mart_string_performance_monthly` | PASS — SELECT 69.747 baris | ~9 d |

**Total:** 5/5 model PASS (~11 menit).

> **Catatan incremental:** Re-run window tidak otomatis menghapus baris Gelora yang sudah terlanjur terbentuk saat calc start masih Desember 2025. Diperlukan cleanup manual (bagian 4).

---

## 4. Cleanup baris stale di mart

```sql
DELETE FROM mart.mart_site_performance_daily d
WHERE d.site_id = 'FS_SITE_NE=61847068'
  AND d.date_key < (
    SELECT calculation_start_date
    FROM dimensions.dim_assets
    WHERE asset_id = 'FS_SITE_NE=61847068'
  );
```

**Hasil:** **32 baris** dihapus (periode **23 Feb – 6 Apr 2026** kalender WIB yang sebelumnya ikut terhitung).

**Verifikasi orphan:**

```sql
SELECT COUNT(*) AS orphan_rows
FROM mart.mart_site_performance_daily d
JOIN dimensions.dim_assets da ON d.site_id = da.asset_id
WHERE d.site_id = 'FS_SITE_NE=61847068'
  AND d.date_key < da.calculation_start_date;
-- orphan_rows = 0
```

---

## 5. Hasil verifikasi mart

### 5.1 Ringkas harian

| Metrik | Nilai |
| ------ | ----- |
| Rentang mart (WIB) | **7 Apr 2026** – **17 Mei 2026** |
| Jumlah hari di mart | **35** |
| Total energi (MWh) | **81,762** |
| `mart_string_performance_daily` | 8.190 baris; **0** baris sebelum calc start |

### 5.2 Agregat per bulan (WIB)

| Bulan | Hari di mart | Energi (MWh) | Keterangan |
| ----- | ------------ | ------------ | ---------- |
| **Apr 2026** | 24 | **81,757** | Periode operasional penuh (7–30 Apr); PR GHI ~0,79–0,87 di hari produktif |
| **Mei 2026** | 11 | **0,005** | Hampir semua hari `daily_energy_mwh = 0` — **bukan** efek calc start; gap ingest **EM-POI** (sudah dilaporkan 17 Mei) |

### 5.3 Sampel harian (7 Apr – 16 Mei 2026, WIB)

| Tanggal | Energi (MWh) | GHI (kWh/m²) | Avail. | PR GHI |
| ------- | ------------ | ------------ | ------ | ------ |
| 2026-04-07 | 3,994 | 5,47 | 99,3% | 0,731 |
| 2026-04-12 | 4,895 | 6,12 | 100% | 0,800 |
| 2026-04-14 | 5,429 | 6,84 | 100% | 0,793 |
| 2026-04-16 | 5,127 | 6,52 | 100% | 0,786 |
| 2026-04-30 | 0,005 | 0,00 | — | — |
| 2026-05-07 | 0,000 | 6,18 | 100% | — |
| 2026-05-14 | 0,000 | 3,14 | 100% | — |

**Interpretasi:**

- **Apr 7–29:** Energi dan GHI konsisten; site siap dilaporkan dari tanggal calc start baru.
- **Apr 30:** Energi hampir nol (hari spesifik / data meter).
- **Mei 7+:** GHI terisi, energi **0** — perlu tindak lanjut ingest **EM-POI** (`AM0610254C346758`), bukan ubah calc start lagi.

---

## 6. Dampak ke laporan / BI

| Area | Dampak |
| ---- | ------ |
| Dashboard site performance | Gelora **tidak muncul** untuk tanggal &lt; 7 Apr 2026 |
| Agregat bulanan Feb–Mar 2026 | **Dihapus** dari mart harian Gelora (sesuai kebijakan baru) |
| Excel cross-check / KPI historis | Bandingkan ulang hanya dari **Apr 2026** untuk Gelora |
| `mart_site_performance_monthly` | View otomatis mengikuti harian — Apr ≈ 81,8 MWh; Feb–Mar tidak lagi tersumbang Gelora |

---

## 7. Tindak lanjut

| # | Tindakan | Prioritas |
| - | -------- | --------- |
| 1 | Cek **ingest EM-POI** FusionSolar untuk Mei 2026 (energi 0 di mart) | P1 |
| 2 | Isi **kapasitas POA** sensor EM011/021/031 → `daily_poa_weighted` terisi | P2 |
| 3 | Jika ada MV/materialized view per-site lama (2025), refresh terpisah jika masih dipakai | P3 |

---

## 8. SQL verifikasi (salin-pakai)

```sql
-- Calc start di dimensi
SELECT asset_name, calculation_start_date::date AS calc_start_wib
FROM dimensions.dim_assets
WHERE asset_id = 'FS_SITE_NE=61847068';

-- Ringkas mart harian
SELECT
  MIN(date_key AT TIME ZONE 'Asia/Jakarta')::date AS min_date_wib,
  MAX(date_key AT TIME ZONE 'Asia/Jakarta')::date AS max_date_wib,
  COUNT(*) AS days,
  ROUND(SUM(daily_energy_mwh)::numeric, 3) AS energy_mwh
FROM mart.mart_site_performance_daily
WHERE site_id = 'FS_SITE_NE=61847068';

-- Per bulan (WIB)
SELECT
  EXTRACT(YEAR FROM date_key AT TIME ZONE 'Asia/Jakarta')::int AS year,
  EXTRACT(MONTH FROM date_key AT TIME ZONE 'Asia/Jakarta')::int AS month,
  COUNT(*) AS days,
  ROUND(SUM(daily_energy_mwh)::numeric, 3) AS energy_mwh
FROM mart.mart_site_performance_daily
WHERE site_id = 'FS_SITE_NE=61847068'
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

## 9. Tariff — Weiss Tech & Gelora Djaja

| Site (`seed_site_config`) | Tariff (Rp/kWh) |
| ------------------------- | --------------- |
| `plts_rooftop_weiss_tech` | **683.61**      |
| `pt_gelora_djaja_1_mwp`   | **776.84**      |

Sudah di-load (`dbt seed --select seed_site_config`), `dim_assets` di-refresh, dan `mart_site_performance_daily` di-reingest **2025-01-01 → 2026-05-18** agar kolom `tariff` terisi di semua baris historis kedua site.

---

## 10. Backfill GHI — gap 5–7 Mei 2026 (18 Mei 2026, malam)

### Konteks

Sensor GHI (**EM0010254C346758**) mati sejak **1 Mei 2026**; data API tersedia lagi dari **5 Mei**. Tanggal **8 Mei ke atas** sudah ada di mart sebelumnya — yang kosong hanya **5, 6, dan 7 Mei** (kalender WIB).

### Tindakan

**1. Tarik ulang dari FusionSolar API** (meteo station, plant Gelora):

```bash
cd fusionsolar
python fusionsolar_data_harvester.py \
  --fetch-historical 2026-05-05 2026-05-07 \
  --plant-codes "NE=61847068" \
  --device-types meteo_station
```

**2. Re-ingest dbt** (wajib sertakan **staging unpivot** + window `2026-05-04` agar hari WIB tidak terpotong UTC):

```bash
cd dbt
dbt run --select stg_fusionsolar__perf_unpivoted mart_sensor_measurements_5min mart_sensor_daily mart_site_performance_daily \
  --vars '{"reingest_start_date": "2026-05-04", "reingest_end_date": "2026-05-07", "reingest_plant_codes": "NE=61847068"}'
```

### Hasil verifikasi

**Raw** (`EM0010254C346758`, interval/hari WIB):

| Tanggal (WIB) | Interval | Avg `radiant_line` (W/m²) |
| ------------- | -------- | ------------------------- |
| 5 Mei 2026    | 288      | 243,6                     |
| 6 Mei 2026    | 288      | 264,2                     |
| 7 Mei 2026    | 288      | 258,1                     |

**Mart** (`mart_sensor_daily` / `mart_site_performance_daily`, GHI kWh/m²):

| `date_key` (konvensi mart) | GHI sensor | GHI site |
| -------------------------- | ---------- | -------- |
| 2026-05-04 17:00 UTC (= **5 Mei** WIB) | **4,57** | **4,57** |
| 2026-05-05 17:00 UTC (= **6 Mei** WIB) | **5,85** | **5,85** |
| 2026-05-06 17:00 UTC (= **7 Mei** WIB) | **6,32** | **6,32** |
| 2026-05-07 17:00 UTC (= **8 Mei** WIB) | 6,18 | 6,18 *(sudah ada)* |

Gap **5–7 Mei** di mart **terisi**. Tidak perlu re-fetch **8 Mei+** (sudah lengkap).

> **Catatan teknis:** Run dbt pertama hanya ke mart (tanpa `stg_fusionsolar__perf_unpivoted`) tidak mengisi hari baru — staging incremental harus di-refresh dulu untuk rentang yang sama.

---

*Dokumen ini melengkapi thread perubahan calc start dan backfill GHI Gelora Djaja (18 Mei 2026).*
