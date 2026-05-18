# Laporan Cakupan Site: Simulasi Harian 25 Tahun (Degradasi)

Dokumen ini merangkum **site mana saja yang sudah ter-generate** dan **yang belum** di pipeline simulasi harian 25 tahun dengan faktor degradasi, serta **data yang dibutuhkan** untuk site yang belum lengkap.

- **Model output**: `mart.mart_simulation_daily_25y`
- **Sumber**: `seed_yearly_simulation_target` (Energy TS 25 tahun), `seed_daily_simulation_target` (profil harian tahun 1)
- **Referensi algoritma**: [DAILY_SIMULATION_DEGRADATION_DESIGN.md](DAILY_SIMULATION_DEGRADATION_DESIGN.md)

---

## 1. Site yang Sudah Ter-Generate

Site berikut **memiliki kedua data** (Energy TS tahun 1 di yearly seed + profil harian tahun 1 di daily seed) sehingga muncul di `mart_simulation_daily_25y`:

| No | Site Name | Site_Code |
|----|-----------|-----------|
| 1 | Garuda Metalindo 1 | 1458125 |
| 2 | Garuda Metalindo 2 | 1453245 |
| 3 | Garuda Metalindo (IKP) | 1445767 |
| 4 | Garuda Metalindo (MPF) | 1449886 |
| 5 | Shoetown Ligung Indonesia | 1479456 |
| 6 | PT. MMKI 1.75 MWp - Painting Building | NE=50488260 |
| 7 | PT. MMKI 5.7 MWp - Phase 2 | NE=51758766 |
| 8 | PLTS Mall Panakkukang | NE=53771627 |
| 9 | PT. Pusan Manis Mulia 2.06 MWp - Tangerang | NE=54435794 |
| 10 | PT. MMKI 4.292 MWP - Phase 3 | NE=58630782 |
| 11 | PLTS Frina Lestari Nusantara | 1628909 |
| 12 | Charoen Pokphand Majalengka | 1614122 |

**Total: 12 site** — semuanya punya data tahunan (Energy TS 25 tahun) dan profil harian tahun 1.

---

## 2. Site yang Belum Ter-Generate

### 2.1 Sudah Punya Profil Harian (Daily), Kurang Data Yearly 25 Tahun

Site berikut **sudah ada di `seed_daily_simulation_target`** (profil harian tahun 1) tetapi **belum ada di `seed_yearly_simulation_target`** dengan Energy TS (MWh) untuk Year 1–25. Setelah data yearly ditambahkan, site ini otomatis akan ikut ter-generate.

| No | Site Name | Site_Code | Data yang dibutuhkan |
|----|-----------|-----------|----------------------|
| 1 | Charoen Pokphand Bandung | 1637095 | **Energy TS (MWh)** 25 tahun (Year 1–25) di `seed_yearly_simulation_target` |
| 2 | Charoen Pokphand Madiun | 1637816 | **Energy TS (MWh)** 25 tahun (Year 1–25) di `seed_yearly_simulation_target` |
| 3 | PLTS Rooftop Sumatera Prima Fibreboard | 1680199 | **Energy TS (MWh)** 25 tahun (Year 1–25) di `seed_yearly_simulation_target` |

**Format yang dibutuhkan** (untuk masing-masing site):

- File: `dbt/seeds/seed_yearly_simulation_target.csv`
- Kolom: `Site_Name`, `Site_Code`, `Year` (1–25), `Energy TS (MWh)`, `Energy Sim Target (MWh)` (opsional)
- Pemisah: titik desimal (e.g. `930.604`), delimiter CSV: semicolon (`;`)
- Satu baris per (Site_Code, Year); untuk degradasi, kolom **Energy TS (MWh)** harus terisi untuk Year 1 dan tahun lainnya.

---

### 2.2 Belum Ada Profil Harian Maupun Data Yearly

Site berikut tercatat di **`seed_site_config`** (konfigurasi site) tetapi **tidak muncul** di seed simulasi (daily maupun yearly). Untuk bisa ter-generate, perlu **kedua** data di bawah.

| No | Site (dari seed_site_config) | Data yang dibutuhkan |
|----|------------------------------|----------------------|
| 1 | PT Gelora Djaja 1 MWp | 1) **Profil harian tahun 1** (365 hari) → `seed_daily_simulation_target` (Site_Code, Date, Energy Simulation/Target, GHI, POA, PR, dll.). 2) **Energy TS (MWh) 25 tahun** (Year 1–25) → `seed_yearly_simulation_target`. |
| 2 | PLTS Ongrid PT Suparma Tbk | 1) **Profil harian tahun 1** (365 hari) → `seed_daily_simulation_target`. 2) **Energy TS (MWh) 25 tahun** (Year 1–25) → `seed_yearly_simulation_target`. |

**Catatan**: Site_Code untuk kedua site ini harus konsisten antara daily dan yearly seed (bisa berupa ID numerik atau format seperti `NE=...` sesuai konvensi yang dipakai).

---

## 3. Ringkasan Data yang Dibutuhkan

| Kebutuhan | Sumber | Format / Isi |
|----------|--------|--------------|
| **Energy TS 25 tahun** (per site) | `seed_yearly_simulation_target.csv` | Site_Code, Year 1–25, Energy TS (MWh); desimal pakai titik; delimiter `;`. |
| **Profil harian tahun 1** (per site) | `seed_daily_simulation_target.csv` | Site_Code, Date (DD/MM/YYYY), Energy Simulation (MW), Energy Target (MW), GHI, POA, Daily PR POA/GHI; 365 hari per site; delimiter `;`. |

Setelah data ditambahkan ke seed yang sesuai:

1. Jalankan: `dbt seed`
2. Jalankan: `dbt run --select mart_simulation_degradation_factors+`
3. Site baru akan muncul di `mart.mart_simulation_daily_25y` dan turunannya (mis. `mart_simulation_targets_daily` untuk `simulation_year = 1`).

---

## 4. Cara Cek Cakupan di Database

Setelah run dbt, cek site yang ter-generate:

```sql
SELECT DISTINCT site_code, site_name
FROM mart.mart_simulation_daily_25y
ORDER BY site_code;
```

Cek site yang punya faktor degradasi (punya yearly) tetapi belum punya daily (tidak akan muncul di mart_simulation_daily_25y):

```sql
-- Site dengan degradation factor (yearly) tapi tidak ada di daily 25y
SELECT DISTINCT df.site_code, df.site_name
FROM mart.mart_simulation_degradation_factors df
WHERE df.simulation_year = 1
  AND NOT EXISTS (
    SELECT 1 FROM mart.mart_simulation_daily_25y d
    WHERE d.site_code = df.site_code
  );
```

---

## 5. Crosscheck: Yearly dari Daily

Yang dibandingkan: **Energy TS (yearly)** dari sumber (seed yearly / `mart_simulation_degradation_factors`) sebagai **referensi**, dengan **yearly dari daily** = jumlah kolom `energy_simulation_mwh` dan `energy_target_mwh` di `mart_simulation_daily_25y` per (site, simulation_year). Jadi: Energy TS yearly vs yearly-from-daily simulation (dan yearly-from-daily target untuk informasi).

### Query crosscheck

- **File**: `queries/validation/crosscheck_simulation_daily_to_yearly.sql`
- **Part 1**: Detail per (site, simulation_year) — kolom: `energy_ts_yearly_mwh`, `yearly_from_daily_simulation_mwh`, `yearly_from_daily_target_mwh`, `diff_simulation_vs_ts_mwh`, status.
- **Part 2** (di komentar): Ringkasan **satu tabel semua site** — satu baris per site dengan angka Year 1 + status 25 tahun.

### Cara pakai

1. Pastikan model sudah di-run: `dbt run --select mart_simulation_daily_25y`
2. Jalankan query via **MCP Postgres** (`execute_sql`) atau psql. Untuk ringkasan satu tabel semua site, gunakan query Part 2 (uncomment di file).
3. OK = selisih simulation vs Energy TS &lt; 0,01 MWh untuk semua 25 tahun.

### Hasil crosscheck — satu tabel semua site (angka + status)

Query Part 2 dijalankan via **MCP Postgres**. Kolom: Energy TS yearly (Year 1), yearly dari daily simulation (Year 1), yearly dari daily target (Year 1), selisih simulation vs TS (Year 1), max selisih absolut 25 tahun, status.

| site_code | site_name | energy_ts_yearly_year1_mwh | yearly_from_daily_simulation_year1_mwh | yearly_from_daily_target_year1_mwh | diff_simulation_vs_ts_year1_mwh | max_abs_diff_25y_mwh | status |
|-----------|-----------|----------------------------|----------------------------------------|------------------------------------|----------------------------------|----------------------|--------|
| 1445767 | Garuda Metalindo (IKP) | 469.30 | 474.04 | 479.44 | 4.7404 | 4.74 | CHECK |
| 1449886 | Garuda Metalindo (MPF) | 500.69 | 505.70 | 511.45 | 5.0170 | 5.02 | CHECK |
| 1453245 | Garuda Metalindo 2 | 880.65 | 889.54 | 899.18 | 8.8953 | 8.90 | CHECK |
| 1458125 | Garuda Metalindo 1 | 930.60 | 940.00 | 950.17 | 9.3999 | 9.40 | CHECK |
| 1479456 | Shoetown Ligung Indonesia | 3807.31 | 3820.68 | 3862.79 | 13.3724 | 13.37 | CHECK |
| 1614122 | Charoen Pokphand Majalengka | 1447.05 | 627.86 | 636.08 | -819.1870 | 819.19 | CHECK |
| 1628909 | PLTS Frina Lestari Nusantara | 1390.22 | 602.93 | 608.82 | -787.2954 | 787.30 | CHECK |
| NE=50488260 | PT. MMKI 1.75 MWp - Painting Building | 2455.20 | 2431.91 | 2458.48 | -23.2902 | 23.29 | CHECK |
| **NE=51758766** | **PT. MMKI 5.7 MWp - Phase 2** | **7780.80** | **7780.79** | **7780.79** | **-0.0095** | **0.01** | **OK** |
| **NE=53771627** | **PLTS Mall Panakkukang** | **1774.36** | **1774.36** | **1792.62** | **0.0002** | **0.00** | **OK** |
| NE=54435794 | PT. Pusan Manis Mulia 2.06 MWp - Tangerang | 2600.34 | 2748.48 | 2772.26 | 148.1433 | 148.14 | CHECK |
| NE=58630782 | PT. MMKI 4.292 MWP - Phase 3 | 2455.20 | 3612.61 | 3636.96 | 1157.4119 | 1157.41 | CHECK |

**Arti kolom:**

- **energy_ts_yearly_year1_mwh**: Energy TS (yearly) tahun 1 — sumber/referensi dari seed yearly.
- **yearly_from_daily_simulation_year1_mwh**: Jumlah `energy_simulation_mwh` dari mart daily 25y untuk tahun 1 (daily dijumlah jadi yearly).
- **yearly_from_daily_target_year1_mwh**: Jumlah `energy_target_mwh` dari mart daily 25y untuk tahun 1.
- **diff_simulation_vs_ts_year1_mwh**: yearly_from_daily_simulation − energy_ts_yearly (yang dibandingkan).
- **max_abs_diff_25y_mwh**: Nilai maksimum |diff| untuk tahun 1–25.
- **status**: OK = semua 25 tahun selisih &lt; 0,01 MWh; CHECK = ada tahun dengan selisih ≥ 0,01 MWh.

**Kesimpulan singkat:**

- **OK (2 site)**: PT. MMKI 5.7 MWp - Phase 2, PLTS Mall Panakkukang — yearly dari daily simulation ≈ Energy TS.
- **CHECK — selisih kecil**: Garuda Metalindo 1/2/IKP/MPF, Shoetown Ligung, PT. MMKI 1.75 — angka yearly dari daily sedikit berbeda dari Energy TS (skala/definisi daily vs TS).
- **CHECK — selisih besar**: Charoen Pokphand Majalengka & PLTS Frina (yearly dari daily jauh lebih kecil), PT. Pusan Manis Mulia (yearly dari daily lebih besar), PT. MMKI 4.292 Phase 3 (yearly dari daily jauh lebih besar; kemungkinan pakai profil harian site lain). Periksa mapping Site_Code dan isi seed daily/yearly.

---

*Terakhir diperbarui berdasarkan isi seed dan hasil crosscheck via MCP Postgres (execute_sql), 2026-02-04.*
