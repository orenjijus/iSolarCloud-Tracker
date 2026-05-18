# Alur Seed → Site Performance

Data dari seed **daily_kpi_monthly** dan **daily_simulation_target** sudah terhubung ke `mart_site_performance_daily`. Setelah `dbt seed`, Anda perlu menjalankan model dbt agar data mengalir sampai site performance.

## Alur data

### 1. seed_daily_kpi_monthly → site performance

```
seed_daily_kpi_monthly  →  mart_site_kpi_monthly  →  mart_site_performance_daily
                        →  mart_simulation_targets_daily (juga pakai KPI monthly)
```

- **mart_site_kpi_monthly**: baca dari `seed_daily_kpi_monthly`, output monthly KPI per site.
- **mart_site_performance_daily**: JOIN ke `mart_site_kpi_monthly` untuk kolom `energy_kpi_daily_mwh`, `energy_a_kpi_daily_mwh`, `energy_kpi_monthly_mwh`, dll.

### 2. seed_daily_simulation_target → site performance

```
seed_daily_simulation_target  →  mart_simulation_targets_daily  →  mart_site_performance_daily
```

- **mart_simulation_targets_daily**: baca langsung dari `seed_daily_simulation_target` (cara lama). Semua site yang ada di seed ikut masuk. Plus JOIN `mart_site_kpi_monthly` untuk KPI harian.
- **mart_site_performance_daily**: JOIN ke `mart_simulation_targets_daily` untuk target harian (GHI, POA, energy target, PR, dll).

**Catatan:** Model **mart_simulation_daily_25y** (dan pipeline degradasi 25 tahun) tetap ada di project tapi **tidak dipakai** untuk alur simulation targets; dipakai hanya untuk crosscheck/eksperimen.

## Langkah setelah seed

Setelah menambah/update seed dan menjalankan:

```bash
dbt seed
```

Jalankan model yang memakai seed tersebut (bisa full run atau hanya downstream dari seed):

```bash
# Opsi 1: Jalankan semua model (paling aman)
dbt run

# Opsi 2: Hanya model yang memakai seed KPI + simulation target sampai site performance
dbt run --select mart_site_kpi_monthly mart_simulation_targets_daily mart_site_performance_daily
```

`mart_site_performance_daily` bersifat **incremental**; untuk tanggal yang sudah ada, gunakan reingest atau full refresh jika ingin memaksa ulang.

### Update semua data Site Performance

**Opsi 1 — Reingest rentang tanggal** (re-process tanggal tertentu; upstream KPI/simulation harus sudah di-run):

```bash
dbt seed
dbt run --select mart_site_kpi_monthly mart_simulation_targets_daily mart_site_performance_daily --vars '{"reingest_start_date": "2024-01-01", "reingest_end_date": "2026-12-31"}'
```

Ganti `2024-01-01` dan `2026-12-31` dengan rentang yang ingin di-update.

**Opsi 2 — Full refresh** (hapus tabel lalu bangun ulang dari nol; butuh semua upstream sudah ada):

```bash
dbt seed
dbt run --select mart_site_kpi_monthly mart_simulation_targets_daily
dbt run --select mart_site_performance_daily --full-refresh
```

**Opsi 3 — Hanya data baru** (tanpa reingest; hanya menambah tanggal yang belum ada):

```bash
dbt seed
dbt run --select mart_site_kpi_monthly mart_simulation_targets_daily mart_site_performance_daily
```

## Ringkasan

| Seed                     | Model pertama        | Sampai ke site performance? |
|--------------------------|-----------------------|-----------------------------|
| seed_daily_kpi_monthly   | mart_site_kpi_monthly | Ya (via JOIN di mart_site_performance_daily) |
| seed_daily_simulation_target | mart_simulation_targets_daily | Ya (langsung dari seed; semua site di seed ikut) |

Setelah `dbt seed` + `dbt run`, data dari kedua seed tersebut sudah masuk sampai **mart_site_performance_daily**.
