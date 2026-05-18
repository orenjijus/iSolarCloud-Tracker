# PoC: Daily Simulation 25 Tahun (Satu Site)

Model **mart_simulation_daily_25y_poc** memvalidasi algoritma daily simulation 25 tahun dengan degradasi untuk **satu site** (Garuda Metalindo 1, `site_code` 1458125) sebelum dipakai untuk semua site.

## Cara menjalankan

1. **Load seed yearly** (di PoC ini seed yearly di-enable untuk test):
   ```bash
   dbt seed --select seed_yearly_simulation_target
   ```

2. **Pastikan seed daily dan staging sites sudah ada** (biasanya sudah dari run biasa):
   ```bash
   dbt seed --select seed_daily_simulation_target
   dbt run --select stg_isolarcloud__sites
   ```

3. **Jalankan model PoC**:
   ```bash
   dbt run --select mart_simulation_daily_25y_poc
   ```

## Validasi hasil

- Tabel output: `mart.mart_simulation_daily_25y_poc`
- Jumlah baris yang diharapkan: **25 × 365 = 9.125** baris (satu site, 25 tahun, 365 hari/tahun)
- Cek contoh:
  - **Tahun 1, day 1**: `energy_simulation_mwh` ≈ nilai 1 Januari dari seed daily; `degradation_factor` = 1
  - **Tahun 2, day 1**: `energy_simulation_mwh` = (nilai 1 Jan tahun 1) × (Energy Year 2 / Energy Year 1); `degradation_factor` < 1
  - **simulation_start_year**: dari `stg_isolarcloud__sites.install_date` untuk `ps_id = '1458125'`; jika tidak ada, fallback 2025

Contoh query validasi:

```sql
-- Sample: tahun 1 vs tahun 2, hari ke-1
SELECT date_key, simulation_year, day_of_year, degradation_factor, energy_simulation_mwh
FROM mart.mart_simulation_daily_25y_poc
WHERE day_of_year = 1
ORDER BY simulation_year
LIMIT 5;
```

## Catatan seed yearly

CSV [seed_yearly_simulation_target.csv](dbt/seeds/seed_yearly_simulation_target.csv) memakai **koma sebagai pemisah desimal** (e.g. 930,604). Jika saat load dbt nilai pecahan terpisah jadi dua kolom (930 dan 604), maka kolom "Energy TS (MWh)" hanya berisi bagian bulat. Untuk hasil benar, nilai di CSV sebaiknya dalam satu sel (e.g. kutip nilai: "930,604") atau gunakan pemisah lain (e.g. titik) agar satu kolom berisi 930.604.

## Referensi

- Desain lengkap: [docs/DAILY_SIMULATION_DEGRADATION_DESIGN.md](../../docs/DAILY_SIMULATION_DEGRADATION_DESIGN.md)
- Action plan: rencana di `.cursor/plans/` (daily simulation 25 tahun)
