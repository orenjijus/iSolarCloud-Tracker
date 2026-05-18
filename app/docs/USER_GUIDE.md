# MMSR Data Manager — Panduan

Lihat `app/README` ringkas: jalankan dari **root repo** (folder yang berisi `app/` dan `dbt/`):

```powershell
.\.venv\Scripts\pip install -r app\requirements.txt
.\.venv\Scripts\streamlit run app\main.py
```

## Reingest error: `No dbt_project.yml`

dbt **wajib** menemukan `dbt_project.yml` di folder project (default: `dbt/` di root repo). Tanpa file ini, `dbt run`, seed, dan perintah dbt lain **semua** gagal.

| Perbaikan | Langkah |
|-----------|---------|
| File terhapus | Dari root repo: `git checkout -- dbt/dbt_project.yml` (jika pernah di-track), atau salin file dari repositori lengkap. |
| Struktur beda | Set environment variable **`MMSR_DBT_PROJECT_DIR`** ke path folder yang **berisi** `dbt_project.yml` (bukan perlu `dbt` di dalam nama: path-nya yang berisi file itu). |
| Hanya milih site, `dbt run` masih error | Wajib `dbt_project.yml` + `profiles.yml` + isi `models/` (termasuk `stg_*__perf_unpivoted`). Daftar site di Reingest dari **`seed_inverter_config.csv`** saja — tidak perlu `dim_site`. |

## Reingest

- Daftar site: dari **`dbt/seeds/seed_inverter_config.csv`** (platform + `site_id` + nama site) — **tanpa** `dbt show` / `dim_site`.
- iSolarCloud → `reingest_ps_ids`; FusionSolar → `reingest_plant_codes`.
- Selalu `stg_*__perf_unpivoted+` (tanpa full-refresh di UI).

## Seed Manager

- Upload CSV: delimiter **koma atau titik koma** (auto).
- Nama kolom harus **sama** dengan `dbt/seeds/<nama>.csv` — unduh template dari tab *Data saat ini*.
- `seed_daily_simulation_target`: header harus memakai nama persis seed (mis. `Energy Simulation (MW)`), bukan sinonim Excel.

## Lineage (ringkas)

`raw` → `staging` (`stg_*`) → `dimensions` (`dim_*`) → `facts` → `marts` / `view_*`.  
Detail DAG: lihat SQL di `dbt/models/` dan `dbt/docs/REINGESTION_WORKFLOW.md` jika ada.

## Troubleshooting

| Error | Penyebab |
|-------|----------|
| `File does not exist: app\main.py` | Bukan di root repo, atau folder `app/` belum ada / belum di-pull dari git. |
| `No dbt_project.yml found` | File hilang di folder project dbt — lihat bagian atas panduan; atau set `MMSR_DBT_PROJECT_DIR`. |
| Daftar site kosong / tidak sesuai | Isi atau perbaiki baris di **`dbt/seeds/seed_inverter_config.csv`** untuk platform yang dipilih (`site_id`, nama site). |
