# dbt seeds (salinan untuk MMSR)

File `seeds/seed_inverter_string_layout.csv` disinkronkan dari Google Sheet **Data Static MMSR** (tab **JOIN String**). Skrip memetakan kolom **Sites** ke `site_id` / `site_name` di seed (18 kode: PMM, MID, SLI, MMKI 1–3, Suparma, CP Bandung/MDN/MJL, FLN, GD, GM 1/2/3/MPF, STBC, SPF — lihat `SITE_MAP` di `scripts/sync_inverter_string_layout_from_gsheet.py`).

## Memperbarui dari sheet

Dari root repo:

```bash
# Semua site yang punya mapping
python scripts/sync_inverter_string_layout_from_gsheet.py

# Hanya beberapa kode (uji coba)
python scripts/sync_inverter_string_layout_from_gsheet.py --only "GM 1,CP MJL"

# Lihat jumlah baris tanpa menulis CSV
python scripts/sync_inverter_string_layout_from_gsheet.py --only "GM 1" --dry-run

# Offline: pakai snapshot terakhir di data/gsheet_exports/join_string_latest.csv
python scripts/sync_inverter_string_layout_from_gsheet.py --use-snapshot
```

## Memuat ke database

Tabel yang dipakai pipeline (mis. `cp_pr_string_audit`) adalah `staging.seed_inverter_string_layout`. Setelah CSV di sini di-update, jalankan **dbt seed** pada project dbt yang mengarah ke database MMSR yang sama, dengan file seed ini (salin ke `seed-paths` project dbt Anda atau ganti file seed di project utama).

Tanpa `dbt seed`, Postgres tetap berisi data lama (mis. Pusan masih 10 inverter).

## Sinkron ke `dimensions.dim_inverter_string_layout`

Setelah `dbt seed` memuat `staging.seed_inverter_string_layout`, jalankan:

```bash
python scripts/refresh_dim_inverter_string_layout_from_seed.py --all-gsheet-sites
```

Atau satu site: `--site-id 1458125` (boleh diulang). Skrip menghapus baris dim untuk site itu lalu mengisi ulang dari staging, dengan mempertahankan kolom inverter/PV dari baris dim lama bila kunci (inverter_no, mppt_no, string_no) cocok.

## Backfill inverter_id dari dim_assets + export CSV seed

Untuk isi `staging.seed_inverter_config.inverter_id` berdasarkan telemetry key yang sudah muncul di
`mart.mart_string_performance_daily`, jalankan:

```bash
python scripts/backfill_seed_inverter_id_from_dim_assets.py --days 120
```

Jika ingin langsung sinkron file seed untuk workflow `dbt seed`, gunakan:

```bash
python scripts/backfill_seed_inverter_id_from_dim_assets.py --days 120 --export-csv
```

File CSV hasil export default:
`dbt/seeds/seed_inverter_config.csv`