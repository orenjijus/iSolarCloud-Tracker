# EDA Site PLTS untuk ML

Exploratory Data Analysis untuk **site daily** (`mart.mart_site_performance_daily`) dan **device 5min** (meter, sensor, inverter) per site sebelum pemodelan ML. **Exclude: Samator & Klinik.**

**Strategi: satu site dulu** — jalankan EDA untuk satu site, cek hasil & validasi metode/data; setelah oke baru implement ke site lain (set `EDA_SITE` atau loop).

## Setup

```bash
pip install -r eda/requirements.txt
```

## Koneksi Database

Gunakan **eda/.env** dengan variabel:

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

Script dan notebook akan memuat **eda/.env** dan memakai variabel tersebut (fallback ke `PGHOST`, `PGUSER`, … jika tidak ada). Jangan commit file `.env`.

Alternatif: ekspor `mart.mart_site_performance_daily` ke CSV lalu load dengan `pd.read_csv()`.

## Isi EDA

**Site daily (excl. Samator/Klinik):**
1. Data loading & shape, missing %
2. Univariate: distribusi energy, GHI, PR, availability; value counts site/system
3. Multivariate: korelasi, scatter, boxplot
4. Time series: trend, seasonality, gap
5. Outlier & data quality
6. Kesiapan ML: target/feature, split, encoding

**Device 5min (meter, sensor, inverter — excl. Samator/Klinik):**
7. Load daily aggregates dari 5min; univariate per device/metric; time series per site.

Ringkasan temuan SQL: [../docs/eda_site_plts_summary.md](../docs/eda_site_plts_summary.md).

---

## EDA ala Solar Data Tools + PVAnalytics + pvlib

Untuk EDA PLTS mengikuti dokumentasi **Solar Data Tools**, **PVAnalytics**, dan **pvlib-python** (power time series, quality control, feature labeling):

- **Script**: `eda/eda_plts_pv_stack.py` — load power 5min (inv_active_power) per site dari DB → Solar Data Tools DataHandler + run_pipeline → report & plot (heatmap, daily energy, data quality) → PVAnalytics (completeness, outliers, daytime).
- **Dokumentasi**: [../docs/EDA_PV_STACK.md](../docs/EDA_PV_STACK.md).
- **Jalankan (satu site dulu)**: `EDA_SITE="PT. MMKI 1.75 MWp - Painting Building" EDA_DAYS=180 python eda/eda_plts_pv_stack.py`. Setelah validasi oke, ganti `EDA_SITE` untuk site lain. Solar Data Tools opsional; di Windows tanpa C compiler bisa gagal install—script tetap jalan dengan PVAnalytics saja.
