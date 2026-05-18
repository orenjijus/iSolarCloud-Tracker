# EDA PLTS dengan Solar Data Tools, PVAnalytics, dan pvlib-python

EDA untuk data PLTS mengikuti praktik dari dokumentasi **Solar Data Tools**, **PVAnalytics**, dan **pvlib-python**: pipeline otomatis untuk power time series, quality control, feature labeling, dan (opsional) validasi irradiance dengan clearsky.

**Strategi: satu site dulu** — jalankan EDA untuk **satu site** (mis. PT. MMKI 1.75 MWp), cek hasil dan validasi metode/data. Setelah oke, baru implement ke site lain (ganti `EDA_SITE` atau loop).

---

## Referensi Dokumentasi

| Library | URL | Peran |
|--------|-----|--------|
| **Solar Data Tools** | https://solar-data-tools.readthedocs.io/ | DataHandler: I/O, cleaning, filtering, plotting; pipeline preprocessing + quality metrics (clear days, capacity change, clipping, time shift). |
| **PVAnalytics** | https://pvanalytics.readthedocs.io/ | Quality (gaps, outliers, irradiance limits, time spacing), features (daytime, clearsky, clipping), metrics (performance ratio, variability index). |
| **pvlib-python** | https://pvlib-python.readthedocs.io/ | Simulasi PV, clearsky irradiance untuk validasi; nomenclature standar. |

---

## Alur EDA yang Dipakai

1. **Power time series**  
   Input: deret waktu daya AC (mis. `inv_active_power` dari `mart.mart_inverter_performance_5min`) per site, resolusi 5 menit. DataFrame dengan **DatetimeIndex** dan kolom power (mis. `ac_power`), timezone lokal (atau naive local).

2. **Solar Data Tools**  
   - `DataHandler(df)` → `run_pipeline(power_col="ac_power", fix_tz=True)`: preprocessing, cleaning (time shift), scoring (clear days, capacity change, clipping).  
   - Report: `dh.report()`.  
   - Plot: heatmap, daily energy, data quality scatter, capacity change, time shift, clipping (`plot_heatmap`, `plot_daily_energy`, `plot_data_quality_scatter`, dll.).

3. **PVAnalytics**  
   - **Quality**: `quality.gaps.completeness_score`, `quality.gaps.stale_values_*`, `quality.outliers.tukey` / `zscore`, `quality.irradiance.check_ghi_limits_qcrad` / `daily_insolation_limits` (jika ada GHI), `quality.time.spacing`.  
   - **Features**: `features.daytime.power_or_irradiance`, `features.clearsky.reno` (jika ada GHI + clearsky), `features.clipping.*`.  
   - **Metrics**: `metrics.performance_ratio_nrel`, `metrics.variability_index` (jika ada irradiance + clearsky).

4. **pvlib (opsional)**  
   - Lokasi dari `dimensions.dim_assets` (lat/lon) → `pvlib.location.Location` → `get_clearsky(times)` untuk GHI/POA.  
   - Hasil dipakai di PVAnalytics: `quality.irradiance.clearsky_limits`, `daily_insolation_limits`, atau `metrics.variability_index`.

---

## Implementasi di Repo

- **Script**: `eda/eda_plts_pv_stack.py`  
  - Load power time series dari DB (satu site, `inv_active_power` 5min, excl. Samator/Klinik).  
  - **Solar Data Tools** (jika terpasang): DataHandler → run_pipeline → report + plot (heatmap, daily energy, data quality scatter). Di Windows tanpa C compiler, `solar-data-tools` bisa gagal install; script tetap jalan dan hanya jalankan PVAnalytics.  
  - **PVAnalytics**: completeness_score, outliers (Tukey), daytime mask.  
  - Opsional: pvlib clearsky (butuh lat/lon).

- **Variabel lingkungan**:  
  - `PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` (koneksi DB).  
  - `EDA_SITE`: **satu site** untuk EDA (default: PT. MMKI 1.75 MWp - Painting Building). Setelah validasi oke, ganti untuk site lain atau loop.  
  - `EDA_DAYS`: jumlah hari data (default: 180).

- **Jalankan**: `python eda/eda_plts_pv_stack.py`  
  Output di `eda/output/`: Solar Data Tools (jika terpasang): `sdt_heatmap.png`, `sdt_daily_energy.png`, `sdt_data_quality_scatter.png`. **PVAnalytics**: `pvanalytics_summary.txt`, `pvanalytics_power_daytime.png`.

---

## Kesesuaian dengan Dokumentasi

- **Solar Data Tools**: Format input (DataFrame, DatetimeIndex, kolom power), `run_pipeline()`, dan method plot mengikuti [User Guide](https://solar-data-tools.readthedocs.io/en/stable/index_user_guide.html) dan [Quick Demo](https://solar-data-tools.readthedocs.io/en/stable/getting_started/notebooks/demo_default.html).  
- **PVAnalytics**: Quality (gaps, outliers) dan features (daytime) mengikuti [API Reference](https://pvanalytics.readthedocs.io/en/stable/api.html).  
- **pvlib**: Clearsky dan lokasi mengikuti [pvlib-python](https://pvlib-python.readthedocs.io/); opsional untuk tahap berikutnya (irradiance + variability index).
