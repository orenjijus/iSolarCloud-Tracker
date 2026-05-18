# Hasil Jalankan EDA (Run by Assistant)

Tanggal run: 2026-01-29. **Strategi: satu site dulu** (PT. MMKI 1.75 MWp - Painting Building). Exclude: Samator & Klinik.

---

## 1. Run Site Daily (run_eda.py) — Satu Site

**Site:** PT. MMKI 1.75 MWp - Painting Building  
**Filter:** EDA_SITE set → hanya site ini.

| Metrik | Nilai |
|--------|--------|
| Total baris | 626 (hari) |
| Jumlah site | 1 |
| Rentang tanggal | 2024-04-30 s/d 2026-01-28 |
| Avg daily_energy_mwh | 5.65 |
| Avg pr_ghi_actual | 0.69 |
| Avg availability_percent | 0.996 |

**Missing (top):** Kolom target/simulasi: `energy_target_mwh`, `ghi_target`, `poa_target`, `daily_pr_ghi_target`, `daily_pr_poa_target`, `energy_actual_vs_target_pct`, dll. (~263–264 null). Energy, GHI, availability, PR actual lengkap untuk site ini.

**Output plot:** `eda/output/eda_daily_energy_hist.png`, `eda_daily_energy_ts.png`, `eda_daily_corr.png`.

---

## 2. Run PV Stack (eda_plts_pv_stack.py) — Satu Site, 14 Hari

**Site:** PT. MMKI 1.75 MWp - Painting Building  
**Days:** 14 (power 5min).

| Metrik | Nilai |
|--------|--------|
| Baris (5min) | 2.246 |
| Rentang | 2026-01-15 05:05 s/d 2026-01-28 18:30 |
| Solar Data Tools | Tidak terpasang → lewat ke PVAnalytics |
| PVAnalytics completeness | NaN (completeness_score mengembalikan kosong untuk input 5min; mungkin butuh daily series) |
| PVAnalytics outliers (Tukey k=2) | 2.226 titik di-flag outlier |
| PVAnalytics daytime | 2.040 / 2.246 titik (91%) terdeteksi siang |

**Hasil PVAnalytics tersimpan di `eda/output/`:**
- **pvanalytics_summary.txt** — ringkasan teks: completeness, outlier count, daytime count.
- **pvanalytics_power_daytime.png** — plot power 5min + mask daytime (power_or_irradiance).

---

## 3. Analisis Hasil

### Site daily (PT. MMKI 1.75 MWp)

- **Energy:** Rata-rata 5.65 MWh/hari; rentang penuh 626 hari. Cocok untuk time-series dan regresi (energy vs GHI, PR).
- **PR GHI:** Rata-rata 0.69 (69%); availability sangat tinggi (99.6%). Untuk ML: target/feature energy, PR, availability siap; kolom target simulasi banyak null → pakai hanya untuk subset yang punya target atau sebagai feature opsional.
- **Plot:** Histogram energy menggambarkan distribusi harian; time series menunjukkan tren dan seasonality; heatmap korelasi menunjukkan hubungan energy–GHI–availability–PR.

### PV stack (power 5min, 14 hari)

- **Data power:** 2.246 titik 5min (≈12 titik/jam × 24 × 14 hari minus malam) — wajar. Daytime 91% konsisten dengan jam operasi PLTS.
- **Completeness:** PVAnalytics `completeness_score` mengembalikan kosong/NaN — kemungkinan API mengharapkan deret harian (satu nilai per hari) atau format lain. Untuk EDA 5min, kelengkapan bisa dihitung manual (mis. jumlah titik per hari vs ekspektasi 288 titik/hari).
- **Outliers (Tukey k=2):** Hampir semua titik (2.226) di-flag outlier karena daya AC punya banyak nilai nol (malam) dan distribusi sangat skew; Tukey IQR sensitif. Rekomendasi: (1) filter dulu daytime saja lalu jalankan outlier, atau (2) gunakan threshold/parameter lain (mis. z-score pada nilai > 0), atau (3) terima bahwa “outlier” di sini lebih ke pola siang/malam.

### Rekomendasi lanjutan

1. **Site daily:** Metode dan data satu site sudah konsisten. Bisa di-roll out ke site lain (ganti EDA_SITE atau loop) untuk bandingkan distribusi dan missing per site.
2. **PV stack 5min:** Untuk outlier, jalankan hanya pada subset daytime atau ubah parameter Tukey/z-score. Untuk completeness, hitung manual (titik per hari) atau gunakan PVAnalytics pada deret harian (agregat 5min → daily).
3. **Solar Data Tools:** Jika terpasang (mis. di lingkungan dengan C compiler), jalankan pipeline lengkap (heatmap, capacity change, time shift, clipping) untuk interpretasi kualitas data lebih dalam.

---

**Ringkasan:** EDA satu site (PT. MMKI 1.75 MWp) berjalan sukses. Site daily siap untuk validasi metode dan roll out ke site lain; PV stack 5min memberi gambaran daytime/outlier dengan catatan penyesuaian parameter atau subset data.
