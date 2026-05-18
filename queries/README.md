# Queries

Kumpulan query SQL dan berkas terkait, dikelompokkan per topik.

## Struktur folder

| Folder | Isi |
|--------|-----|
| **eda/** | Exploratory Data Analysis: overview device 5min, site daily, missing stats, numeric summary, dim_assets join, issue dates. Untuk notebook/ML. |
| **cross_check/** | Cross-check harian, side-by-side, baseline MMKI, ringkasan kecuali MMKI. |
| **compare_monthly/** | Perbandingan bulanan per site, side-by-side, nilai langsung. |
| **shoetown/** | Semua terkait site Shoetown: check devices, sensor/grid dates, compare vs Excel, export, validasi POA & mapping. |
| **poa/** | POA (Plane of Array): sensor per site, investigasi kalkulasi/outlier, systematic check, export & validasi vs Excel. |
| **mmki/** | MMKI: simulasi bulanan phase 3, target monthly MMKI3, GHI goodness of fit, verifikasi MMKI2 POA config/fix. |
| **diagnostics/** | Diagnosa (mis. 2025-12-15, FusionSolar tidak update), investigasi (Madiun meter, site tertentu), fix notes. |
| **analysis/** | Analisis kondisi data staging→mart, perbedaan detail, pattern analysis, output teks. |
| **checks/** | Cek umum: Garuda IKP raw, mart_sensor_daily. |
| **reference/** | Referensi kapasitas device, dll. |
| **validation/** | Verifikasi perhitungan (mis. energy satu hari). |

## Cara pakai

- Jalankan query di folder yang sesuai topik (schema `MMSR` / staging / mart sesuai komentar di file).
- Beberapa query dirancang untuk di-load dari Python (`pd.read_sql`) di notebook EDA.
