# MMKI GHI Goodness-of-Fit Analysis

## Tujuan Analisis

Menganalisis goodness-of-fit antara GHI aktual (dari sensor) dengan GHI simulasi dari tiga fase MMKI untuk menentukan baseline irradiance yang paling representatif untuk seluruh site.

## Konteks

- **PLTS MMKI**: ±11 MW total, dibangun bertahap
- **Phase 1**: PT. MMKI 1.75 MWp - Painting Building
- **Phase 2**: PT. MMKI 5.7 MWp - Phase 2
- **Phase 3**: PT. MMKI 4.292 MWP - Phase 3

### Karakteristik Data

1. **GHI Aktual**: 
   - Satu sensor GHI di lokasi yang sama
   - Data aktual sama untuk semua phase
   - Diambil dari `ghi_adjusted` Phase 1 (PT. MMKI 1.75 MWp - Painting Building)

2. **GHI Simulasi**:
   - Setiap phase punya hasil simulasi PVSyst berbeda
   - Perbedaan disebabkan oleh:
     * Beda waktu SolarGIS
     * Beda asumsi input
     * Beda timeline desain

## Query File

File: `queries/mmki_ghi_goodness_of_fit_analysis.sql`

Query ini terdiri dari 4 bagian:

### STEP 1: Data Harian - GHI Aktual vs Simulasi per Phase

Menampilkan data harian dengan:
- GHI aktual dan simulasi untuk setiap phase
- Residual (error) untuk setiap phase
- Absolute error dan percentage error

**Output**: Data harian Januari - November 2025

### STEP 2: Goodness-of-Fit Metrics per Phase

Menghitung metrik statistik untuk evaluasi model:

**Metrik yang Dihitung:**
- **Sample Size (n)**: Jumlah data points yang dibandingkan
- **Mean GHI Actual**: Rata-rata GHI aktual
- **Mean GHI Simulasi**: Rata-rata GHI simulasi
- **Mean Bias**: Rata-rata residual (bias sistemik)
- **StdDev Residual**: Standar deviasi residual
- **RMSE** (Root Mean Square Error): Mengukur akurasi prediksi
- **MAE** (Mean Absolute Error): Rata-rata absolute error
- **MAPE** (Mean Absolute Percentage Error): Rata-rata percentage error
- **R²** (Coefficient of Determination): Proporsi variasi yang dijelaskan model
- **Correlation**: Koefisien korelasi Pearson

**Interpretasi Metrik:**
- **RMSE & MAE**: Semakin kecil semakin baik (akurasi tinggi)
- **MAPE**: Semakin kecil semakin baik (error persentase rendah)
- **R²**: Semakin mendekati 1 semakin baik (model menjelaskan lebih banyak variasi)
- **Correlation**: Semakin mendekati 1 atau -1 semakin baik (korelasi kuat)
- **Mean Bias**: Mendekati 0 berarti tidak ada bias sistemik

### STEP 3: Agregasi Bulanan - Summary per Bulan

Menampilkan agregasi bulanan untuk melihat:
- Total GHI bulanan (aktual vs simulasi)
- Rata-rata GHI harian (aktual vs simulasi)
- Selisih absolut dan persentase per bulan

**Kegunaan**: 
- Melihat pola musiman
- Identifikasi bulan dengan perbedaan besar
- Evaluasi konsistensi sepanjang tahun

### STEP 4: Rekomendasi Baseline (Summary)

Ringkasan dengan ranking untuk membantu menentukan baseline terbaik:
- Menampilkan semua metrik kunci
- Menandai phase dengan metrik terbaik (✓ Best RMSE, ✓ Best MAE, dll)
- Sorting berdasarkan overall fit score (weighted combination)

**Overall Fit Score Formula:**
```
Score = (R² × 0.4) + (Correlation × 0.3) - (RMSE/10 × 0.2) - (MAE/10 × 0.1)
```

Phase dengan score tertinggi = baseline terbaik

## Cara Menggunakan

1. **Jalankan STEP 1** untuk melihat data detail harian
2. **Jalankan STEP 2** untuk mendapatkan metrik goodness-of-fit
3. **Jalankan STEP 3** untuk melihat pola bulanan
4. **Jalankan STEP 4** untuk rekomendasi baseline

## Rekomendasi Interpretasi

### Kriteria Baseline Terbaik

1. **R² tinggi** (> 0.9): Model menjelaskan >90% variasi
2. **Correlation tinggi** (> 0.95): Korelasi kuat dengan aktual
3. **RMSE rendah**: Error prediksi kecil
4. **MAE rendah**: Rata-rata error kecil
5. **Mean Bias mendekati 0**: Tidak ada bias sistemik

### Pertimbangan Tambahan

- **Konsistensi bulanan**: Phase dengan perbedaan bulanan paling kecil
- **Coverage data**: Phase dengan data paling lengkap (tidak banyak missing)
- **Kontekstual**: Pertimbangkan waktu SolarGIS dan asumsi input yang digunakan

## Output yang Diharapkan

Setelah menjalankan query, Anda akan mendapatkan:

1. **Data harian** untuk analisis detail dan visualisasi
2. **Tabel metrik** yang membandingkan ketiga phase
3. **Agregasi bulanan** untuk melihat pola
4. **Rekomendasi** phase mana yang paling cocok sebagai baseline

## Catatan Penting

- Data GHI aktual diambil dari `ghi_adjusted` Phase 1
- Periode analisis: Januari - November 2025
- Query menggunakan INNER JOIN, jadi hanya tanggal yang ada di kedua tabel (aktual dan simulasi) yang akan muncul
- Jika ada missing data di salah satu phase, tanggal tersebut akan di-exclude dari analisis phase tersebut

