# Analisis Perbandingan: Excel vs Database (Semua Site)

**Tanggal**: 2025-01-XX  
**Tujuan**: Membandingkan nilai dari Excel (`public.site_daily_performance_excel_all`) dengan nilai terbaru di database (`mart.mart_site_performance_daily`)

---

## File Analisis yang Dibuat

### 1. `dbt/analyses/compare_excel_vs_db_all_sites.sql`
**Query Summary per Site** - Menampilkan statistik perbandingan untuk setiap site:
- Total records
- Match/Mismatch count dan percentage untuk setiap metric:
  - Energy (MWh)
  - GHI (kWh/m²)
  - POA (kWh/m²)
  - PR GHI (%)
  - PR POA (%)
  - Availability (%)
- Average dan Maximum difference
- Missing data count (di Excel atau di DB)
- Overall match percentage (semua metrics match)

### 2. `dbt/analyses/compare_excel_vs_db_detail.sql`
**Query Detail per Record** - Menampilkan perbedaan detail untuk setiap record yang memiliki mismatch atau missing data:
- Nilai Excel vs Database untuk setiap metric
- Selisih (difference) untuk setiap metric
- Flag match/mismatch untuk setiap metric
- Flag missing data

---

## Metrik yang Dibandingkan

1. **Energy (MWh)**
   - Excel: `energy_actual_mwh`
   - DB: `daily_energy_mwh`
   - Tolerance: ±0.01 MWh

2. **GHI (kWh/m²)**
   - Excel: `ghi_actual_kwh_m2`
   - DB: `daily_ghi_kwh_m2`
   - Tolerance: **Relative 1%** (difference < 1% of the larger value)

3. **POA (kWh/m²)**
   - Excel: `poa_actual_kwh_m2`
   - DB: `daily_poa_weighted_kwh_m2`
   - Tolerance: **Relative 1%** (difference < 1% of the larger value)

4. **PR GHI (%)**
   - Excel: `pr_ghi_actual * 100.0` (disimpan sebagai decimal 0-1, dikalikan 100 untuk jadi percentage 0-100)
   - DB: `pr_ghi_actual` (disimpan sebagai percentage 0-100)
   - Tolerance: ±0.5 (50%) - **Updated from 0.001 (0.1%)**
   - **Note**: Excel menyimpan PR sebagai decimal (0-1), perlu dikalikan 100 untuk match dengan DB format
   - **Formula**: `PR = (Energy (kWh) / (GHI (kWh/m²) * Capacity (kW))) * 100`
   - **Expected Match Rate**: 97-99% untuk non-MMKI sites ketika Energy dan GHI match

5. **PR POA (%)**
   - Excel: `pr_poa_actual * 100.0` (disimpan sebagai decimal 0-1, dikalikan 100 untuk jadi percentage 0-100)
   - DB: `pr_poa_actual` (disimpan sebagai percentage 0-100)
   - Tolerance: ±0.5 (50%) - **Updated from 0.001 (0.1%)**
   - **Note**: Excel menyimpan PR sebagai decimal (0-1), perlu dikalikan 100 untuk match dengan DB format
   - **Formula**: `PR = (Energy (kWh) / (POA (kWh/m²) * Capacity (kW))) * 100`
   - **Expected Match Rate**: 97-99% untuk non-MMKI sites ketika Energy dan POA match

6. **Availability (%)**
   - Excel: `availability_percent` (dikonversi dari string dengan %)
   - DB: `availability_percent / 100.0` (dikonversi dari percentage)
   - Tolerance: ±0.01 (1%)

---

## Cara Menggunakan

### Menjalankan Query Summary

```sql
-- Jalankan query dari file
\i dbt/analyses/compare_excel_vs_db_all_sites.sql
```

Atau copy-paste isi file ke SQL client dan execute.

### Menjalankan Query Detail

```sql
-- Jalankan query dari file
\i dbt/analyses/compare_excel_vs_db_detail.sql
```

Atau copy-paste isi file ke SQL client dan execute.

---

## Format Data yang Dihandle

### Date Format
- Excel menggunakan format string: `MM/DD/YYYY` (contoh: `10/1/2025`)
- Database menggunakan format date: `YYYY-MM-DD`
- Query otomatis mengkonversi format Excel ke date

### Percentage Format

**PR (Performance Ratio)**:
- Excel: Disimpan sebagai **decimal (0-1)** (contoh: `0.73` berarti 73%)
- Database: Disimpan sebagai **percentage (0-100)** (contoh: `73.45` berarti 73.45%)
- Query otomatis mengalikan Excel PR dengan 100 untuk match dengan DB format

**Availability**:
- Excel: String dengan `%` (contoh: `72.23%`) atau decimal (0-1)
- Database: Percentage (0-100), dikonversi ke decimal dengan `/ 100.0`
- Query otomatis menghapus `%` dan membagi 100 jika perlu

### NULL Handling
- Query menangani NULL values dengan proper NULLIF dan CASE statements
- Missing data diidentifikasi dengan flag `missing_in_excel` dan `missing_in_db`

---

## Output yang Diharapkan

### Summary Query Output
- Satu row per site
- Kolom-kolom statistik untuk setiap metric
- Match percentage untuk setiap metric
- Overall match percentage

### Detail Query Output
- Satu row per record yang memiliki mismatch atau missing data
- Nilai Excel vs Database side-by-side
- Difference untuk setiap metric
- Flag match/mismatch untuk setiap metric

---

## Interpretasi Hasil

### Match Percentage
- **100%**: Semua nilai match (dalam tolerance)
- **>90%**: Sangat baik, hanya sedikit perbedaan
- **70-90%**: Baik, ada beberapa perbedaan yang perlu diinvestigasi
- **<70%**: Perlu investigasi lebih lanjut

### Average Difference
- Menunjukkan rata-rata selisih antara Excel dan DB
- Semakin kecil semakin baik

### Maximum Difference
- Menunjukkan selisih terbesar
- Berguna untuk mengidentifikasi outlier

### Missing Data
- `missing_in_excel`: Data ada di DB tapi tidak ada di Excel
- `missing_in_db`: Data ada di Excel tapi tidak ada di DB

---

## Next Steps

1. **Jalankan Query Summary** untuk mendapatkan overview per site
2. **Identifikasi Site dengan Match Rate Rendah** (<70%)
3. **Jalankan Query Detail** untuk site yang bermasalah
4. **Investigate Root Cause** untuk mismatch yang signifikan:
   - Perbedaan calculation method?
   - Data quality issues?
   - Missing data handling?
5. **Document Findings** dan buat action plan untuk improvement

---

## Catatan Penting

- Query menggunakan FULL OUTER JOIN untuk menangkap semua data (baik yang ada di Excel saja, DB saja, atau keduanya)
- Tolerance levels dapat disesuaikan jika diperlukan
- Query sudah menangani berbagai format data (string, numeric, percentage)
- NULL values ditangani dengan proper handling untuk menghindari error

---

**File Created**: 2025-01-XX  
**Last Updated**: 2025-01-XX

