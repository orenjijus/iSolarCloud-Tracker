# Panduan Perbandingan Data Bulanan: Database vs Excel

## Ringkasan

File ini menjelaskan cara melakukan perbandingan data bulanan antara database dan Excel untuk mendeteksi perbedaan secara sistematis.

## Struktur Data

### 1. View Agregasi Bulanan dari Database
**File**: `dbt/models/marts/mart_site_performance_monthly.sql`

View ini mengagregasi data harian (`mart_site_performance_daily`) menjadi data bulanan dengan:
- **Sum agregasi**: Energy, GHI, POA, Target values
- **PR Calculation**: Dihitung dari **sum bulanan**, BUKAN rata-rata PR harian
  - PR GHI = (SUM(energy_mwh) * 1000) / (SUM(ghi_kwh_m2)) / capacity_kw
  - PR POA = (SUM(energy_mwh) * 1000) / (SUM(poa_kwh_m2)) / capacity_kw

**Cara menggunakan**:
```sql
-- Compile dan run view terlebih dahulu
dbt run --select mart_site_performance_monthly

-- Atau query langsung
SELECT * FROM "MMSR"."mart"."mart_site_performance_monthly"
WHERE year = 2025 AND month = 11
ORDER BY site_id;
```

### 2. Tabel Excel Bulanan
**Tabel**: `public.site_monthly_performance_excel`

Tabel ini berisi data bulanan yang sudah diagregasi dari Excel. Kolom-kolom utama:
- `year`, `month`, `site_id`, `site_name`
- `daily_energy_mwh` (sebenarnya adalah sum bulanan)
- `daily_ghi_kwh_m2` (sebenarnya adalah sum bulanan)
- `pr_ghi_actual` (dihitung dari sum bulanan)
- `energy_target_mwh`, `ghi_target`
- `energy_actual_vs_target_pct`, `ghi_actual_vs_target_pct`
- `energy_vs_ghi_variance_pct`

## Query Perbandingan

### 1. Perbandingan Detail per Site
**File**: `dbt/analyses/compare_monthly_excel_vs_db.sql`

Query ini membandingkan data bulanan per site antara database dan Excel, menampilkan:
- Nilai dari database dan Excel side-by-side
- Selisih absolut dan persentase
- Flag untuk perbedaan yang signifikan (> 0.1% atau > 0.01 MWh)

**Cara menggunakan**:
```sql
-- Compile query
dbt compile --select compare_monthly_excel_vs_db

-- Atau query langsung (setelah view dibuat)
-- Lihat file: dbt/target/compiled/mmsr_solar_data/analyses/compare_monthly_excel_vs_db.sql
```

**Output yang berguna**:
- `has_significant_energy_diff`: TRUE jika ada perbedaan signifikan pada energy
- `has_significant_pr_diff`: TRUE jika ada perbedaan signifikan pada PR
- `data_status`: 'MISSING_IN_DB', 'MISSING_IN_EXCEL', atau 'BOTH_EXIST'

### 2. Ringkasan Total Bulanan
**File**: `dbt/analyses/compare_monthly_summary.sql`

Query ini menampilkan ringkasan agregasi total per bulan untuk:
- Total energy semua site
- Total GHI semua site
- Jumlah site
- Rata-rata PR

**Cara menggunakan**:
```sql
-- Compile query
dbt compile --select compare_monthly_summary

-- Atau query langsung
-- Lihat file: dbt/target/compiled/mmsr_solar_data/analyses/compare_monthly_summary.sql
```

**Output yang berguna**:
- `comparison_status`: 'MATCH', 'HAS_DIFFERENCES', 'MISSING_IN_DB', atau 'MISSING_IN_EXCEL'
- `energy_diff_pct`: Persentase perbedaan total energy
- `ghi_diff_pct`: Persentase perbedaan total GHI

## Workflow Pengecekan

### Step 1: Buat View Agregasi Bulanan
```bash
dbt run --select mart_site_performance_monthly
```

### Step 2: Cek Ringkasan Total Bulanan
Jalankan query `compare_monthly_summary` untuk melihat apakah ada perbedaan di level agregasi total:
- Jika total energy berbeda → ada masalah di agregasi atau data source
- Jika total GHI berbeda → ada masalah di sensor data atau agregasi
- Jika site count berbeda → ada site yang missing di salah satu source

### Step 3: Drill Down ke Site Level
Jika ada perbedaan di level total, jalankan query `compare_monthly_excel_vs_db` untuk melihat:
- Site mana yang berbeda
- Metrik mana yang berbeda (energy, GHI, PR)
- Besarnya perbedaan

### Step 4: Drill Down ke Daily Level
Jika sudah ketemu site yang berbeda, cek data harian untuk bulan tersebut:
```sql
SELECT 
    date_key,
    site_id,
    site_name,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    pr_ghi_actual
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE year = 2025 
    AND month = 11
    AND site_id = 'FS_SITE_NE=50488260'  -- Ganti dengan site_id yang bermasalah
ORDER BY date_key;
```

## Catatan Penting

1. **PR Calculation**: PR dihitung dari sum bulanan, bukan rata-rata PR harian. Ini sesuai dengan metodologi Excel.

2. **Naming Convention**: Excel menggunakan nama kolom `daily_energy_mwh` dan `daily_ghi_kwh_m2` meskipun ini adalah data bulanan. View database mengikuti naming ini untuk memudahkan perbandingan.

3. **Precision**: Semua nilai menggunakan `DECIMAL(18,6)` untuk konsistensi dengan Excel.

4. **Threshold**: 
   - Energy difference signifikan: > 0.01 MWh atau > 0.1%
   - PR difference signifikan: > 0.001 (0.1%)
   - GHI minimum threshold: 0.1 kWh/m²

## Troubleshooting

### Jika view tidak bisa dibuat:
1. Pastikan `mart_site_performance_daily` sudah ada dan berisi data
2. Cek apakah ada error di dbt logs
3. Pastikan schema `mart` ada dan accessible

### Jika perbandingan menunjukkan banyak perbedaan:
1. Cek apakah data Excel sudah di-update dengan benar
2. Cek apakah ada filter atau kondisi yang berbeda antara Excel dan database
3. Cek apakah ada issue dates yang perlu di-exclude
4. Verifikasi perhitungan PR di Excel apakah benar-benar dari sum bulanan

### Jika ada site yang missing:
1. Cek apakah site_id di Excel dan database sama persis (case-sensitive)
2. Cek apakah ada site yang tidak ada data di salah satu source
3. Cek apakah ada filter berdasarkan date range yang berbeda

