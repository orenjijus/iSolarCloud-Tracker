# Panduan Query Investigasi Perbedaan Data Bulanan

**Tanggal**: 2025-01-XX  
**Tujuan**: Query-query untuk investigasi lebih lanjut tentang perbedaan antara database dan Excel

## Daftar Query Investigasi

### 1. Investigasi Detail Site dengan Perbedaan Besar

#### `investigate_mmki_phase2_february_2025.sql`
**Tujuan**: Investigasi detail PT. MMKI 5.7 MWp - Phase 2 di Februari 2025 (perbedaan -193.73 MWh, -41%)

**Cara menggunakan**:
```bash
dbt compile --select investigate_mmki_phase2_february_2025
# Kemudian jalankan query dari dbt/target/compiled/...
```

**Output**: 
- Perbandingan data harian antara DB dan Excel
- Flag issue dates
- Target values
- Status per hari (MISSING_IN_DB, MISSING_IN_EXCEL, HAS_DIFFERENCES, MATCH)

**Kegunaan**:
- Identifikasi hari-hari dengan perbedaan besar
- Cek apakah issue dates mempengaruhi perhitungan
- Verifikasi apakah ada data yang missing di salah satu source

---

### 2. Export Data Site yang Missing

#### `export_missing_sites_monthly_data.sql`
**Tujuan**: Export data bulanan untuk site yang missing di Excel, siap untuk ditambahkan ke Excel

**Cara menggunakan**:
```bash
dbt compile --select export_missing_sites_monthly_data
# Export hasil query ke CSV/Excel
```

**Output**: 
- Data bulanan lengkap untuk site yang missing
- Termasuk: energy, GHI, PR, targets, percentages
- Metadata: days_with_data, days_with_energy, days_with_ghi

**Site yang di-export**:
- PT. MMKI 4.292 MWP - Phase 3 (Jan-Jun 2025)
- PLTS Rooftop Sumatera Prima Fibreboard (Oktober 2025)
- Charoen Pokphand sites (Juli-Agustus 2025)
- Charoen Pokphand Madiun (Agustus 2025)

**Kegunaan**:
- Data siap untuk ditambahkan ke Excel
- Memastikan format data sesuai dengan Excel

---

### 3. Summary Table Site per Bulan

#### `compare_monthly_sites_summary_table.sql`
**Tujuan**: Tabel summary yang menunjukkan site mana yang bermasalah di bulan mana (format pivot-like)

**Cara menggunakan**:
```bash
dbt compile --select compare_monthly_sites_summary_table
```

**Output**: 
- Satu baris per site
- Kolom untuk setiap bulan (Jan-Nov)
- Status code: MISS (missing), DB+ (DB lebih besar), DB- (DB lebih kecil), OK (match)
- Total months with issues
- Total energy difference

**Kegunaan**:
- Overview cepat site yang bermasalah
- Identifikasi pola perbedaan
- Prioritisasi site yang perlu diperbaiki

**Contoh Output**:
```
site_id                    | Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | months_with_issues
---------------------------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------------------
FS_SITE_NE=51758766        | OK  | DB- | DB- | OK  | DB- | DB- | DB- | DB- | DB- | DB- | DB- | 9
FS_SITE_NE=58630782        | MISS| MISS| MISS| MISS| MISS| MISS| DB- | DB- | OK  | OK  | OK  | 8
```

---

### 4. Check Data Availability Timeline

#### `check_site_data_availability.sql`
**Tujuan**: Cek kapan site mulai muncul di database vs Excel, untuk memahami apakah site baru atau hanya missing dari Excel

**Cara menggunakan**:
```bash
dbt compile --select check_site_data_availability
```

**Output**: 
- First date/month di database
- First date/month di Excel
- Availability status (ONLY_IN_DB, ONLY_IN_EXCEL, DB_STARTS_EARLIER, etc.)
- List bulan yang missing di Excel untuk 2025

**Kegunaan**:
- Memahami apakah site benar-benar baru atau hanya belum di-update di Excel
- Identifikasi site yang perlu ditambahkan ke Excel
- Verifikasi timeline data availability

---

### 5. Query Detail Site yang Menyebabkan Perbedaan

#### `compare_monthly_sites_causing_differences.sql`
**Tujuan**: Detail lengkap semua site yang menyebabkan perbedaan, diurutkan berdasarkan impact

**Cara menggunakan**:
```bash
dbt compile --select compare_monthly_sites_causing_differences
```

**Output**: 
- Per site per bulan dengan perbedaan
- Energy, GHI, PR comparison
- Status (MISSING_IN_DB, MISSING_IN_EXCEL, HAS_DIFFERENCES)
- Impact score (energy_impact_mwh)
- Contribution to total difference

**Kegunaan**:
- Drill down ke detail per site
- Identifikasi site dengan impact terbesar
- Analisis root cause per site

---

### 6. Summary Per Bulan

#### `compare_monthly_differences_summary_by_month.sql`
**Tujuan**: Ringkasan per bulan: berapa site yang bermasalah, total impact, top contributing sites

**Cara menggunakan**:
```bash
dbt compile --select compare_monthly_differences_summary_by_month
```

**Output**: 
- Site count differences per bulan
- Total energy/GHI difference per bulan
- Top 5 contributing sites per bulan

**Kegunaan**:
- Overview per bulan
- Identifikasi bulan dengan masalah terbesar
- Tracking progress setelah perbaikan

---

## Workflow Investigasi

### Step 1: Identifikasi Masalah
1. Jalankan `compare_monthly_summary.sql` untuk melihat ringkasan total
2. Jalankan `compare_monthly_differences_summary_by_month.sql` untuk melihat bulan dengan masalah terbesar
3. Jalankan `compare_monthly_sites_summary_table.sql` untuk melihat site yang bermasalah

### Step 2: Investigasi Site dengan Impact Besar
1. Untuk site missing: Jalankan `export_missing_sites_monthly_data.sql` dan tambahkan ke Excel
2. Untuk site dengan perbedaan nilai: Jalankan `investigate_mmki_phase2_february_2025.sql` (atau query serupa untuk site lain)
3. Cek data availability dengan `check_site_data_availability.sql`

### Step 3: Drill Down ke Daily Level
1. Gunakan query investigasi detail untuk melihat data harian
2. Cek issue dates, meter calculation, sensor data
3. Verifikasi perhitungan PR, energy, GHI

### Step 4: Verifikasi Setelah Perbaikan
1. Re-run query summary untuk melihat apakah perbedaan berkurang
2. Cek site yang sudah diperbaiki apakah sudah match
3. Update dokumentasi dengan hasil perbaikan

---

## Tips Penggunaan

### Filter Query untuk Site/Bulan Tertentu
Tambahkan WHERE clause untuk fokus investigasi:
```sql
WHERE site_id = 'FS_SITE_NE=51758766'
AND year = 2025 AND month = 2
```

### Export Hasil ke CSV
```sql
-- Di psql atau client SQL
\copy (SELECT * FROM ...) TO 'output.csv' CSV HEADER;
```

### Bandingkan Sebelum dan Sesudah
Simpan hasil query sebelum perbaikan, kemudian bandingkan dengan hasil setelah perbaikan untuk verifikasi.

---

## Query Tambahan yang Berguna

### Cek Data Harian untuk Site Tertentu
```sql
SELECT 
    date_key,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    pr_ghi_actual,
    is_issue_date
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_id = 'SITE_ID'
    AND year = 2025
    AND month = 2
ORDER BY date_key;
```

### Cek Issue Dates untuk Site
```sql
SELECT 
    site_id,
    issue_date,
    is_active
FROM "MMSR"."staging"."seed_issue_dates"
WHERE site_id = 'SITE_ID'
    AND is_active = TRUE
ORDER BY issue_date;
```

### Cek Meter Data untuk Site
```sql
SELECT 
    date_key,
    asset_id,
    metric_name,
    MIN(metric_value) as min_value,
    MAX(metric_value) as max_value
FROM "MMSR"."mart"."mart_meter_performance_5min"
WHERE site_name = 'SITE_NAME'
    AND date_key BETWEEN '2025-02-01' AND '2025-02-28'
    AND metric_name IN ('positive_active_energy', 'negative_active_energy')
GROUP BY date_key, asset_id, metric_name
ORDER BY date_key, asset_id;
```

---

## Troubleshooting

### Query tidak mengembalikan hasil
- Cek apakah site_id benar (case-sensitive)
- Cek apakah data ada di tabel source
- Cek filter year/month

### Perbedaan tidak sesuai ekspektasi
- Verifikasi apakah query menggunakan view yang benar
- Cek apakah ada filter yang tidak diinginkan
- Bandingkan dengan query manual untuk verifikasi

### Query lambat
- Tambahkan index jika perlu
- Filter ke range tanggal yang lebih kecil
- Gunakan LIMIT untuk testing

