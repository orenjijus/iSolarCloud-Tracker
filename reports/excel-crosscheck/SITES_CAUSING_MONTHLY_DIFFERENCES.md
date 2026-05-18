# Site-Site yang Menyebabkan Perbedaan Agregasi Bulanan

**Tanggal**: 2025-01-XX  
**Sumber**: Perbandingan antara `mart_site_performance_monthly` (DB) vs `site_monthly_performance_excel` (Excel)

## Ringkasan Eksekutif

Dari analisis perbandingan agregasi bulanan tahun 2025, ditemukan bahwa perbedaan total agregasi disebabkan oleh:

### Kategori Masalah:
1. **Site yang Missing di Excel** - Site yang ada di database tapi tidak ada di Excel
2. **Site dengan Perbedaan Nilai** - Site yang ada di kedua source tapi nilainya berbeda

### Top 5 Site dengan Impact Terbesar:

| Rank | Site | Status | Total Impact | Bulan Terpengaruh | Keterangan |
|------|------|--------|--------------|-------------------|------------|
| 1 | **PT. MMKI 5.7 MWp - Phase 2** | HAS_DIFFERENCES | **-340.74 MWh** | 9 bulan | DB lebih kecil, perlu investigasi |
| 2 | **PT. MMKI 4.292 MWP - Phase 3** | MISSING_IN_EXCEL | **+283.56 MWh** | 6 bulan | Missing di Excel Jan-Jun |
| 3 | **PLTS Rooftop Sumatera Prima** | MISSING_IN_EXCEL | **+273.05 MWh** | 1 bulan | Missing di Oktober |
| 4 | **Charoen Pokphand Sites** (3 sites) | MISSING_IN_EXCEL | **+283.17 MWh** | 2 bulan | Missing di Juli-Agustus |
| 5 | **Shoetown Ligung Indonesia** | HAS_DIFFERENCES | **-34.32 MWh** | 3 bulan | DB lebih kecil |

### Ringkasan Statistik:
- **Total site dengan masalah**: 18 sites
- **Site missing di Excel**: 6 sites (berbeda per bulan)
- **Site dengan perbedaan nilai**: 12 sites
- **Bulan dengan masalah terbanyak**: Februari 2025 (7 sites)
- **Bulan dengan impact terbesar**: 
  - Agustus 2025: +276.30 MWh (karena 5 site missing)
  - Juni 2025: +283.65 MWh (karena 1 site missing besar)
  - Oktober 2025: +275.04 MWh (karena 1 site missing besar)

## Detail per Bulan

### November 2025
- **Total Perbedaan**: +2.05 MWh (Energy), +1.50 kWh/m² (GHI)
- **Site dengan Masalah**: 3 sites
- **Status**: Perbedaan kecil, sebagian besar site sudah match

#### Site dengan Perbedaan:
1. **PT. MMKI 1.75 MWp - Painting Building** (FS_SITE_NE=50488260)
   - Status: HAS_DIFFERENCES
   - Energy: DB 119.02 MWh vs Excel 118.04 MWh (diff: +0.98 MWh, +0.83%)
   - Impact: Kecil

2. **PT. MMKI 5.7 MWp - Phase 2** (FS_SITE_NE=51758766)
   - Status: HAS_DIFFERENCES
   - Energy: DB 378.07 MWh vs Excel 376.86 MWh (diff: +1.21 MWh, +0.32%)
   - Impact: Kecil

3. **PLTS Rooftop Sumatera Prima Fibreboard** (ISO_SITE_1680199)
   - Status: HAS_DIFFERENCES
   - Energy: DB 215.85 MWh vs Excel 216.00 MWh (diff: -0.14 MWh, -0.07%)
   - Impact: Sangat kecil

---

### Oktober 2025 ⚠️
- **Total Perbedaan**: +275.04 MWh (Energy), +119.25 kWh/m² (GHI)
- **Site dengan Masalah**: 4 sites (1 missing, 3 differences)
- **Status**: Perbedaan besar, terutama karena 1 site missing

#### Site yang Missing di Excel:
1. **PLTS Rooftop Sumatera Prima Fibreboard** (ISO_SITE_1680199)
   - Status: MISSING_IN_EXCEL
   - Energy: DB 273.05 MWh (tidak ada di Excel)
   - GHI: DB 137.55 kWh/m² (tidak ada di Excel)
   - **Impact: SANGAT BESAR** - Menyebabkan +273.05 MWh perbedaan

#### Site dengan Perbedaan:
1. **Shoetown Ligung Indonesia** (ISO_SITE_1479456)
   - Status: HAS_DIFFERENCES
   - Energy: DB 325.88 MWh vs Excel 323.27 MWh (diff: +2.61 MWh, +0.81%)
   - Impact: Kecil

2. **Garuda Metalindo (IKP)** (ISO_SITE_1445767)
   - Status: HAS_DIFFERENCES
   - Energy: DB 37.63 MWh vs Excel 38.86 MWh (diff: -1.22 MWh, -3.15%)
   - Impact: Kecil

3. **PT. MMKI 5.7 MWp - Phase 2** (FS_SITE_NE=51758766)
   - Status: HAS_DIFFERENCES
   - Energy: DB 656.23 MWh vs Excel 655.63 MWh (diff: +0.60 MWh, +0.09%)
   - Impact: Sangat kecil

---

### September 2025
- **Total Perbedaan**: +12.50 MWh (Energy), +11.04 kWh/m² (GHI)
- **Site dengan Masalah**: 4 sites (1 missing, 3 differences)
- **Status**: Perbedaan kecil

#### Site yang Missing di Excel:
1. **PLTS Rooftop Sumatera Prima Fibreboard** (ISO_SITE_1680199)
   - Status: MISSING_IN_EXCEL
   - Energy: DB ~215 MWh (estimasi, tidak ada di Excel)
   - **Impact**: Menyebabkan sebagian besar perbedaan

---

### Agustus 2025 ⚠️⚠️ PERBEDAAN TERBESAR
- **Total Perbedaan**: +276.30 MWh (Energy), +591.09 kWh/m² (GHI)
- **Site dengan Masalah**: 7 sites (5 missing, 2 differences)
- **Status**: Perbedaan sangat besar, terutama karena banyak site missing

#### Site yang Missing di Excel (5 sites):
1. **Charoen Pokphand Majalengka** (ISO_SITE_1614122)
   - Status: MISSING_IN_EXCEL
   - Energy: DB 106.53 MWh (tidak ada di Excel)
   - GHI: DB 173.27 kWh/m² (tidak ada di Excel)
   - **Impact: BESAR** - Menyebabkan +106.53 MWh perbedaan

2. **PLTS Frina Lestari Nusantara** (ISO_SITE_1628909)
   - Status: MISSING_IN_EXCEL
   - Energy: DB 71.23 MWh (tidak ada di Excel)
   - GHI: DB 141.50 kWh/m² (tidak ada di Excel)
   - **Impact: BESAR** - Menyebabkan +71.23 MWh perbedaan

3. **Charoen Pokphand Bandung** (ISO_SITE_1637095)
   - Status: MISSING_IN_EXCEL
   - Energy: DB 69.34 MWh (tidak ada di Excel)
   - GHI: DB 146.06 kWh/m² (tidak ada di Excel)
   - **Impact: BESAR** - Menyebabkan +69.34 MWh perbedaan

4. **Charoen Pokphand Madiun** (ISO_SITE_1637816)
   - Status: MISSING_IN_EXCEL
   - Energy: DB 31.80 MWh (tidak ada di Excel)
   - GHI: DB 63.16 kWh/m² (tidak ada di Excel)
   - **Impact: SEDANG** - Menyebabkan +31.80 MWh perbedaan

5. **PT Gelora Djaja 1 MWp** (FS_SITE_NE=61847068)
   - Status: MISSING_IN_EXCEL
   - Energy: DB 0.00 MWh (tidak ada di Excel)
   - GHI: DB 0.00 kWh/m² (tidak ada di Excel)
   - **Impact: TIDAK ADA** - Site tidak aktif

#### Site dengan Perbedaan:
1. **PT. MMKI 4.292 MWP - Phase 3** (FS_SITE_NE=58630782)
   - Status: HAS_DIFFERENCES
   - Energy: DB 488.71 MWh vs Excel 491.45 MWh (diff: -2.74 MWh, -0.56%)
   - Impact: Kecil

2. **PT. MMKI 5.7 MWp - Phase 2** (FS_SITE_NE=51758766)
   - Status: HAS_DIFFERENCES
   - Energy: DB 645.57 MWh vs Excel 645.44 MWh (diff: +0.13 MWh, +0.02%)
   - Impact: Sangat kecil

**Total Impact dari Site Missing**: +278.90 MWh (hampir semua perbedaan total)

---

### Juli 2025
- **Total Perbedaan**: -9.04 MWh (Energy), +237.23 kWh/m² (GHI)
- **Site dengan Masalah**: 5 sites (3 missing, 2 differences)
- **Status**: Perbedaan sedang, terutama di GHI

#### Site yang Missing di Excel (3 sites):
1. **PLTS Rooftop Sumatera Prima Fibreboard** (ISO_SITE_1680199)
   - Status: MISSING_IN_EXCEL
   - Energy: DB ~215 MWh (estimasi)
   - **Impact**: Menyebabkan sebagian besar perbedaan

2. **Charoen Pokphand sites** (2 sites)
   - Status: MISSING_IN_EXCEL
   - **Impact**: Menyebabkan perbedaan tambahan

---

### Juni 2025 ⚠️
- **Total Perbedaan**: +283.65 MWh (Energy), +119.82 kWh/m² (GHI)
- **Site dengan Masalah**: 2 sites (1 missing, 1 difference)
- **Status**: Perbedaan besar karena 1 site missing

#### Site yang Missing di Excel:
1. **PT. MMKI 4.292 MWP - Phase 3** (FS_SITE_NE=58630782)
   - Status: MISSING_IN_EXCEL
   - Energy: DB 283.56 MWh (tidak ada di Excel)
   - GHI: DB 118.05 kWh/m² (tidak ada di Excel)
   - **Impact: SANGAT BESAR** - Menyebabkan hampir semua perbedaan (+283.56 MWh)

#### Site dengan Perbedaan:
1. **PT. MMKI 5.7 MWp - Phase 2** (FS_SITE_NE=51758766)
   - Status: HAS_DIFFERENCES
   - Energy: DB 531.36 MWh vs Excel 531.27 MWh (diff: +0.09 MWh, +0.02%)
   - Impact: Sangat kecil

---

### Mei 2025
- **Total Perbedaan**: -131.22 MWh (Energy), +91.08 kWh/m² (GHI)
- **Site dengan Masalah**: 3 sites (1 missing, 2 differences)
- **Status**: Perbedaan sedang

#### Site yang Missing di Excel:
1. **PLTS Rooftop Sumatera Prima Fibreboard** (ISO_SITE_1680199)
   - Status: MISSING_IN_EXCEL
   - **Impact**: Menyebabkan sebagian besar perbedaan

---

### April 2025
- **Total Perbedaan**: -15.49 MWh (Energy), +139.25 kWh/m² (GHI)
- **Site dengan Masalah**: 2 sites (1 missing, 1 difference)
- **Status**: Perbedaan kecil

---

### Maret 2025
- **Total Perbedaan**: -20.15 MWh (Energy), +190.32 kWh/m² (GHI)
- **Site dengan Masalah**: 4 sites (1 missing, 3 differences)
- **Status**: Perbedaan kecil di energy, sedang di GHI

---

### Februari 2025 ⚠️
- **Total Perbedaan**: -210.24 MWh (Energy), +116.14 kWh/m² (GHI)
- **Site dengan Masalah**: 7 sites (1 missing, 6 differences)
- **Status**: Perbedaan besar, terutama karena 1 site dengan perbedaan sangat besar

#### Site dengan Perbedaan Besar:
1. **PT. MMKI 5.7 MWp - Phase 2** (FS_SITE_NE=51758766)
   - Status: HAS_DIFFERENCES
   - Energy: DB 278.76 MWh vs Excel 472.49 MWh (diff: -193.73 MWh, -41.00%)
   - **Impact: SANGAT BESAR** - Menyebabkan hampir semua perbedaan (-193.73 MWh)
   - **Catatan**: Perbedaan sangat besar, perlu investigasi lebih lanjut

#### Site dengan Perbedaan Kecil:
2. **Shoetown Ligung Indonesia** (ISO_SITE_1479456)
   - Energy: DB 253.06 MWh vs Excel 262.09 MWh (diff: -9.03 MWh, -3.45%)

3. **Garuda Metalindo 1** (ISO_SITE_1458125)
   - Energy: DB 61.88 MWh vs Excel 64.67 MWh (diff: -2.79 MWh, -4.32%)

4. **Garuda Metalindo 2** (ISO_SITE_1453245)
   - Energy: DB 58.98 MWh vs Excel 61.12 MWh (diff: -2.14 MWh, -3.49%)

5. **Garuda Metalindo (IKP)** (ISO_SITE_1445767)
   - Energy: DB 27.12 MWh vs Excel 28.40 MWh (diff: -1.28 MWh, -4.50%)

6. **Garuda Metalindo (MPF)** (ISO_SITE_1449886)
   - Energy: DB 34.42 MWh vs Excel 35.70 MWh (diff: -1.28 MWh, -3.57%)

---

### Januari 2025
- **Total Perbedaan**: -44.49 MWh (Energy), +175.98 kWh/m² (GHI)
- **Site dengan Masalah**: 6 sites (1 missing, 5 differences)
- **Status**: Perbedaan sedang

---

## Temuan Utama

### 1. Site yang Sering Missing di Excel

Site-site berikut sering muncul di database tapi tidak ada di Excel:

1. **PT. MMKI 4.292 MWP - Phase 3** (FS_SITE_NE=58630782) ⚠️
   - **Missing di**: 6 bulan (Januari - Juni 2025)
   - **Total Impact**: +283.56 MWh
   - **Bulan terpengaruh**: 2025-01, 2025-02, 2025-03, 2025-04, 2025-05, 2025-06
   - **Status**: Site ini missing di Excel untuk 6 bulan pertama 2025

2. **PLTS Rooftop Sumatera Prima Fibreboard** (ISO_SITE_1680199) ⚠️
   - **Missing di**: 1 bulan (Oktober 2025)
   - **Total Impact**: +273.05 MWh (hanya di Oktober)
   - **Bulan terpengaruh**: 2025-10
   - **Status**: Missing di Oktober, tapi ada perbedaan kecil di November

3. **Charoen Pokphand Sites** (3 sites) ⚠️
   - **ISO_SITE_1614122** (Majalengka): Missing 2 bulan (Juli-Agustus), Impact +106.53 MWh
   - **ISO_SITE_1628909** (Frina Lestari Nusantara): Missing 2 bulan (Juli-Agustus), Impact +107.29 MWh
   - **ISO_SITE_1637095** (Bandung): Missing 2 bulan (Juli-Agustus), Impact +69.35 MWh
   - **Total Impact**: +283.17 MWh
   - **Bulan terpengaruh**: 2025-07, 2025-08
   - **Status**: Semua 3 site missing di Juli-Agustus, kemudian muncul di September

4. **Charoen Pokphand Madiun** (ISO_SITE_1637816)
   - **Missing di**: 1 bulan (Agustus 2025)
   - **Total Impact**: +31.80 MWh
   - **Bulan terpengaruh**: 2025-08

5. **PT Gelora Djaja 1 MWp** (FS_SITE_NE=61847068)
   - **Missing di**: 2 bulan (Agustus-September 2025)
   - **Total Impact**: 0.00 MWh (site tidak aktif)
   - **Bulan terpengaruh**: 2025-08, 2025-09

### 2. Site dengan Perbedaan Nilai Konsisten

1. **PT. MMKI 5.7 MWp - Phase 2** (FS_SITE_NE=51758766) ⚠️⚠️
   - **Perbedaan di**: 9 bulan (Februari - November 2025)
   - **Total Impact**: -340.74 MWh (negatif = DB lebih kecil dari Excel)
   - **Bulan terpengaruh**: 2025-02, 2025-03, 2025-05, 2025-06, 2025-07, 2025-08, 2025-09, 2025-10, 2025-11
   - **PENTING**: 
     - Perbedaan sangat besar di Februari 2025 (-193.73 MWh, -41%)
     - Perbedaan konsisten negatif di semua bulan (DB selalu lebih kecil)
   - **Perlu investigasi khusus**: Kemungkinan issue dates, meter reset, atau perhitungan berbeda

2. **Shoetown Ligung Indonesia** (ISO_SITE_1479456)
   - **Perbedaan di**: 3 bulan (Januari, Februari, Oktober 2025)
   - **Total Impact**: -34.32 MWh
   - **Bulan terpengaruh**: 2025-01, 2025-02, 2025-10
   - **Impact**: Sedang, perbedaan negatif konsisten

3. **Garuda Metalindo Sites** (5 sites)
   - **ISO_SITE_1445767** (IKP): 5 bulan, Total -6.86 MWh
   - **ISO_SITE_1449886** (MPF): 3 bulan, Total -3.59 MWh
   - **ISO_SITE_1458125** (Garuda Metalindo 1): 2 bulan, Total -8.34 MWh
   - **ISO_SITE_1453245** (Garuda Metalindo 2): 2 bulan, Total -8.29 MWh
   - **Total Impact**: -27.08 MWh (semua negatif)
   - **Impact**: Kecil, tapi konsisten negatif

### 3. Pola Perbedaan

- **Missing Sites**: Menyebabkan perbedaan positif (DB lebih besar)
- **Value Differences**: Bervariasi, bisa positif atau negatif
- **GHI Differences**: Umumnya DB lebih tinggi dibanding Excel

## Rekomendasi

### Prioritas Tinggi (Impact Besar)
1. **Tambahkan site yang missing ke Excel** (Semua site ini adalah site baru yang mulai muncul di database lebih awal):
   - **PT. MMKI 4.292 MWP - Phase 3** 
     - Missing: 6 bulan (Jan-Jun 2025), Impact: +283.56 MWh
     - Mulai di DB: April 2024, Mulai di Excel: Juli 2025
     - **Action**: Tambahkan data Jan-Jun 2025 ke Excel
   
   - **PLTS Rooftop Sumatera Prima Fibreboard**
     - Missing: 1 bulan (Oktober 2025), Impact: +273.05 MWh
     - Mulai di DB: Oktober 2025, Mulai di Excel: November 2025
     - **Action**: Tambahkan data Oktober 2025 ke Excel
   
   - **Charoen Pokphand Sites** (3 sites)
     - **Majalengka**: Missing 2 bulan (Juli-Agustus), Impact: +106.53 MWh
     - **Frina Lestari Nusantara**: Missing 2 bulan (Juli-Agustus), Impact: +107.29 MWh
     - **Bandung**: Missing 2 bulan (Juli-Agustus), Impact: +69.35 MWh
     - Mulai di DB: Juli 2025, Mulai di Excel: September 2025
     - **Action**: Tambahkan data Juli-Agustus 2025 ke Excel untuk ketiga site
   
   - **Charoen Pokphand Madiun**
     - Missing: 1 bulan (Agustus 2025), Impact: +31.80 MWh
     - Mulai di DB: Agustus 2025, Mulai di Excel: September 2025
     - **Action**: Tambahkan data Agustus 2025 ke Excel

2. **Investigasi PT. MMKI 5.7 MWp - Phase 2** (9 bulan, -340.74 MWh):
   - **PRIORITAS TINGGI**: Perbedaan sangat besar di Februari 2025 (-193.73 MWh, -41%)
   - Perbedaan konsisten negatif di 9 bulan (DB selalu lebih kecil dari Excel)
   - Kemungkinan penyebab:
     - Issue dates yang tidak di-handle dengan benar
     - Meter reset atau perhitungan cumulative yang berbeda
     - Data error atau missing data di database
     - Perbedaan logika perhitungan energy
   - **Action**: Cek data harian untuk Februari 2025, verifikasi issue dates, cek meter calculation

### Prioritas Sedang
3. **Verifikasi perhitungan untuk site dengan perbedaan kecil-kecil**:
   - Shoetown Ligung Indonesia
   - Garuda Metalindo sites

4. **Cek GHI calculation**:
   - DB umumnya lebih tinggi, perlu verifikasi sumber data

### Prioritas Rendah
5. **Site dengan perbedaan sangat kecil (< 0.1%)**:
   - Bisa diabaikan atau di-round untuk matching

## Query untuk Investigasi Lebih Lanjut

### 1. Cek data harian untuk site dengan perbedaan besar
```sql
-- Contoh: PT. MMKI 5.7 MWp - Phase 2 di Februari 2025
SELECT 
    date_key,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    pr_ghi_actual,
    is_issue_date
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE year = 2025 
    AND month = 2
    AND site_id = 'FS_SITE_NE=51758766'
ORDER BY date_key;
```

### 2. Cek kapan site mulai muncul di database
```sql
-- Cek kapan site missing mulai ada data
SELECT 
    site_id,
    site_name,
    MIN(date_key) as first_date,
    MAX(date_key) as last_date,
    COUNT(DISTINCT date_key) as days_with_data
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_id IN (
    'ISO_SITE_1680199',  -- PLTS Rooftop Sumatera Prima Fibreboard
    'ISO_SITE_1614122',  -- Charoen Pokphand Majalengka
    'ISO_SITE_1628909',  -- PLTS Frina Lestari Nusantara
    'ISO_SITE_1637095',  -- Charoen Pokphand Bandung
    'ISO_SITE_1637816'   -- Charoen Pokphand Madiun
)
GROUP BY site_id, site_name
ORDER BY first_date;
```

### 3. Export data untuk site missing ke Excel
```sql
-- Export data bulanan untuk site yang missing
SELECT 
    year,
    month,
    site_id,
    site_name,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    pr_ghi_actual,
    energy_target_mwh,
    ghi_target
FROM "MMSR"."mart"."mart_site_performance_monthly"
WHERE site_id IN (
    'ISO_SITE_1680199',
    'ISO_SITE_1614122',
    'ISO_SITE_1628909',
    'ISO_SITE_1637095',
    'ISO_SITE_1637816'
)
AND year = 2025
ORDER BY site_id, year, month;
```

