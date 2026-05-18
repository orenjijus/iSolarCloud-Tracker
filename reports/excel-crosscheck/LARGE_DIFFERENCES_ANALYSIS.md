# Analisis Large Differences: Excel vs Database

**Tanggal**: 2025-01-XX  
**Tujuan**: Mengidentifikasi tanggal-tanggal dengan perbedaan besar antara Excel dan Database untuk memudahkan investigasi anomaly

---

## File Query

### 1. `dbt/analyses/compare_excel_vs_db_large_differences.sql`
**Detail per Record** - Menampilkan semua record dengan large differences:
- Date, Site, Metric Type
- Excel Value vs Database Value
- Absolute Difference
- Relative Difference (%)
- Direction (Excel > DB atau Excel < DB)

### 2. `dbt/analyses/compare_excel_vs_db_large_differences_summary.sql`
**Summary per Site** - Menampilkan jumlah large differences per site dan metric type

---

## Threshold untuk Large Differences

### Energy (MWh)
- **Absolute**: > 0.1 MWh
- **Relative**: > 5% dari nilai terbesar

### GHI (kWh/m²)
- **Absolute**: > 0.5 kWh/m²
- **Relative**: > 1% dari nilai terbesar

### POA (kWh/m²)
- **Absolute**: > 0.5 kWh/m²
- **Relative**: > 1% dari nilai terbesar

### PR GHI (%)
- **Absolute**: > 0.01 (1%)
- **Relative**: > 5% dari nilai terbesar

### PR POA (%)
- **Absolute**: > 0.01 (1%)
- **Relative**: > 5% dari nilai terbesar

### Availability (%)
- **Absolute**: > 0.05 (5%)
- **Relative**: > 5% dari nilai terbesar

---

## Summary: Sites dengan Most Large Differences

Berdasarkan hasil query summary:

| Site | Total Large Differences | Energy | GHI | POA | PR GHI | PR POA | Availability |
|------|------------------------|--------|-----|-----|--------|--------|--------------|
| **PT. MMKI 1.75 MWp - Painting Building** | **344** | 1 | 191 | 152 | 0 | 0 | 0 |
| **Shoetown Ligung Indonesia** | **328** | 32 | 6 | 290 | 0 | 0 | 0 |
| **PT. MMKI 5.7 MWp - Phase 2** | **255** | 52 | 187 | 16 | 0 | 0 | 0 |
| **PT. Pusan Manis Mulia 2.06 MWp - Tangerang** | **245** | 3 | 1 | 241 | 0 | 0 | 0 |
| **Garuda Metalindo (IKP)** | **206** | 9 | 4 | 193 | 0 | 0 | 0 |
| **Garuda Metalindo 1** | **137** | 2 | 1 | 134 | 0 | 0 | 0 |
| **PT. MMKI 4.292 MWP - Phase 3** | **106** | 4 | 101 | 1 | 0 | 0 | 0 |
| **Charoen Pokphand Bandung** | **92** | 21 | 1 | 70 | 0 | 0 | 0 |
| **PLTS Frina Lestari Nusantara** | **54** | 9 | 0 | 45 | 0 | 0 | 0 |
| **PLTS Rooftop Sumatera Prima Fibreboard** | **21** | 3 | 0 | 18 | 0 | 0 | 0 |

---

## Key Findings

### Sites dengan Most Issues

1. **PT. MMKI 1.75 MWp - Painting Building** (344 large differences)
   - GHI: 191 records
   - POA: 152 records
   - **Action**: Investigate GHI dan POA calculation differences

2. **Shoetown Ligung Indonesia** (328 large differences)
   - POA: 290 records (sangat tinggi!)
   - Energy: 32 records
   - **Action**: Critical - POA calculation method perlu diinvestigasi

3. **PT. MMKI 5.7 MWp - Phase 2** (255 large differences)
   - GHI: 187 records
   - Energy: 52 records
   - **Action**: Investigate GHI dan Energy calculations

4. **PT. Pusan Manis Mulia 2.06 MWp - Tangerang** (245 large differences)
   - POA: 241 records (sangat tinggi!)
   - **Action**: Critical - POA calculation method perlu diinvestigasi

### Metric Types dengan Most Issues

1. **POA**: Banyak site memiliki >100 records dengan large differences
   - Shoetown Ligung: 290 records
   - PT. Pusan Manis Mulia: 241 records
   - Garuda Metalindo (IKP): 193 records
   - PT. MMKI I: 152 records

2. **GHI**: Beberapa site memiliki banyak large differences
   - PT. MMKI I: 191 records
   - PT. MMKI II: 187 records
   - PT. MMKI III: 101 records

3. **Energy**: Beberapa site memiliki large differences
   - PT. MMKI II: 52 records
   - Shoetown Ligung: 32 records
   - Charoen Pokphand Bandung: 21 records

---

## Cara Menggunakan

### 1. Untuk melihat semua large differences (detail):
```sql
-- Jalankan query dari file
\i dbt/analyses/compare_excel_vs_db_large_differences.sql
```

Atau copy-paste isi file ke SQL client dan execute.

**Output**: List semua record dengan large differences, sorted by:
- Site name
- Metric type
- Absolute difference (descending)
- Date

### 2. Untuk melihat summary per site:
```sql
-- Jalankan query dari file
\i dbt/analyses/compare_excel_vs_db_large_differences_summary.sql
```

**Output**: Summary count per site dan metric type

### 3. Untuk filter site tertentu:
Tambahkan WHERE clause:
```sql
WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
```

### 4. Untuk filter metric tertentu:
Tambahkan WHERE clause:
```sql
WHERE metric_type = 'POA'
```

### 5. Untuk filter date range:
Tambahkan WHERE clause:
```sql
WHERE date_key >= '2025-01-01' AND date_key <= '2025-03-31'
```

---

## Interpretasi Hasil

### Direction Column
- **Excel > DB**: Nilai di Excel lebih besar dari Database
- **Excel < DB**: Nilai di Excel lebih kecil dari Database

### Absolute vs Relative Difference
- **Absolute Difference**: Selisih absolut (misalnya 0.5 MWh, 2.0 kWh/m²)
- **Relative Difference**: Selisih dalam persentase (misalnya 5%, 10%)

### Priority untuk Investigation

**High Priority** (Large differences dengan impact tinggi):
1. Energy differences > 1 MWh
2. GHI/POA differences > 2 kWh/m² atau > 5% relative
3. PR differences > 0.05 (5%)
4. Availability differences > 0.10 (10%)

**Medium Priority**:
1. Energy differences 0.1-1 MWh
2. GHI/POA differences 0.5-2 kWh/m² atau 1-5% relative
3. PR differences 0.01-0.05 (1-5%)

**Low Priority** (Mungkin rounding differences):
1. Differences yang masih dalam threshold acceptable

---

## Next Steps

1. **Run Query Detail** untuk site dengan most issues
2. **Export to Excel/CSV** untuk analisis lebih lanjut
3. **Group by Date Range** untuk melihat pola temporal
4. **Investigate Root Cause** untuk large differences:
   - Calculation method differences?
   - Data quality issues?
   - Missing data handling?
   - Unit conversion errors?

---

**File Created**: 2025-01-XX  
**Last Updated**: 2025-01-XX

