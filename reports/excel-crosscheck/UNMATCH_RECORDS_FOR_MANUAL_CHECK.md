# Unmatch Records for Manual Crosscheck

File ini berisi daftar tanggal yang unmatch antara Excel dan Database untuk crosscheck manual.

## Cara Menggunakan File Ini

1. Query lengkap tersedia di: `dbt/analyses/compare_excel_vs_db_unmatch_for_manual_check.sql`
2. Jalankan query tersebut di database untuk mendapatkan semua data unmatch
3. Export hasil ke CSV atau Excel untuk crosscheck manual
4. Bandingkan nilai Excel vs Database untuk setiap metric

## Ringkasan Unmatch per Site

| Site Name | Total Unmatch | Energy | GHI | POA | PR GHI | PR POA | Availability | Missing Excel | Missing DB |
|-----------|---------------|--------|-----|-----|--------|--------|--------------|---------------|------------|
| PT. MMKI 5.7 MWp - Phase 2 | 569 | 569 | 443 | 270 | 468 | 381 | 569 | 253 | 0 |
| PT. MMKI 1.75 MWp - Painting Building | 558 | 557 | 436 | 398 | 440 | 431 | 558 | 241 | 4 |
| PT. MMKI 4.292 MWP - Phase 3 | 554 | 554 | 494 | 495 | 496 | 496 | 554 | 393 | 0 |
| Garuda Metalindo (IKP) | 403 | 100 | 93 | 286 | 102 | 324 | 403 | 86 | 0 |
| Garuda Metalindo (MPF) | 400 | 92 | 86 | 86 | 96 | 96 | 400 | 83 | 0 |
| Garuda Metalindo 2 | 399 | 87 | 84 | 82 | 89 | 87 | 399 | 82 | 0 |
| Garuda Metalindo 1 | 391 | 79 | 80 | 213 | 85 | 273 | 391 | 74 | 0 |
| PT. Pusan Manis Mulia 2.06 MWp - Tangerang | 372 | 59 | 57 | 296 | 61 | 326 | 372 | 55 | 0 |
| Shoetown Ligung Indonesia | 364 | 82 | 72 | 337 | 101 | 360 | 364 | 47 | 0 |
| PLTS Mall Panakkukang | 347 | 30 | 31 | 32 | 62 | 33 | 347 | 30 | 0 |
| Charoen Pokphand Majalengka | 128 | 23 | 23 | 23 | 28 | 28 | 128 | 23 | 0 |
| PLTS Frina Lestari Nusantara | 127 | 22 | 22 | 67 | 31 | 89 | 127 | 22 | 0 |
| Charoen Pokphand Bandung | 119 | 17 | 17 | 86 | 37 | 97 | 119 | 14 | 0 |
| Charoen Pokphand Madiun | 91 | 11 | 7 | 8 | 12 | 12 | 91 | 7 | 0 |
| PLTS Rooftop Sumatera Prima Fibreboard | 50 | 15 | 10 | 26 | 14 | 36 | 50 | 10 | 0 |
| PT Gelora Djaja 1 MWp | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 5 | 0 |
| PT. MMKI 5.7 MWp - Phase 3 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 1 |

## Format Data untuk Crosscheck

Setiap record unmatch akan menampilkan:

### Kolom yang Tersedia:
- **Date**: Tanggal data
- **Site**: Nama site
- **Capacity (kW)**: Kapasitas site

### Untuk Setiap Metric (Energy, GHI, POA, PR GHI, PR POA, Availability):
- **Excel Value**: Nilai dari Excel
- **DB Value**: Nilai dari Database
- **Difference**: Selisih (Excel - DB)
- **Match Status**: ✓ (match) atau ✗ (unmatch) atau N/A (salah satu NULL)

### Flags:
- **Missing_Excel**: "YES" jika data tidak ada di Excel
- **Missing_DB**: "YES" jika data tidak ada di Database
- **Unmatch_Type**: Jenis unmatch pertama yang ditemukan (Energy, GHI, POA, PR_GHI, PR_POA, Availability, Missing_Excel, Missing_DB)

## Tolerance yang Digunakan

- **Energy**: Absolute tolerance ≤ 0.01 MWh
- **GHI**: Relative tolerance ≤ 1% (0.01)
- **POA**: Relative tolerance ≤ 1% (0.01)
- **PR GHI**: Absolute tolerance ≤ 0.5% (updated from 0.1%)
- **PR POA**: Absolute tolerance ≤ 0.5% (updated from 0.1%)
- **Availability**: Absolute tolerance ≤ 0.01

## Catatan Penting

1. **PR Format**: Excel menyimpan PR sebagai decimal (0-1), sedangkan DB menyimpan sebagai percentage (0-100). Query sudah melakukan konversi dengan mengalikan Excel PR dengan 100.

2. **MMKI Sites**: Site MMKI memiliki banyak unmatch karena GHI di Excel sudah dikoreksi secara manual, sehingga berbeda dengan DB.

3. **Availability**: Banyak unmatch pada Availability karena format atau perhitungan yang berbeda antara Excel dan DB.

4. **Missing Data**: Beberapa tanggal tidak ada di Excel (terutama untuk site MMKI) atau tidak ada di DB.

## Query untuk Mendapatkan Data Lengkap

Jalankan query berikut untuk mendapatkan semua data unmatch:

```sql
-- File: dbt/analyses/compare_excel_vs_db_unmatch_for_manual_check.sql
```

Atau gunakan query yang lebih ringkas dengan limit per site:

```sql
-- File: dbt/analyses/compare_excel_vs_db_unmatch_details.sql
```

## Langkah Crosscheck Manual

1. **Export Data**: Jalankan query dan export hasil ke CSV/Excel
2. **Buka Excel Source**: Buka file Excel asli untuk site yang akan dicek
3. **Buka Database**: Akses database dan cek nilai di `mart.mart_site_performance_daily`
4. **Bandingkan**: Untuk setiap tanggal unmatch, bandingkan:
   - Energy: Excel vs DB
   - GHI: Excel vs DB
   - POA: Excel vs DB
   - PR GHI: Excel vs DB (pastikan format sudah dikonversi)
   - PR POA: Excel vs DB (pastikan format sudah dikonversi)
   - Availability: Excel vs DB
5. **Catat Temuan**: Catat alasan perbedaan (jika ada) atau konfirmasi bahwa perbedaan tersebut expected

## Tips

- Mulai dari site dengan unmatch paling sedikit untuk memahami pola
- Fokus pada PR GHI dan PR POA karena ini yang paling sering unmatch
- Perhatikan site MMKI karena GHI sudah dikoreksi di Excel
- Cek juga data yang missing (tidak ada di Excel atau DB)

