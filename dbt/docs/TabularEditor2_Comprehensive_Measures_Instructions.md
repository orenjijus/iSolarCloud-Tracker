# Comprehensive Measures Script - Instructions

> **Panduan ringkas operasional (Power BI)**: [reports/guides/TabularEditor2_Instructions.md](../../reports/guides/TabularEditor2_Instructions.md)

## Overview
Script ini membuat semua measures yang diperlukan untuk analisis performa site dengan dukungan:
- **Energy**: Actual, Target, KPI, Actual/Target, Actual/KPI
- **GHI**: Actual, Target, Actual/Target
- **POA**: Actual, Target, Actual/Target
- **GHI vs Energy**: Selisih (Energy Actual/KPI - GHI Actual/Target)
- **PR**: PR GHI dan PR POA

Semua measures tersedia untuk 5 time periods:
- **Daily**: Data harian
- **Monthly**: Agregasi bulanan
- **MTD**: Month-to-Date (dari awal bulan sampai tanggal terpilih)
- **Yearly**: Agregasi tahunan
- **YTD**: Year-to-Date (dari awal tahun sampai tanggal terpilih)

## Prerequisites

1. **Parameter_ViewType Table**: Pastikan tabel `Parameter_ViewType` sudah dibuat (gunakan script `TabularEditor2_Create_Parameter_Table.cs`)

2. **Table Name**: Pastikan nama tabel fact table sudah benar. Default: `"Measurement"`. Jika berbeda, edit baris 6 di script:
   ```csharp
   var factTableName = "Measurement"; // Ganti dengan nama tabel Anda
   ```

3. **KPI Table**: Pastikan tabel `mart_site_kpi_monthly` ada di model PowerBI. Jika tidak ada, measures KPI akan di-skip.

## Cara Menggunakan

### 1. Buka Tabular Editor 2
- Connect ke PowerBI Desktop model Anda
- File > New > C# Script

### 2. Copy Script
- Copy seluruh isi file `TabularEditor2_Comprehensive_Measures.cs`
- Paste ke Tabular Editor 2

### 3. Update Table Name (jika perlu)
- Edit baris 6 jika nama tabel berbeda dari `"Measurement"`

### 4. Run Script
- Tekan **F5** untuk menjalankan script
- Pastikan tidak ada error di output window

### 5. Save Changes
- File > Save to DB... (Ctrl+S)
- Atau: File > Save All

### 6. Refresh PowerBI Desktop
- Kembali ke PowerBI Desktop
- Refresh model (jika perlu)
- Measures akan muncul di tabel "Measurement"

## Measures yang Dibuat

### Energy Measures (25 measures)
- Energy Actual: Daily, Monthly, MTD, Yearly, YTD
- Energy Target: Daily, Monthly, MTD, Yearly, YTD
- Energy KPI: Monthly, MTD, Yearly, YTD (jika tabel KPI tersedia)
- Energy Actual/Target: Daily, Monthly, MTD, Yearly, YTD
- Energy Actual/KPI: Monthly, MTD, Yearly, YTD (jika tabel KPI tersedia)

### GHI Measures (15 measures)
- GHI Actual: Daily, Monthly, MTD, Yearly, YTD
- GHI Target: Daily, Monthly, MTD, Yearly, YTD
- GHI Actual/Target: Daily, Monthly, MTD, Yearly, YTD

### POA Measures (15 measures)
- POA Actual: Daily, Monthly, MTD, Yearly, YTD
- POA Target: Daily, Monthly, MTD, Yearly, YTD
- POA Actual/Target: Daily, Monthly, MTD, Yearly, YTD

### GHI vs Energy (4 measures)
- GHI vs Energy: Monthly, MTD, Yearly, YTD
- Formula: Energy Actual/KPI - GHI Actual/Target

### PR Measures (10 measures)
- PR GHI: Daily, Monthly, MTD, Yearly, YTD
- PR POA: Daily, Monthly, MTD, Yearly, YTD

**Total: ~69 measures** (tergantung apakah tabel KPI tersedia)

## Exclude Issue Dates

Semua measures otomatis sync dengan toggle **Actual/Adjusted** via slicer:

1. **Buat Slicer** di PowerBI:
   - Insert > Slicer
   - Field: `Parameter_ViewType[ViewType]`
   - Format: Single select

2. **Toggle Behavior**:
   - **Actual**: Menampilkan semua data (termasuk issue dates)
   - **Adjusted**: Exclude issue dates dari kalkulasi

3. **Cara Kerja**:
   - Semua measures menggunakan helper measure `_ShouldExcludeIssueDates`
   - Helper measure membaca nilai dari slicer `Parameter_ViewType`
   - Jika "Adjusted" dipilih, measures akan filter `is_issue_date = FALSE()`

## Format Measures

- **Actual/Target Values**: Format number dengan 2 desimal (`#,##0.00`)
- **Ratios (Actual/Target, Actual/KPI)**: Format percentage (`0.00%`)
- **PR Values**: Format percentage (`0.00%`)
- **GHI vs Energy**: Format percentage (`0.00%`)

## Troubleshooting

### Error: "Table 'Measurement' not found"
- **Solusi**: Edit baris 6 di script, ganti `"Measurement"` dengan nama tabel yang benar di PowerBI model Anda

### Error: "Cannot find table 'Parameter_ViewType'"
- **Solusi**: Jalankan script `TabularEditor2_Create_Parameter_Table.cs` terlebih dahulu

### Measures KPI tidak muncul
- **Penyebab**: Tabel `mart_site_kpi_monthly` tidak ada di model
- **Solusi**: Import tabel `mart_site_kpi_monthly` ke PowerBI model, atau measures KPI akan di-skip (tidak error)

### Measures tidak update saat toggle Actual/Adjusted
- **Penyebab**: Slicer `Parameter_ViewType` belum dibuat atau tidak terhubung
- **Solusi**: Pastikan slicer menggunakan field `Parameter_ViewType[ViewType]` dan measures menggunakan helper `_ShouldExcludeIssueDates`

### Time aggregation tidak akurat
- **Penyebab**: Filter time mungkin tidak sesuai dengan struktur data
- **Solusi**: Pastikan kolom `date_key` ada di tabel dan format date-nya benar

## Notes

1. **Helper Measure**: Measure `_ShouldExcludeIssueDates` akan dibuat sebagai hidden measure. Jangan hapus atau ubah measure ini.

2. **KPI Measures**: KPI adalah monthly data, jadi:
   - Daily KPI tidak tersedia (tidak ada daily KPI)
   - MTD KPI = Monthly KPI (karena KPI adalah monthly)
   - YTD KPI = Sum of all months up to current month

3. **Time Filters**: 
   - Monthly: Filter berdasarkan year dan month yang dipilih
   - Yearly: Filter berdasarkan year yang dipilih
   - MTD: Filter dari awal bulan sampai tanggal terpilih
   - YTD: Filter dari awal tahun sampai tanggal terpilih

4. **Performance**: Measures menggunakan `CALCULATE` dengan filter yang efisien. Untuk dataset besar, pastikan ada index pada kolom `date_key` dan `is_issue_date`.

## Next Steps

Setelah measures dibuat:

1. **Buat Visualizations**:
   - Gunakan measures di charts, tables, dan cards
   - Toggle antara Actual/Adjusted menggunakan slicer

2. **Test Measures**:
   - Verifikasi nilai measures sesuai ekspektasi
   - Test toggle Actual/Adjusted
   - Test time aggregations (Daily, Monthly, MTD, Yearly, YTD)

3. **Create Dashboards**:
   - Buat dashboard dengan berbagai time periods
   - Gunakan slicers untuk filtering
   - Tambahkan conditional formatting untuk highlight performa

