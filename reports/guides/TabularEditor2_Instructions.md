# Tabular Editor 2 - C# Script Instructions

## Cara Menggunakan Script C# untuk Membuat Measures

### Prerequisites
1. **Tabular Editor 2** sudah terinstall
2. PowerBI model sudah dibuat dan memiliki:
   - Tabel `mart mart_site_performance_daily` (atau nama tabel fact yang sesuai)
   - Tabel `Parameter_ViewType` (calculated table untuk slicer)

### Langkah-langkah

#### 1. Buka Tabular Editor 2
- Install Tabular Editor 2 jika belum ada
- Buka aplikasi Tabular Editor 2

#### 2. Connect ke PowerBI Model
- **File > Open > From DB...**
- Pilih **Power BI Desktop** atau **Power BI Service**
- Connect ke model PowerBI Anda

#### 2a. Buat Parameter Table (WAJIB - Lakukan SEBELUM membuat measures!)
**PENTING**: Tabel `Parameter_ViewType` HARUS dibuat terlebih dahulu sebelum membuat measures!

**Cara 1: Menggunakan Script C# (Recommended)**
- **File > New > C# Script**
- Copy paste isi file `TabularEditor2_Create_Parameter_Table.cs`
- Tekan **F5** untuk run script
- **File > Save to DB...** (Ctrl+S) untuk save
- Verifikasi tabel sudah dibuat di Model Explorer

**Cara 2: Manual di PowerBI Desktop**
- Buka PowerBI Desktop
- **Modeling > New Table**
- Copy paste DAX berikut:
  ```DAX
  Parameter_ViewType = 
  DATATABLE(
      "ViewType", STRING,
      "ViewTypeValue", INTEGER,
      {
          {"Actual", 0},
          {"Adjusted", 1}
      }
  )
  ```
- Tekan Enter
- Tabel akan muncul di Fields pane

#### 3. Edit Script Measures
- **File > New > C# Script**
- Copy paste isi file `TabularEditor2_Actual_vs_Adjusted_Measures.cs`
- **PENTING**: Edit baris berikut di bagian atas script:
  ```csharp
  string tableName = "YourTableName"; // Ganti dengan nama tabel yang sesuai
  ```
  
  Contoh:
  ```csharp
  string tableName = "Measurement"; // atau nama tabel lain yang sesuai
  ```

#### 4. Run Script
- Tekan **F5** atau klik tombol **Run** (▶️)
- Script akan membuat semua measures secara otomatis
- Jika ada error, periksa:
  - Nama tabel sudah benar
  - Tabel `Parameter_ViewType` sudah ada
  - Tabel fact `mart mart_site_performance_daily` sudah ada

#### 5. Save & Deploy ke PowerBI
**PENTING**: Setelah script berhasil di-run, Anda HARUS save/deploy changes ke PowerBI!

**Cara 1: Save to Database (Live Connection)**
- **File > Save to DB...** (atau tekan **Ctrl+S**)
- Akan muncul dialog konfirmasi
- Klik **Yes** untuk save changes ke PowerBI model
- Tunggu sampai proses selesai (ada progress bar di bawah)

**Cara 2: Save to File (File-based)**
- **File > Save to Folder...**
- Pilih folder tempat file PowerBI Anda
- Atau **File > Save All** jika menggunakan folder mode

**Cara 3: Refresh PowerBI Desktop**
- Setelah save di Tabular Editor 2, buka PowerBI Desktop
- **File > Options & Settings > Options > Global > Security**
- Pastikan "Allow unsupported Power Query extensions" sudah di-check (jika perlu)
- **Refresh** model di PowerBI Desktop:
  - Klik kanan pada dataset di panel kanan
  - Pilih **Refresh** atau tekan **F5**
- Atau tutup dan buka kembali file PowerBI Desktop

**Verifikasi:**
- Di PowerBI Desktop, buka **Model view** (icon di kiri bawah)
- Cari tabel "Measurement" (atau nama tabel yang Anda gunakan)
- Measures seharusnya sudah muncul di tabel tersebut
- Jika belum muncul, coba:
  1. Tutup dan buka kembali PowerBI Desktop
  2. Atau refresh model sekali lagi

### Measures yang Akan Dibuat

Script ini akan membuat **25 measures**:

#### Helper Measure
- `_ShouldExcludeIssueDates` - Helper untuk check slicer selection

#### Energy Measures (3)
- `Energy MWh`
- `Energy Monthly MWh`
- `Energy YTD MWh`

#### GHI Measures (3)
- `GHI kWh/m²`
- `GHI Monthly Avg kWh/m²`
- `GHI YTD Avg kWh/m²`

#### POA Measures (3)
- `POA kWh/m²`
- `POA Monthly Avg kWh/m²`
- `POA YTD Avg kWh/m²`

#### PR Measures (6)
- `PR GHI`
- `PR GHI Monthly Avg`
- `PR GHI YTD Avg`
- `PR POA`
- `PR POA Monthly Avg`
- `PR POA YTD Avg`

#### Availability Measures (3)
- `Availability %`
- `Availability Monthly Avg %`
- `Availability YTD Avg %`

#### Target Comparison Measures (5)
- `Energy vs Target %`
- `Energy vs Target Monthly %`
- `Energy vs Target YTD %`
- `GHI vs Target %`
- `POA vs Target %`

#### Count Measures (4)
- `Total Days`
- `Issue Days Count`
- `Adjusted Days Count`
- `Issue Days %`

### Format String

Measures sudah dikonfigurasi dengan format string yang sesuai:
- **Energy**: `#,##0.00` (number dengan 2 desimal)
- **GHI/POA**: `#,##0.00` (number dengan 2 desimal)
- **PR/Availability/Target %**: `0.00%` (percentage dengan 2 desimal)
- **Count**: `#,##0` (integer dengan thousand separator)

### Troubleshooting

#### Error: "Table 'YourTableName' tidak ditemukan"
- Pastikan nama tabel sudah benar
- Cek nama tabel di Tabular Editor 2 (biasanya ada di panel kiri)
- Nama tabel harus exact match (case-sensitive)

#### Error: "Column tidak ditemukan"
- Pastikan tabel `mart mart_site_performance_daily` sudah ada
- Pastikan kolom `is_issue_date` sudah ada di tabel tersebut
- Pastikan semua kolom yang direferensikan sudah ada

#### Error: "Cannot find table 'Parameter_ViewType'" atau "Parameter_ViewType tidak ditemukan"
**Ini adalah error yang PALING UMUM!** Tabel `Parameter_ViewType` belum dibuat.

**Solusi:**
1. Jalankan script `TabularEditor2_Create_Parameter_Table.cs` terlebih dahulu
2. Atau buat manual di PowerBI Desktop:
   - **Modeling > New Table**
   - Copy paste DAX:
     ```DAX
     Parameter_ViewType = 
     DATATABLE(
         "ViewType", STRING,
         "ViewTypeValue", INTEGER,
         {
             {"Actual", 0},
             {"Adjusted", 1}
         }
     )
     ```
3. Setelah tabel dibuat, refresh Tabular Editor 2 (F5 di Model Explorer)
4. Measures akan otomatis ter-fix jika tabel sudah ada

### Tips

1. **Backup dulu** model PowerBI sebelum run script
2. **Test di development** environment dulu sebelum production
3. **Group measures** di Tabular Editor 2 untuk organisasi yang lebih baik
4. **Review measures** setelah dibuat untuk memastikan semuanya benar

### Next Steps

Setelah measures dibuat:
1. Buat **Slicer** di PowerBI report:
   - Insert > Slicer
   - Field: `Parameter_ViewType[ViewType]`
   - Format: Single select
2. Gunakan measures di visuals
3. Test toggle Actual vs Adjusted

### Support

Jika ada masalah, periksa:
- Tabular Editor 2 version (disarankan versi terbaru)
- PowerBI model compatibility
- DAX expression syntax

