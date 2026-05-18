# Panduan Implementasi Cleaning Log Table

## Ringkasan

Saya telah membuat sistem log pembersihan untuk sensor dan module. **Logika DAX yang Anda pikirkan BENAR!** Menggunakan `TODAY() - [TanggalPembersihan]` adalah cara yang tepat untuk menghitung hari sejak pembersihan terakhir.

## File yang Dibuat

### 1. **dbt/seeds/seed_cleaning_log.csv**
   - File CSV untuk data log pembersihan
   - Format: `asset_type;site_id;cleaning_date;notes;is_active`
   - **PENTING**: Cleaning log adalah **per site**, bukan per sensor/module individual
   - Contoh data sudah disertakan

### 2. **scripts/create_cleaning_log_table.sql**
   - Script SQL untuk membuat tabel di database
   - Sudah termasuk index untuk performa
   - Bisa dijalankan langsung di PostgreSQL

### 3. **scripts/DAX_Cleaning_Log_Measures.txt**
   - Kumpulan formula DAX siap pakai
   - Termasuk measures untuk Sensor dan Module
   - Termasuk status dan alert indicators

### 4. **docs/cleaning-log-table-design.md**
   - Dokumentasi lengkap desain tabel
   - Penjelasan kolom dan relationship
   - Contoh query SQL

## Struktur Tabel

```sql
staging.seed_cleaning_log
├── id (SERIAL PRIMARY KEY)
├── asset_type (VARCHAR)      -- 'Sensor' atau 'Module'
├── site_id (VARCHAR)         -- site_id (cleaning per site)
├── cleaning_date (DATE)      -- Tanggal pembersihan
├── notes (TEXT)              -- Catatan (opsional)
├── is_active (BOOLEAN)       -- Flag aktif
├── created_at (TIMESTAMPTZ)
└── updated_at (TIMESTAMPTZ)
```

**CATATAN PENTING**: 
- Cleaning log adalah **per site**, bukan per sensor/module individual
- Satu record mewakili pembersihan **semua sensor** atau **semua module** di site tersebut
- Gunakan `site_id` yang sama dengan di `seed_sensor_config` atau `seed_site_config`

## Logika DAX (Yang Anda Tanyakan)

**YA, logika Anda BENAR!** Formula dasar:

```dax
Days Since Last Cleaning = TODAY() - [LastCleaningDate]
```

Formula lengkap yang sudah dibuat:

```dax
Days Since Last Cleaning (Sensor) = 
VAR LastCleaningDate = 
    CALCULATE(
        MAX('seed_cleaning_log'[cleaning_date]),
        FILTER(
            'seed_cleaning_log',
            'seed_cleaning_log'[asset_type] = "Sensor" &&
            'seed_cleaning_log'[site_id] = RELATED('seed_sensor_config'[site_id]) &&
            'seed_cleaning_log'[is_active] = TRUE()
        )
    )
RETURN
    IF(
        ISBLANK(LastCleaningDate),
        BLANK(),
        DATEDIFF(LastCleaningDate, TODAY(), DAY)
    )
```

**PENTING**: Formula menggunakan `site_id`, bukan `sensor_id` atau `module_id`, karena cleaning dilakukan per site.

**Penjelasan:**
1. `MAX(cleaning_date)` - Mengambil tanggal pembersihan terakhir
2. `FILTER` - Filter berdasarkan asset_type dan asset_id
3. `DATEDIFF(LastCleaningDate, TODAY(), DAY)` - Menghitung selisih hari
4. Jika belum pernah dibersihkan, return `BLANK()`

## Cara Menggunakan

### Langkah 1: Buat Tabel di Database

Jalankan script SQL:
```bash
psql -U your_user -d your_database -f scripts/create_cleaning_log_table.sql
```

Atau jalankan langsung di database client Anda.

### Langkah 2: Load Data Seed

Jalankan dbt seed:
```bash
cd dbt
dbt seed --select seed_cleaning_log
```

### Langkah 3: Tambahkan Data Pembersihan

Edit file `dbt/seeds/seed_cleaning_log.csv` dan tambahkan baris baru:

```csv
asset_type;site_id;cleaning_date;notes;is_active
Sensor;NE=50488260;2024-12-20;Pembersihan rutin semua sensor di site;
Module;NE=50488260;2024-12-18;Pembersihan setelah hujan semua modul di site;
```

**CATATAN**: Gunakan `site_id`, bukan `sensor_id` atau `module_id`. Satu record mewakili pembersihan semua sensor atau semua module di site tersebut.

Kemudian jalankan lagi:
```bash
dbt seed --select seed_cleaning_log
```

### Langkah 4: Setup di Power BI

1. **Refresh Data Source**
   - Pastikan tabel `seed_cleaning_log` sudah di-import ke Power BI
   - Refresh data source

2. **Buat Relationship**
   - `seed_cleaning_log[site_id]` → `seed_sensor_config[site_id]` (untuk Sensor)
   - `seed_cleaning_log[site_id]` → `seed_site_config[Site]` atau `dim_assets[site_id]` (untuk Module)
   
   **PENTING**: Relationship menggunakan `site_id` karena cleaning log adalah per site!

3. **Buat Measure**
   - Copy formula dari `scripts/DAX_Cleaning_Log_Measures.txt`
   - Paste ke Power BI sebagai New Measure
   - Sesuaikan nama tabel jika berbeda

4. **Buat Visual**
   - Gunakan measure "Days Since Last Cleaning" di table/matrix
   - Tambahkan conditional formatting untuk warna (hijau/kuning/merah)

## Contoh Visual di Power BI

### Table Visual:
| Sensor Name | Last Cleaning Date | Days Since Cleaning | Status |
|-------------|-------------------|---------------------|--------|
| Sensor GHI  | 2024-12-01        | 19                  | 🟢 OK  |
| Sensor POA  | 2024-11-20        | 30                  | 🟢 OK  |
| Sensor POA  | 2024-11-10        | 40                  | 🟡 Warning |

### Conditional Formatting:
- **Hijau**: ≤ 30 hari
- **Kuning**: 31-60 hari
- **Oranye**: 61-90 hari
- **Merah**: > 90 hari

## Catatan Penting

1. **Multiple Cleaning Records**: Satu site bisa punya banyak record pembersihan (untuk tanggal berbeda). DAX menggunakan `MAX()` untuk mengambil tanggal terakhir.

2. **Site ID Consistency**: Pastikan `site_id` di cleaning log sama dengan `site_id` di tabel referensi (`seed_sensor_config`, `seed_site_config`, dll).

3. **Per Site, Bukan Per Asset**: Ingat bahwa satu record cleaning log mewakili pembersihan **semua sensor** atau **semua module** di site tersebut, bukan per sensor/module individual.

3. **Date Format**: Gunakan format `YYYY-MM-DD` untuk `cleaning_date`.

4. **Performance**: Index sudah dibuat untuk mempercepat query.

## Query SQL untuk Verifikasi

```sql
-- Melihat last cleaning date per site
SELECT 
    asset_type,
    site_id,
    MAX(cleaning_date) as last_cleaning_date,
    CURRENT_DATE - MAX(cleaning_date) as days_since_cleaning
FROM staging.seed_cleaning_log
WHERE is_active = TRUE
GROUP BY asset_type, site_id
ORDER BY days_since_cleaning DESC;
```

## Troubleshooting

### Problem: Measure return BLANK
**Solusi**: 
- Pastikan relationship sudah dibuat dengan benar menggunakan `site_id`
- Pastikan `site_id` di cleaning log sesuai dengan `site_id` di tabel referensi
- Pastikan ada data dengan `is_active = TRUE`
- Ingat bahwa cleaning log adalah per site, jadi pastikan `site_id` match

### Problem: Days count salah
**Solusi**:
- Pastikan `cleaning_date` format DATE (bukan TIMESTAMP)
- Gunakan `DATEDIFF()` untuk menghitung selisih hari

### Problem: Data tidak muncul di Power BI
**Solusi**:
- Refresh data source di Power BI
- Pastikan tabel sudah di-import ke model
- Cek filter di visual

## Kesimpulan

✅ **Logika DAX Anda BENAR**: `TODAY() - cleaning_date` adalah cara yang tepat

✅ **Tabel sudah dibuat**: Siap digunakan untuk tracking pembersihan

✅ **Formula DAX siap pakai**: Copy-paste dari file yang sudah dibuat

✅ **Dokumentasi lengkap**: Semua detail ada di file-file yang dibuat

Silakan mulai dengan menjalankan script SQL dan load seed data!

