# Langkah Selanjutnya Setelah dbt Seed

Setelah Anda menjalankan `dbt seed --select seed_cleaning_log`, ikuti langkah-langkah berikut:

## ✅ Langkah 1: Verifikasi Data di Database

Jalankan query verifikasi untuk memastikan data sudah masuk dengan benar:

```sql
-- Cek total records
SELECT COUNT(*) as total_records FROM staging.seed_cleaning_log;

-- Cek per asset_type
SELECT asset_type, COUNT(*) as count 
FROM staging.seed_cleaning_log 
GROUP BY asset_type;

-- Cek sites dengan cleaning_date
SELECT 
    asset_type,
    COALESCE(site_id, site_name) as site_identifier,
    MAX(cleaning_date) as last_cleaning_date,
    CURRENT_DATE - MAX(cleaning_date) as days_since_cleaning
FROM staging.seed_cleaning_log
WHERE is_active = TRUE AND cleaning_date IS NOT NULL
GROUP BY asset_type, site_id, site_name
ORDER BY days_since_cleaning DESC;
```

Atau jalankan script lengkap:
```bash
psql -U your_user -d your_database -f scripts/verify_cleaning_log_data.sql
```

## ✅ Langkah 2: Perbaiki Data yang cleaning_date NULL

Jika ada records dengan `cleaning_date` NULL (kosong), Anda perlu mengisi tanggalnya:

```sql
-- Lihat data yang belum ada cleaning_date
SELECT 
    asset_type,
    asset_id,
    COALESCE(site_id, site_name) as site_identifier,
    cleaning_date
FROM staging.seed_cleaning_log
WHERE cleaning_date IS NULL;

-- Update cleaning_date untuk records yang kosong
-- Contoh: Update untuk site tertentu
UPDATE staging.seed_cleaning_log
SET cleaning_date = '2025-12-01',  -- Ganti dengan tanggal yang benar
    updated_at = NOW()
WHERE cleaning_date IS NULL
  AND site_id = 'NE=50488260'
  AND asset_type = 'sensor';
```

**Catatan**: Di CSV, jika `cleaning_date` kosong, akan menjadi NULL di database. Pastikan semua records yang aktif memiliki `cleaning_date`.

## ✅ Langkah 3: Setup di Power BI

### 3.1. Refresh Data Source

1. Buka Power BI Desktop
2. Klik **Transform Data** atau **Refresh** untuk memuat data terbaru dari database
3. Pastikan tabel `seed_cleaning_log` sudah muncul di model

### 3.2. Buat Relationship

Buat relationship antara `seed_cleaning_log` dengan tabel referensi:

**Untuk Sensor:**
- `seed_cleaning_log[site_id]` → `seed_sensor_config[site_id]` (untuk FusionSolar)
- `seed_cleaning_log[site_name]` → `seed_site_config[Site]` (untuk MMKI dan sites lainnya)
- `seed_cleaning_log[asset_id]` → `dim_assets[asset_id]` (alternatif)

**Untuk Module:**
- `seed_cleaning_log[site_id]` → `dim_assets[site_id]` (untuk FusionSolar)
- `seed_cleaning_log[site_name]` → `seed_site_config[Site]` (untuk MMKI)
- `seed_cleaning_log[asset_id]` → `dim_assets[asset_id]` (alternatif)

**Cara membuat relationship:**
1. Klik **Model** view di Power BI
2. Drag kolom dari `seed_cleaning_log` ke kolom yang sesuai di tabel referensi
3. Set cardinality: **Many to One** (Many: cleaning_log, One: referensi)
4. Set cross filter direction: **Both** (jika perlu)

### 3.3. Buat DAX Measures

Copy formula dari `scripts/DAX_Cleaning_Log_Measures.txt` dan paste sebagai measure baru:

1. Klik kanan pada tabel `seed_sensor_config` atau `dim_assets`
2. Pilih **New Measure**
3. Paste formula DAX
4. Beri nama measure (contoh: "Days Since Last Cleaning (Sensor)")

**Formula yang direkomendasikan:**

```dax
Days Since Last Cleaning (Sensor) = 
VAR LastCleaningDate = 
    CALCULATE(
        MAX('seed_cleaning_log'[cleaning_date]),
        FILTER(
            'seed_cleaning_log',
            'seed_cleaning_log'[asset_type] = "sensor" &&
            (
                ('seed_cleaning_log'[site_id] = RELATED('seed_sensor_config'[site_id]) && NOT ISBLANK(RELATED('seed_sensor_config'[site_id]))) ||
                ('seed_cleaning_log'[site_name] = RELATED('seed_site_config'[Site]) && NOT ISBLANK(RELATED('seed_site_config'[Site])))
            ) &&
            'seed_cleaning_log'[is_active] = TRUE() &&
            NOT ISBLANK('seed_cleaning_log'[cleaning_date])
        )
    )
RETURN
    IF(
        ISBLANK(LastCleaningDate),
        BLANK(),
        DATEDIFF(LastCleaningDate, TODAY(), DAY)
    )
```

**Catatan**: Perhatikan case sensitivity:
- Di CSV: `sensor` dan `module` (lowercase)
- Di DAX: Pastikan match dengan data di database

## ✅ Langkah 4: Test di Power BI

### 4.1. Buat Visual Test

1. Buat **Table** visual
2. Tambahkan kolom:
   - `site_name` atau `site_id` dari tabel referensi
   - `Days Since Last Cleaning (Sensor)` measure
   - `Days Since Last Cleaning (Module)` measure

3. Filter untuk melihat data yang ada cleaning_date

### 4.2. Verifikasi Hasil

- ✅ Measure menampilkan angka (days) untuk sites yang sudah ada cleaning_date
- ✅ Measure menampilkan BLANK untuk sites yang belum ada cleaning_date
- ✅ Angka sesuai dengan selisih hari dari cleaning_date ke TODAY()

## ✅ Langkah 5: Update Data cleaning_date yang Kosong

Jika ada records dengan `cleaning_date` kosong di CSV, Anda perlu:

1. **Update CSV** dengan tanggal yang benar
2. **Jalankan dbt seed lagi**:
   ```bash
   dbt seed --select seed_cleaning_log
   ```
3. **Refresh Power BI** untuk melihat perubahan

## ✅ Langkah 6: Buat Visual Dashboard

Setelah measure bekerja, buat visual untuk monitoring:

### Table Visual:
- Site Name
- Last Cleaning Date (Sensor)
- Days Since Last Cleaning (Sensor)
- Last Cleaning Date (Module)
- Days Since Last Cleaning (Module)
- Cleaning Status (dengan conditional formatting)

### Conditional Formatting:
- **Hijau**: ≤ 30 hari
- **Kuning**: 31-60 hari
- **Oranye**: 61-90 hari
- **Merah**: > 90 hari atau BLANK

## 🔧 Troubleshooting

### Problem: Measure return BLANK untuk semua sites
**Solusi**:
- Cek relationship sudah dibuat dengan benar
- Cek case sensitivity: `sensor` vs `Sensor`, `module` vs `Module`
- Cek apakah `cleaning_date` tidak NULL
- Cek apakah `is_active = TRUE`

### Problem: Measure return error
**Solusi**:
- Pastikan semua tabel referensi sudah di-import ke Power BI
- Pastikan kolom yang digunakan di RELATED() ada di tabel referensi
- Cek syntax DAX (tanda kurung, koma, dll)

### Problem: Data tidak muncul di Power BI
**Solusi**:
- Refresh data source
- Cek filter di visual
- Cek apakah tabel sudah di-import ke model

## 📝 Checklist

- [ ] Data sudah masuk ke database (verifikasi dengan query)
- [ ] Semua records yang aktif sudah ada `cleaning_date`
- [ ] Relationship sudah dibuat di Power BI
- [ ] DAX measures sudah dibuat dan tidak error
- [ ] Visual test menampilkan data dengan benar
- [ ] Conditional formatting sudah diterapkan
- [ ] Dashboard sudah dibuat dan siap digunakan

## 🎯 Next Steps

Setelah semua setup selesai:
1. **Update data secara berkala** - Tambahkan cleaning_date baru ke CSV dan run dbt seed
2. **Monitor dashboard** - Cek sites yang sudah overdue untuk cleaning
3. **Maintain data** - Update `is_active = FALSE` jika ada data yang salah

Selamat! Cleaning log sudah siap digunakan! 🎉

