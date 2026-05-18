# Penjelasan Kolom `is_active` di Cleaning Log

## Apa itu `is_active`?

Kolom `is_active` adalah **flag boolean** yang menandai apakah sebuah record cleaning log masih **aktif** atau sudah **dinonaktifkan**.

## Kegunaan Utama

### 1. **Soft Delete (Penghapusan Lunak)**
   - **Tidak menghapus data** dari database
   - Hanya menandai record sebagai tidak aktif
   - Data tetap tersimpan untuk **audit trail** dan **history**

### 2. **Koreksi Data Tanpa Menghapus**
   - Jika ada **kesalahan input** (misalnya tanggal salah)
   - Set `is_active = FALSE` pada record yang salah
   - Buat record baru dengan data yang benar
   - History tetap tersimpan untuk referensi

### 3. **Membatalkan Pembersihan**
   - Jika pembersihan **dibatalkan** atau **tidak jadi dilakukan**
   - Set `is_active = FALSE` tanpa menghapus record
   - Tetap ada catatan bahwa ada rencana pembersihan yang dibatalkan

## Contoh Penggunaan

### Contoh 1: Koreksi Data Salah Input

**Situasi**: Input tanggal pembersihan salah (seharusnya 2024-12-15, tapi terinput 2024-12-05)

**Solusi**:
```sql
-- 1. Nonaktifkan record yang salah
UPDATE staging.seed_cleaning_log
SET is_active = FALSE,
    updated_at = NOW()
WHERE id = 123;

-- 2. Tambahkan record baru dengan tanggal yang benar
INSERT INTO staging.seed_cleaning_log 
(asset_type, site_id, cleaning_date, notes, is_active)
VALUES 
('Sensor', 'NE=50488260', '2024-12-15', 'Pembersihan rutin - koreksi tanggal', TRUE);
```

### Contoh 2: Membatalkan Pembersihan

**Situasi**: Pembersihan yang direncanakan dibatalkan karena cuaca buruk

**Solusi**:
```sql
-- Nonaktifkan record pembersihan yang dibatalkan
UPDATE staging.seed_cleaning_log
SET is_active = FALSE,
    notes = notes || ' - DIBATALKAN: Cuaca buruk',
    updated_at = NOW()
WHERE site_id = 'NE=50488260' 
  AND cleaning_date = '2024-12-20'
  AND asset_type = 'Module';
```

### Contoh 3: Di CSV File

```csv
asset_type;site_id;cleaning_date;notes;is_active
Sensor;NE=50488260;2024-12-01;Pembersihan rutin;
Sensor;NE=50488260;2024-12-05;Data salah - tanggal salah;FALSE
Sensor;NE=50488260;2024-12-15;Pembersihan rutin - koreksi;
Module;NE=50488260;2024-12-20;Dibatalkan - cuaca buruk;FALSE
```

## Bagaimana Cara Kerjanya?

### Di Query SQL

Semua query yang mengambil data aktif menggunakan filter:
```sql
WHERE is_active = TRUE
```

### Di DAX Formula

Semua formula DAX sudah include filter:
```dax
FILTER(
    'seed_cleaning_log',
    ...
    'seed_cleaning_log'[is_active] = TRUE()
)
```

Jadi, record yang `is_active = FALSE` **tidak akan muncul** di perhitungan "Days Since Last Cleaning".

## Default Value

- **Default**: `TRUE` (aktif)
- **Di CSV**: Jika kolom kosong, dianggap `TRUE`
- **Di Database**: Default constraint `DEFAULT TRUE`

## Kapan Menggunakan `FALSE`?

Gunakan `is_active = FALSE` ketika:

1. ✅ **Data salah input** - Tanggal, site_id, atau asset_type salah
2. ✅ **Pembersihan dibatalkan** - Rencana pembersihan tidak jadi dilakukan
3. ✅ **Data duplikat** - Ada record yang sama yang sudah diinput sebelumnya
4. ✅ **Data uji coba** - Record yang dibuat untuk testing
5. ✅ **Koreksi manual** - Data perlu dikoreksi tapi tetap simpan history

## Kapan TIDAK Menggunakan `FALSE`?

Jangan gunakan `is_active = FALSE` untuk:

1. ❌ **Pembersihan selesai** - Ini normal, tetap `TRUE`
2. ❌ **Pembersihan rutin** - Ini normal, tetap `TRUE`
3. ❌ **Data lama** - Data lama tetap aktif selama valid

## Best Practice

1. **Jangan hapus data** - Selalu gunakan `is_active = FALSE` daripada DELETE
2. **Tambahkan catatan** - Update kolom `notes` dengan alasan dinonaktifkan
3. **Buat record baru** - Jika perlu koreksi, buat record baru dengan data benar
4. **Review berkala** - Sesekali review data yang `is_active = FALSE` untuk cleanup

## Query untuk Monitoring

### Melihat Record yang Dinonaktifkan:

```sql
SELECT 
    id,
    asset_type,
    site_id,
    cleaning_date,
    notes,
    created_at,
    updated_at
FROM staging.seed_cleaning_log
WHERE is_active = FALSE
ORDER BY updated_at DESC;
```

### Statistik:

```sql
SELECT 
    asset_type,
    COUNT(*) FILTER (WHERE is_active = TRUE) as aktif,
    COUNT(*) FILTER (WHERE is_active = FALSE) as dinonaktifkan,
    COUNT(*) as total
FROM staging.seed_cleaning_log
GROUP BY asset_type;
```

## Kesimpulan

Kolom `is_active` adalah cara yang **aman** dan **profesional** untuk:
- ✅ Menjaga **data integrity** (tidak menghapus data)
- ✅ Mempertahankan **audit trail** (history tetap ada)
- ✅ Memungkinkan **koreksi data** tanpa kehilangan informasi
- ✅ **Filter otomatis** di query dan DAX (hanya data aktif yang digunakan)

**TL;DR**: `is_active = TRUE` = data digunakan, `is_active = FALSE` = data dinonaktifkan tapi tetap tersimpan.

