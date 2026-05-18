# Seed vs Mart untuk Weekly Log - Penjelasan Lengkap

## ❓ Pertanyaan: Apakah Perlu Mart atau Bisa Langsung Seed?

**Jawaban Singkat**: 
- ✅ **Bisa langsung dari Seed** jika tidak perlu enrichment/transformasi
- ✅ **Perlu Mart** jika butuh kolom tambahan (week_year, week_number, week_end_date, dll)

---

## 📊 Perbandingan: Seed vs Mart

### Option 1: Langsung Import Seed ke Power BI

**Tabel**: `staging.seed_weekly_log`

**Kolom yang Tersedia**:
- `week_start_date`
- `site_id`
- `site_name`
- `problem_identification`
- `corrective_action`
- `status`
- `created_at`
- `updated_at`

**Keuntungan**:
- ✅ Lebih sederhana, tidak perlu run dbt model
- ✅ Langsung dari source data
- ✅ Update lebih cepat (hanya perlu `dbt seed`)

**Kekurangan**:
- ❌ Tidak ada kolom tambahan (week_year, week_number, week_end_date)
- ❌ Harus hitung week_number, week_end_date di Power BI (DAX)
- ❌ Tidak ada enrichment/standardization

**Kapan Cocok**:
- Data sederhana, tidak perlu transformasi
- Tidak perlu kolom tambahan
- Ingin setup cepat

---

### Option 2: Import Mart ke Power BI

**Tabel**: `marts.mart_weekly_log`

**Kolom yang Tersedia** (semua dari seed + tambahan):
- `week_start_date`
- `site_id`
- `site_name`
- `problem_identification`
- `corrective_action`
- `status`
- `created_at`
- `updated_at`
- ✅ **`week_year`** (tahun dari week_start_date)
- ✅ **`week_number`** (nomor minggu dalam tahun)
- ✅ **`week_month`** (bulan dari week_start_date)
- ✅ **`week_month_name`** (nama bulan, contoh: "December 2025")
- ✅ **`week_end_date`** (tanggal akhir minggu, Minggu)

**Keuntungan**:
- ✅ Kolom tambahan sudah dihitung (week_year, week_number, week_end_date)
- ✅ Lebih mudah untuk filtering dan grouping di Power BI
- ✅ Konsisten dengan pattern lain (semua marts di schema `marts`)
- ✅ Bisa ditambah enrichment di masa depan (join dengan dim_assets, dll)

**Kekurangan**:
- ❌ Perlu run 2 command: `dbt seed` + `dbt run`
- ❌ Sedikit lebih kompleks

**Kapan Cocok**:
- Butuh kolom tambahan untuk filtering/grouping
- Ingin konsisten dengan pattern lain
- Mungkin perlu enrichment di masa depan

---

## 🔍 Verifikasi Data

### Cek Seed Data

```sql
-- Cek apakah seed sudah ter-load
SELECT COUNT(*) as total_rows
FROM staging.seed_weekly_log;
```

### Cek Mart Data

```sql
-- Cek apakah mart sudah ter-build
SELECT COUNT(*) as total_rows
FROM marts.mart_weekly_log;
```

### Cek Data di Mart (dengan kolom tambahan)

```sql
SELECT 
    week_start_date,
    site_name,
    week_year,
    week_number,
    week_month_name,
    week_end_date,
    problem_identification,
    corrective_action,
    status
FROM marts.mart_weekly_log
ORDER BY week_start_date DESC, site_name
LIMIT 10;
```

---

## 🎯 Rekomendasi

### Untuk Weekly Log: **Gunakan Mart** ✅

**Alasan**:
1. **Kolom Tambahan Berguna**: `week_year`, `week_number`, `week_end_date` sangat berguna untuk:
   - Filtering berdasarkan tahun/minggu
   - Grouping per bulan
   - Menampilkan week range (start - end)

2. **Konsistensi**: Semua data untuk Power BI sebaiknya di schema `marts` untuk konsistensi

3. **Fleksibilitas Masa Depan**: Jika nanti perlu join dengan dim_assets atau enrichment lain, sudah siap

4. **Mudah di Power BI**: Tidak perlu hitung week_number, week_end_date dengan DAX

---

## 🔧 Troubleshooting: Data Tidak Masuk ke Mart

### Masalah: Seed sudah masuk, tapi mart kosong

**Penyebab**:
1. Seed belum di-load sebelum run mart
2. Filter di mart model (`WHERE week_start_date IS NOT NULL`) menghapus semua data
3. Data di seed tidak valid

**Solusi**:

#### Step 1: Pastikan Seed Sudah Di-load

```bash
cd dbt
dbt seed --select seed_weekly_log
```

**Verifikasi**:
```sql
SELECT * FROM staging.seed_weekly_log LIMIT 5;
```

#### Step 2: Run Mart Model

```bash
dbt run --select mart_weekly_log
```

#### Step 3: Verifikasi Mart

```sql
SELECT * FROM marts.mart_weekly_log LIMIT 5;
```

#### Step 4: Cek Log dbt

Jika masih kosong, cek log dbt untuk error:
- Apakah ada error saat run?
- Apakah filter menghapus semua data?

---

## 📝 Contoh: Langsung Import Seed (Jika Tidak Perlu Mart)

### Step 1: Load Seed

```bash
dbt seed --select seed_weekly_log
```

### Step 2: Import ke Power BI

1. **Get Data** → PostgreSQL
2. Import: `staging.seed_weekly_log`
3. Import: `dimensions.dim_assets` (untuk relationship)
4. Import: `dimensions.dim_date_generated` (untuk filtering)

### Step 3: Buat Relationship

- `dim_assets[asset_id]` → `seed_weekly_log[site_id]`
- `dim_date_generated[date_key]` → `seed_weekly_log[week_start_date]`

### Step 4: Buat DAX Measures (jika perlu week_number, dll)

```dax
Week Number = 
WEEKNUM('seed_weekly_log'[week_start_date], 2)  -- ISO week (Monday = week start)

Week End Date = 
'seed_weekly_log'[week_start_date] + 6  -- Add 6 days to get Sunday

Week Year = 
YEAR('seed_weekly_log'[week_start_date])
```

---

## 📝 Contoh: Import Mart (Recommended)

### Step 1: Load Seed dan Build Mart

```bash
dbt seed --select seed_weekly_log
dbt run --select mart_weekly_log
```

### Step 2: Import ke Power BI

1. **Get Data** → PostgreSQL
2. Import: `marts.mart_weekly_log`
3. Import: `dimensions.dim_assets` (untuk relationship)
4. Import: `dimensions.dim_date_generated` (untuk filtering)

### Step 3: Buat Relationship

- `dim_assets[asset_id]` → `mart_weekly_log[site_id]`
- `dim_date_generated[date_key]` → `mart_weekly_log[week_start_date]`

### Step 4: Langsung Pakai Kolom Tambahan

Tidak perlu DAX measures, langsung pakai:
- `week_year` untuk filtering tahun
- `week_number` untuk sorting/grouping
- `week_end_date` untuk display week range
- `week_month_name` untuk grouping per bulan

---

## ✅ Kesimpulan

| Aspek | Seed Langsung | Mart (Recommended) |
|-------|---------------|---------------------|
| **Setup** | Lebih sederhana | Sedikit lebih kompleks |
| **Kolom Tambahan** | Tidak ada (harus DAX) | Sudah ada (week_year, week_number, dll) |
| **Konsistensi** | Tidak konsisten | Konsisten dengan pattern lain |
| **Fleksibilitas** | Terbatas | Lebih fleksibel (bisa enrichment) |
| **Power BI** | Perlu DAX untuk week_number | Langsung pakai kolom |

**Rekomendasi**: **Gunakan Mart** untuk weekly log karena kolom tambahan sangat berguna dan setup tidak terlalu kompleks.

---

**Last Updated**: 2025-12-14

