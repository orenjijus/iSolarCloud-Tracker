# Perbaikan Konfigurasi POA MMKI 2

## 📋 Overview

Dokumen ini menjelaskan perbaikan konfigurasi sensor POA untuk MMKI 2 (PT. MMKI 5.7 MWp - Phase 2) yang mencakup:
1. Sensor yang salah dikategorikan sebagai POA
2. Sensor fisik MMKI 2 yang lama masih muncul padahal sudah digantikan oleh sensor MMKI 1 yang di-override

**Informasi Site:**
- **Site Name:** PT. MMKI 5.7 MWp - Phase 2
- **Site ID:** FS_SITE_NE=51758766
- **Plant Code (FusionSolar):** NE=51758766
- **Device ID yang Salah:** EM011023C7355634 (IRR-AMB-NW-Bod Stamp)
- **POA Override Aktif:** Sejak 2025-09-15, sensor dari MMKI 1 (NE=50488260) di-override ke MMKI 2

---

## 🔍 Masalah yang Ditemukan

### Masalah 1: Sensor yang Salah Konfigurasi

**Sensor:** `IRR-AMB-NW-Bod Stamp`  
**Device ID:** `EM011023C7355634`  
**Site ID:** `NE=51758766` (MMKI 2)

Sensor ini **sebelumnya** dikategorikan sebagai sensor **POA** dengan `sensor_capacity` tertentu, padahal seharusnya **BUKAN** sensor POA. Sensor ini tidak memiliki `sensor_type` dan `sensor_capacity` yang valid.

### Masalah 2: Sensor MMKI 2 yang Lama Masih Muncul

**Masalah:** Sensor fisik MMKI 2 yang lama (EM001023C7355634, EM011023B7436516, EM021023B7436516, EM031023B7436516, EM051023B7436516) masih muncul sebagai POA di beberapa tanggal, padahal sudah digantikan oleh sensor MMKI 1 yang di-override sejak 2025-09-15.

**Root Cause:** Logika POA_OVERRIDE hanya mengubah site assignment dari physical site ke logical site, tetapi tidak mengecualikan sensor fisik dari logical site tersebut. Akibatnya, kedua set sensor (sensor fisik MMKI 2 dan sensor MMKI 1 yang di-override) muncul bersamaan.

**Dampak:**
- Total sensor POA menjadi 6 (seharusnya 5)
- Total capacity salah (7175.04 kWp seharusnya 5707.065 kWp)
- Perhitungan POA weighted average salah

### Dampak Keseluruhan

1. **Perhitungan POA MMKI 2 salah dari awal** - Sensor yang salah ikut terhitung dalam weighted average POA
2. **Data historis perlu di-recalculate** - Semua data POA MMKI 2 yang sudah terhitung sebelumnya menggunakan konfigurasi yang salah
3. **Sensor fisik MMKI 2 yang lama masih muncul** - Harus di-exclude ketika POA_OVERRIDE aktif

---

## ✅ Perbaikan yang Dilakukan

### Perbaikan 1: Update Seed Configuration

**Perubahan di `seed_sensor_config.csv`:**

**Sebelum:**
```csv
FusionSolar;NE=51758766;IRR-AMB-NW-Bod Stamp;EM011023C7355634;POA;<capacity_value>
```

**Sesudah:**
```csv
FusionSolar;NE=51758766;IRR-AMB-NW-Bod Stamp;EM011023C7355634;;
```

**Perubahan:**
- `sensor_type` diubah dari `POA` menjadi kosong (tidak ada tipe)
- `sensor_capacity` dihapus (kosong)

**Catatan Penting:** Seed tetap menyimpan data statis bahwa sensor MMKI 2 yang lama adalah POA dengan kapasitas (data referensi/historis). Logika exclude diterapkan di model, bukan di seed.

### Perbaikan 2: Logika Exclude di Model

**Masalah:** Sensor fisik MMKI 2 yang lama masih muncul padahal sudah digantikan oleh sensor MMKI 1 yang di-override.

**Solusi:** Menambahkan logika exclude di model untuk mengecualikan sensor fisik dari logical site ketika POA_OVERRIDE aktif.

**Perubahan di Model:**

1. **`mart_sensor_daily.sql`** - Menambahkan CTE `logical_site_override_check` dan filter exclude di `daily_sensor_with_override`
2. **`fact_sensor_calculations_5min.sql`** - Menambahkan CTE `logical_site_override_check` dan filter exclude di `poa_with_override`

**Logika Exclude:**
- Ketika ada POA_OVERRIDE aktif untuk logical site (misalnya "PT. MMKI 5.7 MWp - Phase 2")
- Dan sensor fisik berasal dari logical site tersebut (physical_site_name = logical_site_id)
- Dan sensor tersebut tidak punya POA_OVERRIDE (bukan sensor yang di-override)
- Dan tanggal berada dalam periode override efektif
- Maka sensor fisik tersebut di-exclude dari logical site tersebut

### Sensor POA yang Valid untuk MMKI 2

Setelah perbaikan, sensor POA yang valid untuk MMKI 2 adalah **sensor dari MMKI 1 yang di-override** (sejak 2025-09-15):

| Device ID | Sensor Name | Capacity (kWp) | Source |
|-----------|-------------|----------------|--------|
| EM03102287046729 | IRR-MOD-NW-Bod-Stamp | 1467.975 | MMKI 1 (Override) |
| EM04102287046729 | IRR-MOD-NW-ASSY2 | 973.47 | MMKI 1 (Override) |
| EM05102287046729 | IRR-AMB-SE-ASSY | 1078.92 | MMKI 1 (Override) |
| EM06102287046729 | IRR-MOD-NW-ASSEMBLY | 1365.855 | MMKI 1 (Override) |
| EM07102287046729 | IRR-AMB-NE-ASSY1 | 820.845 | MMKI 1 (Override) |

**Total Capacity:** 5707.065 kWp

**Catatan:** Sensor fisik MMKI 2 yang lama (EM001023C7355634, EM011023B7436516, EM021023B7436516, EM031023B7436516, EM051023B7436516) **tidak digunakan** karena sudah digantikan oleh sensor MMKI 1 yang di-override.

---

## 📊 Dependency Chain yang Terpengaruh

Perubahan konfigurasi sensor dan logika exclude mempengaruhi seluruh pipeline dari level 5min hingga daily:

```
mart_sensor_measurements_5min (source data)
    ↓
fact_sensor_calculations_5min (filter POA + exclude logic, unique_key: ['timestamp', 'sensor_id'])
    ↓
fact_inverter_calculations_5min (menggunakan MIT dari fact_sensor)
    ↓
fact_site_calculations_5min (aggregasi dari fact_inverter)
    ↓
mart_sensor_daily (filter sensor_type IS NOT NULL + exclude logic, unique_key: ['date_key', 'asset_id', 'sensor_type'])
    ↓
mart_site_performance_daily (menggunakan POA dari mart_sensor_daily, unique_key: ['date_key', 'site_id'])
```

**Semua models di atas perlu di-recalculate** dengan logika exclude yang baru.

## 🔧 Logika Exclude untuk POA Override

### Konsep

Ketika ada POA_OVERRIDE aktif untuk logical site, sensor fisik dari logical site tersebut harus di-exclude karena sudah digantikan oleh sensor dari site lain yang di-override.

### Implementasi

**Di `mart_sensor_daily.sql` dan `fact_sensor_calculations_5min.sql`:**

1. **CTE `logical_site_override_check`**: Mendeteksi logical site yang punya POA_OVERRIDE aktif
2. **Filter Exclude**: Mengecualikan sensor fisik dari logical site ketika:
   - Physical site name = logical site (sensor berasal dari logical site itu sendiri)
   - POA_OVERRIDE aktif untuk logical site tersebut
   - Sensor tidak punya POA_OVERRIDE (bukan sensor yang di-override)
   - Tanggal berada dalam periode override efektif

### Contoh: MMKI 2

**Sebelum Logika Exclude:**
- Sensor fisik MMKI 2 (EM001023C7355634, dll) muncul sebagai POA
- Sensor MMKI 1 yang di-override (EM03102287046729, dll) muncul sebagai POA
- **Total: 6 sensor** ❌

**Sesudah Logika Exclude:**
- Sensor fisik MMKI 2 (EM001023C7355634, dll) **di-exclude** (tidak muncul)
- Sensor MMKI 1 yang di-override (EM03102287046729, dll) muncul sebagai POA
- **Total: 5 sensor** ✅

### Catatan Penting

- **Seed tetap menyimpan data statis**: Sensor MMKI 2 yang lama tetap memiliki `sensor_type='POA'` di seed (data referensi/historis)
- **Logika exclude di model**: Sensor fisik MMKI 2 yang lama di-exclude oleh logika di model, bukan di seed
- **Date-aware**: Logika exclude hanya berlaku untuk tanggal >= `effective_date_start` dari POA_OVERRIDE

---

## 🔄 Cara Memperbaiki Data Historis

### ⚠️ Masalah Data Historis

**Penting:** Perubahan konfigurasi sensor dan logika exclude mempengaruhi **semua layer** dari 5min hingga daily. Data historis yang salah perlu di-recalculate dengan logika baru.

**Models yang Terpengaruh:**

1. **`mart_sensor_measurements_5min`** ✅ **PERLU RE-RUN**
   - LEFT JOIN `seed_sensor_config` (line 148)
   - Mengambil `sensor_type` sebagai kolom dari seed
   - Unique key: `['timestamp', 'asset_id', 'metric_id']` - **TIDAK termasuk sensor_type**
   - **Data historis masih memiliki `sensor_type='POA'`** (dari seed lama)
   - Ketika seed di-reload dengan sensor_type NULL, data baru akan memiliki sensor_type NULL
   - Tapi data historis perlu di-re-run untuk update sensor_type dari 'POA' menjadi NULL
   - **Perlu re-run untuk periode historis** agar sensor_type ter-update

2. **`fact_sensor_calculations_5min`** ✅ **PERLU DIPERBAIKI**
   - JOIN `seed_sensor_config` dengan filter `WHERE sc.sensor_type = 'POA'` (line 220)
   - Unique key: `['timestamp', 'sensor_id']`
   - Data historis dengan `sensor_id='FS_EM011023C7355634'` masih ada
   - **Perlu hapus data historis yang salah**

3. **`fact_inverter_calculations_5min`** ✅ **PERLU RE-RUN**
   - Menggunakan MIT dari `fact_sensor_calculations_5min` (line 68)
   - Unique key: `['timestamp', 'inverter_id']`
   - **Perlu re-run** setelah `fact_sensor_calculations_5min` diperbaiki

4. **`fact_site_calculations_5min`** ✅ **PERLU RE-RUN**
   - Menggunakan `fact_inverter_calculations_5min`
   - Unique key: `['timestamp', 'site_id']`
   - **Perlu re-run** setelah `fact_inverter_calculations_5min` diperbaiki

5. **`mart_sensor_daily`** ✅ **PERLU DIPERBAIKI**
   - LEFT JOIN `seed_sensor_config`, filter `WHERE sensor_type IS NOT NULL` (line 136)
   - Unique key: `['date_key', 'asset_id', 'sensor_type']`
   - Data historis dengan `sensor_type='POA'` untuk device `EM011023C7355634` masih ada
   - **Perlu hapus data historis yang salah**

6. **`mart_site_performance_daily`** ✅ **PERLU RE-RUN**
   - Menggunakan `mart_sensor_daily` dengan filter `sensor_type = 'POA'` (line 403)
   - Unique key: `['date_key', 'site_id']`
   - Perhitungan POA weighted average perlu di-recalculate
   - **Perlu re-run** setelah `mart_sensor_daily` diperbaiki

**Kenapa Data Lama Tidak Terhapus Otomatis?**
- `mart_sensor_measurements_5min`: Unique key `['timestamp', 'asset_id', 'metric_id']` tidak termasuk sensor_type. Data historis masih memiliki `sensor_type='POA'` dari seed lama. Perlu re-run untuk update sensor_type menjadi NULL.
- `fact_sensor_calculations_5min`: Menggunakan JOIN dengan filter `WHERE sc.sensor_type = 'POA'`, jadi data baru tidak muncul. Tapi data lama dengan `sensor_id='FS_EM011023C7355634'` masih ada di table karena unique_key `['timestamp', 'sensor_id']` tidak termasuk sensor_type.
- `mart_sensor_daily`: Data baru tidak muncul karena `WHERE sensor_type IS NOT NULL`, tapi data lama dengan unique_key berbeda (`sensor_type='POA'`) masih ada karena unique_key `['date_key', 'asset_id', 'sensor_type']` termasuk sensor_type.
- Unique key yang berbeda atau tidak termasuk sensor_type → data lama tidak ter-overwrite secara otomatis

### Step 1: Reload Seed Configuration

Pertama, pastikan seed configuration sudah ter-update di database:

```bash
dbt seed --select seed_sensor_config
```

### Step 2: Re-run `mart_sensor_measurements_5min` (Hanya Device yang Salah)

**Re-run untuk update sensor_type dari 'POA' menjadi NULL (hanya untuk device yang salah):**

```bash
dbt run --select mart_sensor_measurements_5min --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-12-31", "reingest_device_ids": "EM011023C7355634"}'
```

**Catatan:** 
- Karena unique_key `['timestamp', 'asset_id', 'metric_id']` tidak termasuk sensor_type, re-run akan update existing rows dengan sensor_type baru (NULL) dari seed yang sudah di-reload.
- Filter `reingest_device_ids` berlaku untuk semua system (iSolarCloud dan FusionSolar).
- Filter berdasarkan device ID yang spesifik lebih tepat daripada filter berdasarkan plant_code.

### Step 3: Update Data Historis yang Salah (Opsional)

**Catatan:** Dengan logika exclude yang baru, data historis akan otomatis di-exclude ketika model di-re-run. Namun, jika ingin mempercepat proses, bisa update data historis secara manual:

#### 3.1. Update `fact_sensor_calculations_5min` (Hanya MMKI 2)

**Update sensor_type menjadi NULL untuk sensor yang salah:**

```sql
-- Update data POA yang salah dari fact_sensor_calculations_5min (hanya MMKI 2)
UPDATE mart.fact_sensor_calculations_5min
SET sensor_type = NULL
WHERE sensor_id = 'FS_EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2';
```

**Atau dengan date range:**

```sql
-- Update data untuk periode tertentu (hanya MMKI 2)
UPDATE mart.fact_sensor_calculations_5min
SET sensor_type = NULL
WHERE sensor_id = 'FS_EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND date_key >= '2025-01-01'
    AND date_key <= '2025-12-31';
```

#### 3.2. Update `mart_sensor_daily` (Hanya MMKI 2)

**Update sensor_type menjadi NULL untuk sensor yang salah:**

```sql
-- Update data historis yang salah dari mart_sensor_daily (hanya MMKI 2)
UPDATE mart.mart_sensor_daily
SET sensor_type = NULL
WHERE device_id = 'EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2';
```

**Atau dengan date range:**

```sql
-- Update data untuk periode tertentu (hanya MMKI 2)
UPDATE mart.mart_sensor_daily
SET sensor_type = NULL
WHERE device_id = 'EM011023C7355634'
    AND sensor_type = 'POA'
    AND site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND date_key >= '2025-01-01'
    AND date_key <= '2025-12-31';
```

**Catatan:** Update ini opsional karena logika exclude di model akan otomatis mengecualikan sensor fisik MMKI 2 yang lama ketika POA_OVERRIDE aktif.

### Step 4: Re-run dbt Models

#### Option A: Re-run Hanya MMKI 2 (Recommended untuk Testing)

Setelah update seed dan logika exclude, re-run **semua models yang terpengaruh (hanya untuk MMKI 2)** dengan satu command:

```bash
dbt run --select mart_sensor_measurements_5min+ --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-12-31", "reingest_plant_codes": "NE=51758766"}'
```

**Penjelasan:**
- `mart_sensor_measurements_5min+` = mart_sensor_measurements_5min dan semua downstream dependencies
- `reingest_plant_codes: "NE=51758766"` = Filter hanya untuk MMKI 2
- Models yang akan di-re-run:
  1. `mart_sensor_measurements_5min` (update sensor_type dari 'POA' menjadi NULL untuk sensor yang salah)
  2. `fact_sensor_calculations_5min` (apply logika exclude untuk sensor fisik MMKI 2 yang lama)
  3. `fact_inverter_calculations_5min` (menggunakan MIT dari fact_sensor)
  4. `fact_site_calculations_5min` (aggregasi dari fact_inverter)
  5. `mart_sensor_daily` (apply logika exclude untuk sensor fisik MMKI 2 yang lama)
  6. `mart_site_performance_daily` (menggunakan POA dari mart_sensor_daily)

**Catatan:** 
- Filter `reingest_plant_codes` berlaku untuk FusionSolar system.
- Logika exclude akan otomatis mengecualikan sensor fisik MMKI 2 yang lama ketika POA_OVERRIDE aktif (sejak 2025-09-15).
- Models downstream akan otomatis hanya memproses data dari MMKI 2 karena mereka depend pada `mart_sensor_measurements_5min` yang sudah di-filter.
- Semua models akan di-run secara berurutan sesuai dependency order.

#### Option B: Full Refresh untuk Semua Site (Production)

Untuk menerapkan logika exclude ke **semua site** (termasuk site lain yang mungkin punya POA_OVERRIDE), gunakan **full refresh**:

```bash
# Full refresh untuk semua models yang terpengaruh
dbt run --select mart_sensor_measurements_5min+ --full-refresh
```

**Atau dengan date range tertentu:**

```bash
# Full refresh dengan date range (untuk data historis tertentu)
dbt run --select mart_sensor_measurements_5min+ --full-refresh --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-12-31"}'
```

**Penjelasan:**
- `--full-refresh` = Rebuild seluruh table dari scratch (hapus data lama, insert data baru)
- Tanpa filter site = Semua site akan di-process
- Logika exclude akan diterapkan ke semua site yang punya POA_OVERRIDE aktif
- **Waktu eksekusi lebih lama** karena memproses semua data historis

**Kapan Menggunakan Full Refresh?**
- ✅ Setelah implementasi logika exclude baru (seperti sekarang)
- ✅ Ketika ada perubahan logika yang mempengaruhi semua site
- ✅ Untuk memastikan konsistensi data di semua site
- ⚠️ **Hati-hati**: Full refresh akan memproses semua data historis, bisa memakan waktu lama

**Catatan Penting:**
- Full refresh akan **menghapus semua data lama** dan rebuild dari scratch
- Pastikan backup database sebelum full refresh jika diperlukan
- Untuk production, pertimbangkan untuk full refresh di waktu maintenance window

### Step 5: Verifikasi Hasil

Setelah re-run, verifikasi bahwa:

1. **Sensor yang salah sudah tidak ikut dalam perhitungan POA:**
```sql
-- Cek sensor POA yang digunakan untuk MMKI 2
SELECT 
    date_key,
    site_name,
    device_id,
    sensor_dev_name,
    sensor_type,
    sensor_capacity_kwp,
    daily_irradiance_kwh_m2
FROM mart.mart_sensor_daily
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND sensor_type = 'POA'
    AND date_key >= '2025-01-01'
ORDER BY date_key DESC, device_id;

-- Pastikan EM011023C7355634 TIDAK muncul dengan sensor_type = 'POA'
```

2. **Perhitungan POA Weighted Average sudah benar:**
```sql
-- Cek perhitungan POA weighted untuk MMKI 2
SELECT 
    sp.date_key,
    sp.site_name,
    sp.daily_poa_weighted_kwh_m2,
    -- Manual calculation untuk verifikasi
    SUM(sd.daily_irradiance_kwh_m2 * sd.sensor_capacity_kwp) / 
        NULLIF(SUM(sd.sensor_capacity_kwp), 0) as manual_poa_weighted
FROM mart.mart_site_performance_daily sp
LEFT JOIN mart.mart_sensor_daily sd
    ON sp.date_key = sd.date_key
    AND sp.site_name = sd.site_name
    AND sd.sensor_type = 'POA'
WHERE sp.site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND sp.date_key >= '2025-01-01'
GROUP BY sp.date_key, sp.site_name, sp.daily_poa_weighted_kwh_m2
ORDER BY sp.date_key DESC
LIMIT 10;
```

3. **Jumlah sensor POA yang digunakan:**
```sql
-- Hitung jumlah sensor POA per hari untuk MMKI 2
SELECT 
    date_key,
    COUNT(DISTINCT device_id) as poa_sensor_count,
    SUM(sensor_capacity_kwp) as total_capacity_kwp
FROM mart.mart_sensor_daily
WHERE site_name = 'PT. MMKI 5.7 MWp - Phase 2'
    AND sensor_type = 'POA'
    AND date_key >= '2025-01-01'
GROUP BY date_key
ORDER BY date_key DESC
LIMIT 30;
```

**Expected Result:**
- Jumlah sensor POA harus **5 sensor** (sensor dari MMKI 1 yang di-override, bukan 6)
- Total capacity harus **5707.065 kWp** (bukan 7175.04 kWp)
- Sensor fisik MMKI 2 yang lama (EM001023C7355634, EM011023B7436516, dll) **tidak muncul** untuk tanggal >= 2025-09-15

---

## 📊 Dampak Perubahan

### Perhitungan POA Sebelum vs Sesudah

**Sebelum (Salah):**
```
POA_weighted = SUM(POA_i × capacity_i) / SUM(capacity_i)
dimana i = 6 sensor (termasuk EM011023C7355634 yang salah)
```

**Sesudah (Benar):**
```
POA_weighted = SUM(POA_i × capacity_i) / SUM(capacity_i)
dimana i = 5 sensor (hanya sensor POA yang valid)
```

### Dampak pada Metrik Lain

Perubahan ini akan mempengaruhi:

1. **Daily POA (kWh/m²)** - Nilai akan berubah karena denominator (total capacity) berubah
2. **Performance Ratio (PR)** - Akan berubah karena PR = Energy / (POA × Capacity)
3. **MIT (Minimum Irradiance Threshold)** - Jika menggunakan POA untuk MIT calculation
4. **Power Available Hours** - Jika menggunakan POA untuk availability calculation

---

## ⚠️ Catatan Penting

1. **Data Historis:** Semua data historis MMKI 2 perlu di-recalculate untuk konsistensi
2. **Downstream Impact:** Pastikan semua downstream models yang depend pada POA juga ter-update
3. **Verification:** Selalu verifikasi hasil setelah re-run untuk memastikan perhitungan sudah benar
4. **Documentation:** Update dokumentasi lain jika ada yang mereferensikan sensor ini sebagai POA

---

## 🔗 Related Files

- `dbt/seeds/seed_sensor_config.csv` - Konfigurasi sensor
- `dbt/models/marts/mart_sensor_daily.sql` - Daily sensor aggregations
- `dbt/models/marts/mart_site_performance_daily.sql` - Daily POA weighted calculation
- `dbt/docs/REINGESTION_WORKFLOW.md` - Panduan re-ingestion

---

## ✅ Checklist

- [x] Update `seed_sensor_config.csv` - Hapus sensor_type POA dari EM011023C7355634
- [x] Implementasi logika exclude di `mart_sensor_daily.sql`
- [x] Implementasi logika exclude di `fact_sensor_calculations_5min.sql`
- [x] Re-run `mart_sensor_daily` untuk data historis dengan logika exclude baru
- [x] Re-run `fact_sensor_calculations_5min` untuk data historis dengan logika exclude baru
- [x] Verifikasi sensor POA yang digunakan (harus 5 sensor dari MMKI 1 yang di-override)
- [x] Verifikasi sensor fisik MMKI 2 yang lama sudah tidak muncul (untuk tanggal >= 2025-09-15)
- [x] Verifikasi perhitungan POA weighted average (total capacity = 5707.065 kWp)
- [ ] Verifikasi dampak pada metrik lain (PR, MIT, dll)
- [x] Update dokumentasi

---

---

## 📋 Quick Reference: Semua Command dalam 1 Line

Berikut adalah semua command yang diperlukan untuk memperbaiki konfigurasi POA MMKI 2:

### 1. Reload Seed Configuration
```bash
dbt seed --select seed_sensor_config
```

### 2. Re-run mart_sensor_measurements_5min (Update sensor_type) - Opsional
```bash
dbt run --select mart_sensor_measurements_5min --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-12-31", "reingest_device_ids": "EM011023C7355634"}'
```

### 3. Update Data Historis yang Salah (Opsional)

**Update dari fact_sensor_calculations_5min:**
```sql
UPDATE mart.fact_sensor_calculations_5min SET sensor_type = NULL WHERE sensor_id = 'FS_EM011023C7355634' AND sensor_type = 'POA' AND site_name = 'PT. MMKI 5.7 MWp - Phase 2';
```

**Update dari mart_sensor_daily:**
```sql
UPDATE mart.mart_sensor_daily SET sensor_type = NULL WHERE device_id = 'EM011023C7355634' AND sensor_type = 'POA' AND site_name = 'PT. MMKI 5.7 MWp - Phase 2';
```

**Catatan:** Update ini opsional karena logika exclude di model akan otomatis mengecualikan sensor fisik MMKI 2 yang lama ketika POA_OVERRIDE aktif.

### 4. Re-run Semua Models

**Option A: Hanya MMKI 2 (Testing/Development)**
```bash
dbt run --select mart_sensor_measurements_5min+ --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-12-31", "reingest_plant_codes": "NE=51758766"}'
```

**Option B: Full Refresh Semua Site (Production) - RECOMMENDED**
```bash
dbt run --select mart_sensor_measurements_5min+ --full-refresh
```

**Atau dengan date range (untuk data historis tertentu):**
```bash
dbt run --select mart_sensor_measurements_5min+ --full-refresh --vars '{"reingest_start_date": "2025-01-01", "reingest_end_date": "2025-12-31"}'
```

**Urutan Eksekusi:**
1. Jalankan Step 1 (reload seed)
2. Jalankan Step 2 (re-run mart_sensor_measurements_5min - opsional, untuk update sensor_type)
3. Jalankan Step 3 (update data historis - 2 SQL commands, opsional)
4. Jalankan Step 4 (re-run semua models dengan logika exclude baru)
   - **Option A**: Hanya MMKI 2 (lebih cepat, untuk testing)
   - **Option B**: Full refresh semua site (lebih lama, untuk production) ⭐ **RECOMMENDED untuk production**

**Catatan Full Refresh:**
- `--full-refresh` akan **menghapus semua data lama** dan rebuild dari scratch
- Logika exclude akan diterapkan ke **semua site** yang punya POA_OVERRIDE aktif
- Pastikan backup database sebelum full refresh jika diperlukan
- Untuk production, pertimbangkan untuk full refresh di waktu maintenance window

---

**Last Updated:** 2025-12-17  
**Updated By:** Data Engineering Team  
**Issue:** 
1. Sensor IRR-AMB-NW-Bod Stamp (EM011023C7355634) salah dikategorikan sebagai POA
2. Sensor fisik MMKI 2 yang lama masih muncul padahal sudah digantikan oleh sensor MMKI 1 yang di-override

**Solution:**
1. Update seed configuration untuk menghapus sensor_type POA dari EM011023C7355634
2. Menambahkan logika exclude di model untuk mengecualikan sensor fisik dari logical site ketika POA_OVERRIDE aktif

