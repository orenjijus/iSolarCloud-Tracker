# Excel Format Validation Guide - Shoetown Ligung Indonesia

## ⚠️ Masalah yang Ditemukan

### 1. Capacity Issue (CRITICAL)

**Problem:**
- `SLI-IRR-2-A` dan `SLI-IRR-4-F` memiliki capacity **76,328 kWp** di database
- Seharusnya: **763.28 kWp** (100x lebih kecil!)
- Ini menyebabkan weighted average salah

**Root Cause:**
- Seed file menggunakan koma desimal (763,28)
- Parsing koma mungkin tidak bekerja dengan benar
- Perlu cek apakah seed sudah di-reload setelah update

**Impact:**
- Weighted average di database: **8.965** (salah)
- Weighted average di Excel: **7.181** (benar, menggunakan capacity 763.28)

### 2. Format Excel - AMAN dengan Catatan

Format Excel Anda **AMAN** untuk validasi, dengan beberapa catatan:

✅ **Yang Sudah Benar:**
- Column names menggunakan underscore (SLI_IRR_1_A) - OK
- Date format DD/MM/YYYY - OK
- Weighted_Avg_POA column - OK
- Precision (9 decimals) - OK

⚠️ **Yang Perlu Diperhatikan:**
- `site_id` column dengan format `ISO_SITE_DD/MM/YYYY` - tidak standar, tapi OK untuk validasi
- Pastikan capacity yang digunakan di Excel sesuai dengan seed file (763.28, bukan 76328)

---

## Format Excel yang Disarankan

### Struktur Kolom (Sesuai dengan Excel Anda):

| Column | Header | Format | Description |
|--------|--------|--------|-------------|
| A | site_id | Text | ISO_SITE_DD/MM/YYYY (optional, untuk reference) |
| B | Date | Date | DD/MM/YYYY |
| C | SLI_IRR_1_A | Number (9 decimals) | POA sensor 1-A |
| D | SLI_IRR_2_A | Number (9 decimals) | POA sensor 2-A |
| E | SLI_IRR_3_F | Number (9 decimals) | POA sensor 3-F |
| F | SLI_IRR_4_F | Number (9 decimals) | POA sensor 4-F |
| G | **Weighted_Avg_POA** | Number (9 decimals) | **Nilai utama untuk dibandingkan** |

### Tambahkan Kolom Validasi:

| Column | Formula | Purpose |
|--------|---------|---------|
| H | `=SUMPRODUCT(C2:F2, {928; 763.28; 928; 763.28}) / SUM({928; 763.28; 928; 763.28})` | Manual Weighted Avg |
| I | `=G2-H2` | Difference (DB vs Excel) |
| J | `=ABS(I2)` | Absolute Difference |
| K | `=IF(J2<0.001, "✓ OK", "✗ CHECK")` | Validation Flag |

**Note:** Capacity values: SLI-IRR-1-A=928, SLI-IRR-2-A=763.28, SLI-IRR-3-F=928, SLI-IRR-4-F=763.28

---

## Langkah Perbaikan

### Step 1: Fix Capacity Issue

1. **Cek seed file** (`dbt/seeds/seed_sensor_config.csv`):
   - Line 83: `SLI-IRR-2-A` harus `763,28` (dengan koma)
   - Line 87: `SLI-IRR-4-F` harus `763,28` (dengan koma)

2. **Reload seed:**
   ```bash
   dbt seed --select seed_sensor_config
   ```

3. **Re-run mart_sensor_daily:**
   ```bash
   dbt run --select mart_sensor_daily
   ```

### Step 2: Validasi dengan Excel

1. **Jalankan query** `validate_shoetown_november_excel_format.sql`
2. **Copy hasil** ke Excel
3. **Bandingkan** kolom `Weighted_Avg_POA` dengan Excel
4. **Cek kolom debug** (Sum_POA_x_Capacity, Sum_Capacity, Cap_*) untuk verifikasi

### Step 3: Verifikasi Capacity

Setelah fix, cek capacity di database:
```sql
SELECT 
    sensor_dev_name,
    sensor_capacity_kwp
FROM mart.mart_sensor_daily
WHERE site_name = 'Shoetown Ligung Indonesia'
    AND sensor_type = 'POA'
GROUP BY sensor_dev_name, sensor_capacity_kwp;
```

**Expected values:**
- SLI-IRR-1-A: 928.00
- SLI-IRR-2-A: **763.28** (bukan 76328!)
- SLI-IRR-3-F: 928.00
- SLI-IRR-4-F: **763.28** (bukan 76328!)

---

## Query untuk Validasi

Gunakan query `queries/validate_shoetown_november_excel_format.sql` yang sudah disesuaikan dengan format Excel Anda.

Query ini akan output:
- Format `site_id` sesuai Excel (ISO_SITE_DD/MM/YYYY)
- Column names dengan underscore (SLI_IRR_1_A, dll)
- Weighted_Avg_POA dengan 9 decimals
- Kolom debug untuk verifikasi capacity

---

## Kesimpulan

✅ **Format Excel AMAN** - struktur dan format sudah sesuai

⚠️ **Perlu Fix Capacity** - ada masalah parsing koma desimal yang menyebabkan capacity 100x lebih besar

🔧 **Action Required:**
1. Reload seed_sensor_config
2. Re-run mart_sensor_daily
3. Validasi ulang dengan Excel

Setelah fix capacity, weighted average di database harus match dengan Excel (7.181, bukan 8.965).

