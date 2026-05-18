# dim_assets — Panduan Lengkap

Dokumen master untuk `dim_assets`: cara memastikan ter-refresh, best practice, solusi auto-refresh, dan FAQ (mengapa tidak ikut run).

---

## 1. Overview

- **`dim_assets`** bergantung pada `stg_*__sites`, `stg_*__devices`, dan `seed_site_config`.
- Banyak model downstream (fact_*, mart_*) bergantung pada `dim_assets`; jika staging sites/devices berubah, `dim_assets` harus di-refresh.
- **`dim_date_generated`** bersifat statis; tidak perlu di-refresh setiap run.

---

## 2. Quick reference — Memastikan dim_assets terikut

### Setelah sync plant/devices (disarankan)

```bash
dbt run --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

Ini akan include: staging sites/devices → `dim_assets` → semua downstream yang depend pada `dim_assets`.

### Daily run biasa

```bash
dbt run
# atau
dbt run --select tag:daily_staging tag:daily_dimensions tag:daily_marts_measurement_5min
# atau
dbt run --select staging dimensions marts
```

### Hanya refresh dim_assets

```bash
dbt run --select tag:daily_dimensions
# atau
dbt run --select dimensions.dim_assets
```

### Verifikasi sebelum run

```bash
dbt list --select stg_isolarcloud__sites+ stg_fusionsolar__sites+ stg_isolarcloud__devices+ stg_fusionsolar__devices+
```

Jika `dim_assets` muncul di list, ia akan terikut.

---

## 3. Best practice

- **Selalu refresh `dim_assets`** setiap kali staging sites/devices berubah.
- Gunakan selector `+` agar dependency chain ikut (staging → dim_assets → marts).
- `dim_assets` punya tag `daily_dimensions`; bisa di-include via `tag:daily_dimensions` atau `tag:dimensions`.
- Runtime `dim_assets` sangat singkat (~0.25s); aman di-include setiap run.

---

## 4. Solusi: Auto-refresh setelah sync

**Masalah**: Staging sites/devices saat ini berupa VIEW; dbt tidak menganggap perubahan di raw sebagai “perubahan” pada VIEW, sehingga `dim_assets` tidak otomatis di-refresh.

**Opsi**:

1. **Explicit select (disarankan)**  
   Setelah sync, jalankan dengan selector yang include staging + downstream (lihat Quick reference di atas).

2. **Ubah staging sites/devices jadi TABLE**  
   Dengan materialized table, dbt bisa mendeteksi perubahan; `dim_assets` akan ikut refresh saat staging di-run. Trade-off: storage kecil (~519 rows), runtime <1s. Detail implementasi ada di arsip `SOLUTION_DIM_ASSETS_AUTO_REFRESH.md`.

---

## 5. FAQ — Kenapa dim_assets tidak ikut run?

**Penyebab**: `dim_assets` adalah TABLE; dbt akan skip jika menganggap dependency tidak berubah. Dependency-nya (stg_*__sites, stg_*__devices) adalah VIEW; VIEW selalu “fresh” di query-time sehingga dbt menganggap tidak ada perubahan → `dim_assets` di-skip.

**Solusi**: Selalu include `dim_assets` secara eksplisit (selector `+` dari staging sites/devices, atau `tag:daily_dimensions`, atau `dimensions`).

---

*Konsolidasi dari: BEST_PRACTICE_DIM_ASSETS, QUICK_REFERENCE_DIM_ASSETS, SOLUTION_DIM_ASSETS_AUTO_REFRESH, WHY_DIM_ASSETS_NOT_RUN. File asli dapat diarsipkan atau dihapus setelah konfirmasi.*
