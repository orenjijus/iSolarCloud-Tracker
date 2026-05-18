# Seed Management - Panduan Lengkap

## 🎯 Overview

Seeds di dbt adalah file CSV yang di-load ke database sebagai tabel. **Penting:** Seeds **TIDAK otomatis di-run** saat `dbt run`. Seeds harus di-run secara manual dengan `dbt seed`.

## 📊 Kategori Seeds

### 1. Static Seeds (Tidak Perlu Update)

Seeds yang sudah disesuaikan dengan sistem dan **tidak perlu di-update** kecuali ada perubahan sistem:

- ✅ **`seed_metric_mapper`** - Mapping metric ID ke unified name
  - **Alasan:** Sudah disesuaikan dengan sistem, jika salah berarti mapping dari awal salah
  - **Update:** Hanya jika ada perubahan sistem atau penambahan metric baru

### 2. Dynamic Seeds (Perlu Update Berkala)

Seeds yang perlu di-update secara berkala karena nilai bisa berubah:

- 🔄 **`seed_daily_simulation_target`** - Target simulasi harian
  - **Update:** Bulanan atau saat ada rekalkulasi/adjustment
  - **Alasan:** Nilai bisa berubah karena rekalkulasi atau adjustment

- 🔄 **`seed_daily_kpi_monthly`** - KPI bulanan
  - **Update:** Bulanan atau saat ada rekalkulasi
  - **Alasan:** Nilai bisa belum ada di bulan tertentu, perlu update saat sudah ada

- 🔄 **`seed_energy_adjustment_daily`** - Adjustment energy harian
  - **Update:** Saat ada rekalkulasi atau adjustment
  - **Alasan:** Nilai bisa berubah karena rekalkulasi

- 🔄 **`seed_ghi_adjustment_daily`** - Adjustment GHI harian
  - **Update:** Saat ada rekalkulasi atau adjustment
  - **Alasan:** Nilai bisa berubah karena rekalkulasi

### 3. Config Seeds (Update Saat Ada Perubahan Config)

Seeds yang berisi konfigurasi, update hanya saat ada perubahan:

- ⚙️ **`seed_sensor_config`** - Konfigurasi sensor
  - **Update:** Saat ada perubahan kapasitas sensor atau penambahan sensor baru

- ⚙️ **`seed_meter_config`** - Konfigurasi meter
  - **Update:** Saat ada perubahan konfigurasi meter atau penambahan meter baru

- ⚙️ **`seed_site_config`** - Konfigurasi site
  - **Update:** Saat ada perubahan kapasitas, tariff, atau konfigurasi site

- ⚙️ **`seed_sensor_site_mapping`** - Mapping sensor ke site
  - **Update:** Saat ada perubahan mapping (POA_OVERRIDE, GHI_FALLBACK)

- ⚙️ **`seed_inverter_site_mapping`** - Mapping inverter ke site
  - **Update:** Saat ada perubahan mapping inverter

- ⚙️ **`seed_issue_dates`** - Tanggal issue/maintenance
  - **Update:** Saat ada tanggal issue baru atau perubahan

## 🔄 Workflow Update Seeds

### Update Seeds yang Perlu Update Berkala

**Option 1: Menggunakan Script (Recommended)**

```powershell
# Windows PowerShell
cd dbt
.\scripts\update_dynamic_seeds.ps1

# Update seed tertentu
.\scripts\update_dynamic_seeds.ps1 seed_daily_kpi_monthly
```

```bash
# Linux/Mac
cd dbt
chmod +x scripts/update_dynamic_seeds.sh
./scripts/update_dynamic_seeds.sh

# Update seed tertentu
./scripts/update_dynamic_seeds.sh seed_daily_kpi_monthly
```

**Option 2: Manual Command**

```bash
cd dbt

# Update simulation targets
dbt seed --select seed_daily_simulation_target

# Update KPI monthly
dbt seed --select seed_daily_kpi_monthly

# Update energy adjustment
dbt seed --select seed_energy_adjustment_daily

# Update GHI adjustment
dbt seed --select seed_ghi_adjustment_daily

# Update semua dynamic seeds sekaligus
dbt seed --select seed_daily_simulation_target seed_daily_kpi_monthly seed_energy_adjustment_daily seed_ghi_adjustment_daily
```

### Update Seeds Config (Saat Ada Perubahan)

```bash
# Update sensor config
dbt seed --select seed_sensor_config

# Update meter config
dbt seed --select seed_meter_config

# Update site config
dbt seed --select seed_site_config

# Update sensor mapping
dbt seed --select seed_sensor_site_mapping

# Update inverter mapping
dbt seed --select seed_inverter_site_mapping

# Update issue dates
dbt seed --select seed_issue_dates
```

### Update Seed Metric Mapper (Hanya Saat Perlu)

```bash
# Hanya update jika ada perubahan sistem atau penambahan metric
dbt seed --select seed_metric_mapper
```

## 📝 Best Practice

### 1. Update Seeds Sebelum dbt run

Jika seeds berubah, update seeds **sebelum** menjalankan `dbt run`:

```bash
# Step 1: Update seeds yang berubah
dbt seed --select seed_daily_kpi_monthly seed_energy_adjustment_daily

# Step 2: Run models
dbt run
```

### 2. Full Refresh untuk Seeds yang Berubah

Jika ada perubahan struktur atau banyak perubahan, gunakan `--full-refresh`:

```bash
dbt seed --select seed_daily_kpi_monthly --full-refresh
```

### 3. Verifikasi Seeds Setelah Update

```sql
-- Cek jumlah rows
SELECT COUNT(*) FROM staging.seed_daily_kpi_monthly;

-- Cek data terbaru
SELECT MAX(date_key) as max_date, COUNT(*) as total_rows
FROM staging.seed_daily_kpi_monthly;
```

## 🚨 Catatan Penting

1. **Seeds TIDAK otomatis di-run saat `dbt run`**
   - Seeds harus di-run manual dengan `dbt seed`
   - Jika seeds berubah tapi tidak di-run, models akan menggunakan data lama

2. **Update Seeds yang Perlu Update Berkala**
   - Simulation targets: Bulanan atau saat rekalkulasi
   - KPI monthly: Bulanan atau saat rekalkulasi
   - Adjustments: Saat ada rekalkulasi atau adjustment

3. **Seed Metric Mapper Tidak Perlu Update**
   - Hanya update jika ada perubahan sistem atau penambahan metric baru
   - Jika mapping salah, berarti dari awal sudah salah

4. **Setelah Update Seeds, Re-run Models yang Depend**
   - Jika update `seed_daily_kpi_monthly`, re-run `mart_site_kpi_monthly`
   - Jika update `seed_energy_adjustment_daily`, re-run `mart_site_performance_daily`
   - Jika update `seed_ghi_adjustment_daily`, re-run `mart_site_performance_daily`

## 📋 Checklist Update Seeds

### Monthly Update (Bulanan)

- [ ] Update `seed_daily_simulation_target` (jika ada rekalkulasi)
- [ ] Update `seed_daily_kpi_monthly` (jika ada nilai baru atau rekalkulasi)
- [ ] Re-run models yang depend:
  ```bash
  dbt run --select mart_site_kpi_monthly+ mart_site_performance_daily+
  ```

### On-Demand Update (Saat Ada Perubahan)

- [ ] Update `seed_energy_adjustment_daily` (saat ada rekalkulasi)
- [ ] Update `seed_ghi_adjustment_daily` (saat ada rekalkulasi)
- [ ] Update config seeds (saat ada perubahan config)
- [ ] Re-run models yang depend:
  ```bash
  dbt run --select mart_site_performance_daily+
  ```

## 🔍 Dependencies Seeds

### seed_daily_kpi_monthly
- **Used by:** `mart_site_kpi_monthly`
- **Impact:** Jika update, perlu re-run `mart_site_kpi_monthly`

### seed_daily_simulation_target
- **Used by:** `mart_simulation_targets_daily`, `mart_site_performance_daily`
- **Impact:** Jika update, perlu re-run `mart_simulation_targets_daily+` dan `mart_site_performance_daily+`

### seed_energy_adjustment_daily
- **Used by:** `mart_site_performance_daily`
- **Impact:** Jika update, perlu re-run `mart_site_performance_daily+`

### seed_ghi_adjustment_daily
- **Used by:** `mart_site_performance_daily`
- **Impact:** Jika update, perlu re-run `mart_site_performance_daily+`

### seed_metric_mapper
- **Used by:** Semua mart 5min models
- **Impact:** Jika update, perlu re-run semua mart 5min models (jarang update)

## 🛠️ Scripts

Scripts untuk memudahkan update seeds:

- **Windows:** `scripts/update_dynamic_seeds.ps1`
- **Linux/Mac:** `scripts/update_dynamic_seeds.sh`

**Usage:**
```powershell
# Update semua dynamic seeds
.\scripts\update_dynamic_seeds.ps1

# Update seed tertentu
.\scripts\update_dynamic_seeds.ps1 seed_daily_kpi_monthly
```

## 🔄 Auto-Update Seeds saat dbt run

**Masalah**: `dbt run` dan `dbt seed` terpisah; seeds tidak otomatis di-update saat `dbt run`.

**Solusi — Opsi 1 (disarankan): Script**

```powershell
# Windows
cd dbt
.\scripts\run_dbt_with_seeds.ps1
.\scripts\run_dbt_with_seeds.ps1 run --select staging+
```

```bash
# Linux/Mac
./scripts/run_dbt_with_seeds.sh
./scripts/run_dbt_with_seeds.sh run --select staging+
```

**Opsi 2: Manual sequence**

```bash
dbt seed --select seed_daily_simulation_target seed_daily_kpi_monthly seed_energy_adjustment_daily seed_ghi_adjustment_daily
dbt run
```

**Workflow harian**: Jalankan script run_dbt_with_seeds dulu (update dynamic seeds), lalu dbt run. **Rekalkulasi**: Update seed_energy_adjustment_daily + seed_ghi_adjustment_daily, lalu `dbt run --select mart_site_performance_daily+` (dengan vars jika perlu).

---

## 📚 Related Documentation

- [DEPENDENCY_GRAPH_EXPLANATION.md](./DEPENDENCY_GRAPH_EXPLANATION.md)
- [DIM_ASSETS.md](./DIM_ASSETS.md)

