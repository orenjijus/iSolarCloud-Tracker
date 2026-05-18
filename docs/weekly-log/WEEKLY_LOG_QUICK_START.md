# Weekly Log - Quick Start Guide

## 🚀 Quick Setup (5 Menit)

### Step 1: Update CSV File

Edit file `dbt/seeds/seed_weekly_log.csv`. **Input penting**: tanggal saja (`log_date`), site, problem, corrective action, status. Week **tidak** di-input; week digenerate di mart.

```csv
log_date,site_id,site_name,problem_identification,corrective_action,status,created_at,updated_at
2025-12-09,ISO_SITE_1637095,CP Majalengka,"1. Overcurrent protection active
2. Client's component make power quality problem
3. Other trip in 13 December 2025",Adjust ACB Settings,Open,2025-12-14 10:00:00,2025-12-14 10:00:00
2025-12-09,ISO_SITE_1637095,Charoen Pokphand,Cleaning Proposed not scheduled yet,,Open,2025-12-14 10:00:00,2025-12-14 10:00:00
```

**Catatan Penting**:
- `log_date`: Tanggal kejadian saja, format YYYY-MM-DD (boleh hari apa saja)
- `site_id`: Harus match dengan `dim_assets.asset_id`
- `problem_identification`: Bisa multi-line, gunakan quotes dan newline
- `status`: **Input manual** — Open (masalah baru), Resolved (sudah selesai). Lihat [WEEKLY_LOG_INPUT_AND_STATUS.md](WEEKLY_LOG_INPUT_AND_STATUS.md)

### Step 2: Load ke Database

```bash
cd dbt
dbt seed --select seed_weekly_log
dbt run --select mart_weekly_log
```

### Step 3: Connect di Power BI

1. **Get Data** → PostgreSQL → Connect ke database
2. Import tabel: `mart.mart_weekly_log`
3. Import tabel: `dimensions.dim_assets` (untuk relationship)
4. Import tabel: `dimensions.dim_date_generated` (untuk filtering)

### Step 4: Setup Relationships

Di Power BI Model view:
- `dim_assets[asset_id]` → `mart_weekly_log[site_id]` (Many-to-One)
- `dim_date_generated[date_key]` → `mart_weekly_log[log_date]` (Many-to-One)

### Step 5: Buat Table Visual

1. Drag `mart_weekly_log` ke canvas
2. Pilih kolom:
   - `site_name`
   - `log_date`
   - `year`, `month`, `week_of_month` (digenerate di mart)
   - `problem_identification`
   - `corrective_action`
   - `status`
3. Enable "Word wrap" untuk text columns
4. Add conditional formatting untuk `status`:
   - Open: Red
   - In Progress: Yellow
   - Resolved: Green
   - Closed: Gray

### Step 6: Add Slicers

Tambahkan slicers untuk filtering:
- **Tanggal**: `log_date` (Between)
- **Site**: `site_name` (Dropdown)
- **Status**: `status` (Dropdown)
- **Minggu/Bulan**: `year`, `month`, `week_of_month` (dari mart)

---

## 📝 Format CSV yang Benar

### Single Line Problem

```csv
log_date,site_id,site_name,problem_identification,corrective_action,status,created_at,updated_at
2025-12-09,ISO_SITE_1637095,CP Majalengka,MV ACB Trip,Adjust ACB Settings,Open,2025-12-14 10:00:00,2025-12-14 10:00:00
```

### Multi-Line Problem (dengan numbering)

```csv
log_date,site_id,site_name,problem_identification,corrective_action,status,created_at,updated_at
2025-12-09,ISO_SITE_1637095,CP Majalengka,"1. Overcurrent protection active
2. Client's component make power quality problem
3. Other trip in 13 December 2025",Adjust ACB Settings,Open,2025-12-14 10:00:00,2025-12-14 10:00:00
```

**Penting**: Multi-line text harus dalam quotes (`"..."`) dan gunakan newline di CSV.

---

## 🔄 Update Status (Open → Resolved)

1. Edit `dbt/seeds/seed_weekly_log.csv`
2. Ubah kolom `status` di baris yang bersangkutan: `Open` → `Resolved`
3. Run: `dbt seed --select seed_weekly_log`
4. Run: `dbt run --select mart_weekly_log`
5. Refresh Power BI

---

## 📚 Status (Input Manual)

- **Open**: Saat pertama kali input log (masalah belum selesai)
- **Resolved**: Saat masalah sudah ditangani — **kita yang update** (edit CSV lalu seed + run lagi)
- Opsional: **In Progress**, **Closed**

Detail: [WEEKLY_LOG_INPUT_AND_STATUS.md](WEEKLY_LOG_INPUT_AND_STATUS.md)

---

## ❓ Troubleshooting

### Data tidak muncul
- ✅ Check: `SELECT * FROM mart.mart_weekly_log LIMIT 1;`
- ✅ Verify relationships di Power BI
- ✅ Check filters tidak terlalu restrictive

### Multi-line text tidak ter-display
- ✅ Enable "Word wrap" di table visual
- ✅ Adjust column width
- ✅ Gunakan tooltip untuk full text

### Week / bulan tidak sesuai
- ✅ Week dan month digenerate di mart dari `log_date` (algoritma sama dengan mart_site_performance_daily)
- ✅ Pakai kolom `year`, `month`, `week_of_month` dari mart

---

**Last Updated**: 2025-12-14  
**Version**: 1.1 — Input: log_date (tanggal saja); week di mart; status input manual
