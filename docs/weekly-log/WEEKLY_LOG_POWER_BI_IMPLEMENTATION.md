# Weekly Log Power BI Implementation Guide

## Overview

Panduan lengkap untuk mengimplementasikan Weekly Log di Power BI, mulai dari database hingga visualisasi di dashboard.

## Struktur Data

### Input (Seed)

Yang Anda input di seed: **tanggal saja** (`log_date`), **site**, **problem_identification**, **corrective_action**, **status**. Week tidak di-input; week digenerate di mart (algoritma sama seperti `mart_site_performance_daily`).  
Detail input dan **cara input status** (Open / Resolved): lihat [WEEKLY_LOG_INPUT_AND_STATUS.md](WEEKLY_LOG_INPUT_AND_STATUS.md).

### Tabel: `mart.mart_weekly_log`

Tabel ini berisi data weekly log. Kolom week/month/year **digenerate di mart** dari `log_date` (join ke `dim_date_generated`):

| Kolom | Tipe | Deskripsi |
|-------|------|-----------|
| `log_date` | DATE | Tanggal log (input) |
| `site_id` | VARCHAR | ID site (untuk relationship) |
| `site_name` | VARCHAR | Nama site (untuk display) |
| `problem_identification` | TEXT | Deskripsi masalah (bisa multi-line) |
| `corrective_action` | TEXT | Tindakan korektif yang dilakukan |
| `status` | VARCHAR | Status: Open, In Progress, Resolved, Closed (input manual) |
| `days_open` | INTEGER | **Berapa hari sejak issue di-log** (untuk prioritas: issue lama = prioritas tinggi) |
| `is_open` | INTEGER | 1 = masih Open/In Progress, 0 = Resolved/Closed (untuk filter prioritas) |
| `year` | INTEGER | Tahun dari log_date (dari dim_date_generated) |
| `month` | INTEGER | Bulan dari log_date |
| `month_name` | VARCHAR | Nama bulan |
| `week_of_month` | INTEGER | Minggu ke berapa dalam bulan (algoritma sama dengan mart lain) |
| `day_of_week` | INTEGER | Hari dalam minggu |
| `day_name` | VARCHAR | Nama hari |
| `day_type` | VARCHAR | Weekday / Weekend |
| `created_at` | TIMESTAMPTZ | Waktu record dibuat |
| `updated_at` | TIMESTAMPTZ | Waktu record terakhir di-update |

---

## Step 1: Setup Database

### 1.1 Load Seed Data

Jalankan dbt seed untuk memuat data weekly log ke database:

```bash
cd dbt
dbt seed --select seed_weekly_log
```

### 1.2 Build Mart Model

Jalankan dbt model untuk membuat tabel mart:

```bash
dbt run --select mart_weekly_log
```

### 1.3 Verify Data

Verifikasi data sudah ter-load dengan benar:

```sql
SELECT 
    log_date,
    site_name,
    days_open,
    is_open,
    problem_identification,
    corrective_action,
    status
FROM mart.mart_weekly_log
WHERE is_open = 1
ORDER BY days_open DESC, site_name
LIMIT 10;
```
(Contoh: issue yang masih open, diurutkan prioritas — yang paling lama di atas.)

---

## Step 2: Power BI Connection

### 2.1 Connect to Database

1. Buka Power BI Desktop
2. Klik **Get Data** → **PostgreSQL database**
3. Masukkan connection details:
   - **Server**: [your_server]
   - **Database**: [your_database]
   - **Data Connectivity mode**: Import (recommended) atau DirectQuery

### 2.2 Import Tables

Import tabel berikut:
- ✅ `mart.mart_weekly_log` (weekly log data)
- ✅ `dimensions.dim_assets` (untuk relationship dengan site)
- ✅ `dimensions.dim_date_generated` (untuk filtering berdasarkan tanggal)

---

## Step 3: Data Model Setup

### 3.1 Create Relationships

Buat relationship antara tabel:

```
┌─────────────────────────┐
│ dim_assets              │
│ PK: asset_id           │
└──────────┬──────────────┘
           │
           │ 1:many
           │
┌──────────▼──────────────┐
│ mart_weekly_log         │
│ FK: site_id            │
└─────────────────────────┘

┌─────────────────────────┐
│ dim_date_generated      │
│ PK: date_key           │
└──────────┬──────────────┘
           │
           │ 1:many
           │
┌──────────▼──────────────┐
│ mart_weekly_log         │
│ FK: log_date (= date_key)
└─────────────────────────┘
```

**Relationship Configuration:**
- `dim_assets[asset_id]` → `mart_weekly_log[site_id]` (Many-to-One, Active)
- `dim_date_generated[date_key]` → `mart_weekly_log[log_date]` (Many-to-One, Active)

### 3.2 Hide Unnecessary Columns

Hide kolom yang tidak perlu di Fields pane:
- `created_at`
- `updated_at`
- `site_id` (keep untuk relationship, tapi hide dari visual)

---

## Step 4: Create DAX Measures (Optional)

### 4.1 Count Measures

```dax
Total Weekly Logs = 
COUNTROWS('mart_weekly_log')

Open Issues = 
CALCULATE(
    COUNTROWS('mart_weekly_log'),
    'mart_weekly_log'[status] = "Open"
)

Resolved Issues = 
CALCULATE(
    COUNTROWS('mart_weekly_log'),
    'mart_weekly_log'[status] = "Resolved"
)

Issues by Status = 
SWITCH(
    SELECTEDVALUE('mart_weekly_log'[status]),
    "Open", [Open Issues],
    "Resolved", [Resolved Issues],
    "In Progress", CALCULATE(COUNTROWS('mart_weekly_log'), 'mart_weekly_log'[status] = "In Progress"),
    [Total Weekly Logs]
)
```

### 4.2 Days Open & Prioritas (untuk decision)

Kolom **`days_open`** dan **`is_open`** sudah ada di mart — pakai langsung untuk prioritas. Opsional, buat measure untuk conditional formatting atau tooltip:

```dax
Days Open = 
DATEDIFF('mart_weekly_log'[log_date], TODAY(), DAY)
```

**Cara pakai untuk set prioritas:**
1. **Filter**: Hanya tampilkan issue yang masih open → Slicer/Filter **`is_open` = 1** (atau `status` IN ("Open", "In Progress")).
2. **Sort**: Urutkan by **`days_open`** (Descending) — issue yang sudah berjalan paling lama di atas.
3. **Conditional formatting**: Warna `days_open` (mis. merah jika > 7 hari, kuning jika > 3 hari) untuk prioritas visual.

### 4.3 Week Filter Measures

```dax
Current Week Logs = 
CALCULATE(
    COUNTROWS('mart_weekly_log'),
    'mart_weekly_log'[log_date] >= TODAY() - 7
)

This Month Logs = 
CALCULATE(
    COUNTROWS('mart_weekly_log'),
    'mart_weekly_log'[week_month] = MONTH(TODAY()),
    'mart_weekly_log'[week_year] = YEAR(TODAY())
)
```

---

## Step 5: Create Visualizations

### 5.1 Weekly Log Table

**Visual Type**: Table

**Fields**:
- `site_name` (Row)
- `log_date` (Row) - Format: "MMM DD, YYYY"
- `problem_identification` (Row) - Wrap text enabled
- `corrective_action` (Row) - Wrap text enabled
- `status` (Row) - Conditional formatting

**Conditional Formatting untuk Status**:
- Open: Red background
- In Progress: Yellow background
- Resolved: Green background
- Closed: Gray background

**Sorting**: 
- Sort by `log_date` (Descending)
- Then by `site_name` (Ascending)

**Prioritas decision (issue sudah berjalan berapa lama):**
- Tambahkan kolom **`days_open`** ke tabel — menampilkan berapa hari sejak issue di-log.
- Untuk **hanya issue yang masih open**: Filter **`is_open` = 1** (atau filter `status` = Open, In Progress).
- **Urutkan prioritas**: Sort by **`days_open`** (Descending) — issue yang paling lama berjalan di atas (prioritas tinggi).

### 5.2 Status Summary Card

**Visual Type**: Card

**Fields**: 
- `[Open Issues]` (Value)
- Label: "Open Issues"

**Format**: 
- Font size: 24pt
- Color: Red

### 5.3 Issues by Site Matrix

**Visual Type**: Matrix

**Fields**:
- `site_name` (Rows)
- `status` (Columns)
- `[Total Weekly Logs]` (Values)

**Format**:
- Show on rows: ON
- Subtotals: ON

### 5.4 Weekly Timeline

**Visual Type**: Table atau Matrix

**Fields**:
- `log_date` (Rows) - Group by Week; atau pakai `year`, `month`, `week_of_month`
- `site_name` (Rows)
- `problem_identification` (Values) - Show as Text
- `status` (Values) - Show as Text with conditional formatting

### 5.5 Filter Panel

**Visual Type**: Slicers

**Slicer 1: Week Range**
- Field: `log_date`
- Type: Between
- Format: Date

**Slicer 2: Site**
- Field: `site_name`
- Type: Dropdown
- Select all: Enabled

**Slicer 3: Status**
- Field: `status`
- Type: Dropdown
- Select all: Enabled

---

## Step 6: Advanced Features

### 6.1 Drill-Through Page

Buat halaman detail untuk drill-through:

**Source Page**: Weekly Log Overview
**Target Page**: Weekly Log Detail

**Drill-Through Fields**:
- `site_id` → Filter target page by site
- `log_date` → Filter target page by week

**Detail Page Visuals**:
- Card: Selected Site Name
- Card: Selected Week Range
- Table: Full problem and corrective action details
- Text Box: Additional notes or history

### 6.2 Tooltip Page

Buat tooltip page untuk hover details:

**Tooltip Fields**:
- `site_name`
- `log_date`
- `problem_identification` (full text)
- `corrective_action` (full text)
- `status`

**Usage**: 
- Set tooltip pada table visual
- Hover over row untuk melihat detail lengkap

### 6.3 Export to Excel Button

Tambahkan button untuk export data:

**DAX Measure untuk Export**:
```dax
Export Weekly Log = 
"Export functionality can be added using Power BI Export feature or Power Automate"
```

**Alternative**: Gunakan Power BI Export to Excel feature (built-in)

---

## Step 7: Refresh Strategy

### 7.1 Manual Refresh

1. Update CSV file: `dbt/seeds/seed_weekly_log.csv`
2. Run dbt seed: `dbt seed --select seed_weekly_log`
3. Run dbt model: `dbt run --select mart_weekly_log`
4. Refresh Power BI: Click **Refresh** button

### 7.2 Automated Refresh (Recommended)

**Option 1: Power BI Scheduled Refresh**
- Set up scheduled refresh di Power BI Service
- Refresh frequency: Daily atau Weekly

**Option 2: Power Automate Flow**
- Trigger: Schedule (daily/weekly)
- Action 1: Run dbt command (via API atau script)
- Action 2: Refresh Power BI dataset

**Option 3: Airflow DAG**
- Add weekly log update task to existing pipeline
- Run after daily pipeline completes

---

## Step 8: Best Practices

### 8.1 Data Entry Guidelines

1. **Log Date**: Input tanggal kejadian saja (YYYY-MM-DD). Week digenerate di mart dari log_date.
2. **Problem Identification**: 
   - Gunakan numbering untuk multiple problems (1., 2., 3.)
   - Gunakan line breaks untuk readability
3. **Status**: 
   - Open: Masalah belum ditangani
   - In Progress: Sedang ditangani
   - Resolved: Sudah ditangani
   - Closed: Selesai dan tidak perlu follow-up

### 8.2 Performance Optimization

1. **Import Mode**: Gunakan Import mode untuk better performance
2. **Incremental Load**: Jika data besar, pertimbangkan incremental refresh
3. **Index**: Pastikan index sudah dibuat di database (sudah ada di SQL script)

### 8.3 Security

1. **Row-Level Security**: Jika perlu, set up RLS untuk membatasi akses berdasarkan site
2. **Data Privacy**: Mark sensitive columns as "Private" jika perlu

---

## Step 9: Troubleshooting

### Issue: Data tidak muncul di Power BI

**Solution**:
1. Check connection string
2. Verify table exists: `SELECT * FROM marts.mart_weekly_log LIMIT 1;`
3. Check relationships are active
4. Verify filters tidak terlalu restrictive

### Issue: Multi-line text tidak ter-display dengan baik

**Solution**:
1. Enable "Word wrap" pada column di table visual
2. Adjust column width
3. Consider using tooltip untuk full text

### Issue: Week start date tidak sesuai

**Solution**:
1. Verify log_date format: YYYY-MM-DD
2. Check relationship: dim_date_generated[date_key] → mart_weekly_log[log_date]
3. Week/month/year sudah di mart: pakai kolom `year`, `month`, `week_of_month`

---

## Step 10: Example Queries

### SQL: Get Current Week Logs

```sql
SELECT 
    site_name,
    problem_identification,
    corrective_action,
    status
FROM marts.mart_weekly_log
WHERE log_date >= DATE_TRUNC('week', CURRENT_DATE)
ORDER BY site_name;
```

### SQL: Get Open Issues by Site

```sql
SELECT 
    site_name,
    COUNT(*) as open_issues_count,
    STRING_AGG(problem_identification, ' | ') as all_problems
FROM marts.mart_weekly_log
WHERE status = 'Open'
GROUP BY site_name
ORDER BY open_issues_count DESC;
```

### DAX: Filter by Current Month

```dax
Current Month Weekly Logs = 
CALCULATE(
    COUNTROWS('mart_weekly_log'),
    'mart_weekly_log'[week_month] = MONTH(TODAY()),
    'mart_weekly_log'[week_year] = YEAR(TODAY())
)
```

---

## Summary Checklist

- [ ] Database table created (`staging.seed_weekly_log`)
- [ ] Seed CSV file created (`dbt/seeds/seed_weekly_log.csv`)
- [ ] dbt model created (`dbt/models/marts/mart_weekly_log.sql`)
- [ ] Data loaded: `dbt seed --select seed_weekly_log`
- [ ] Model built: `dbt run --select mart_weekly_log`
- [ ] Power BI connected to database
- [ ] Tables imported: `mart_weekly_log`, `dim_assets`, `dim_date_generated`
- [ ] Relationships created
- [ ] DAX measures created (optional)
- [ ] Table visual created
- [ ] Slicers added
- [ ] Conditional formatting applied
- [ ] Refresh strategy configured
- [ ] Documentation updated

---

**Last Updated**: 2025-12-14  
**Version**: 1.0  
**Status**: Ready for Implementation

