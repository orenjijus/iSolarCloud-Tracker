# PowerBI Implementation Guide: Actual vs Adjusted Performance

## Overview

Guide ini menjelaskan cara implementasi toggle **Actual vs Adjusted** performance di PowerBI menggunakan slicer. Dengan satu slicer, semua metrics akan otomatis exclude issue dates ketika dipilih "Adjusted".

---

## Step-by-Step Implementation

### Step 1: Buat Parameter Table

1. Buka PowerBI Desktop
2. Klik tab **Modeling** > **New Table**
3. Copy paste code berikut:

```DAX
Parameter_ViewType = 
DATATABLE(
    "ViewType", STRING,
    "ViewTypeValue", INTEGER,
    {
        {"Actual", 0},
        {"Adjusted", 1}
    }
)
```

4. Klik **Check** (✓) untuk validate
5. Table `Parameter_ViewType` akan muncul di Fields pane

---

### Step 2: Buat Helper Measures

#### Helper Measure 1: Check Exclusion Flag

1. Klik tab **Modeling** > **New Measure**
2. Name: `_ShouldExcludeIssueDates`
3. Copy paste code:

```DAX
_ShouldExcludeIssueDates = 
VAR SelectedView = SELECTEDVALUE(Parameter_ViewType[ViewTypeValue], 0)
RETURN
    IF(SelectedView = 1, TRUE(), FALSE())
```

4. Klik **Check** (✓)

**Note**: Measure ini menggunakan prefix `_` (underscore) agar tidak muncul di Fields pane (hidden measure).

---

### Step 3: Buat Measures untuk Semua Metrics

Buat measures berikut satu per satu. Semua measures menggunakan pattern yang sama:

#### 3.1 Energy Measures

**Energy MWh (Daily)**
```DAX
Energy MWh = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('mart_site_performance_daily'[daily_energy_mwh]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        SUM('mart_site_performance_daily'[daily_energy_mwh])
    )
```

**Energy Monthly MWh**
```DAX
Energy Monthly MWh = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR CurrentMonth = SELECTEDVALUE('mart_site_performance_daily'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('mart_site_performance_daily'[daily_energy_mwh]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('mart_site_performance_daily'[daily_energy_mwh]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        )
    )
```

**Energy YTD MWh**
```DAX
Energy YTD MWh = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR MaxDate = MAX('mart_site_performance_daily'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('mart_site_performance_daily'[daily_energy_mwh]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('mart_site_performance_daily'[daily_energy_mwh]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        )
    )
```

#### 3.2 GHI Measures

**GHI kWh/m² (Daily Average)**
```DAX
GHI kWh/m² = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_ghi_kwh_m2]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[daily_ghi_kwh_m2])
    )
```

**GHI Monthly Avg kWh/m²**
```DAX
GHI Monthly Avg kWh/m² = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR CurrentMonth = SELECTEDVALUE('mart_site_performance_daily'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_ghi_kwh_m2]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_ghi_kwh_m2]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        )
    )
```

**GHI YTD Avg kWh/m²**
```DAX
GHI YTD Avg kWh/m² = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR MaxDate = MAX('mart_site_performance_daily'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_ghi_kwh_m2]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_ghi_kwh_m2]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        )
    )
```

#### 3.3 POA Measures

**POA kWh/m² (Daily Average)**
```DAX
POA kWh/m² = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_poa_weighted_kwh_m2]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[daily_poa_weighted_kwh_m2])
    )
```

**POA Monthly Avg kWh/m²**
```DAX
POA Monthly Avg kWh/m² = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR CurrentMonth = SELECTEDVALUE('mart_site_performance_daily'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_poa_weighted_kwh_m2]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_poa_weighted_kwh_m2]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        )
    )
```

**POA YTD Avg kWh/m²**
```DAX
POA YTD Avg kWh/m² = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR MaxDate = MAX('mart_site_performance_daily'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_poa_weighted_kwh_m2]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[daily_poa_weighted_kwh_m2]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        )
    )
```

#### 3.4 PR (Performance Ratio) Measures

**PR GHI**
```DAX
PR GHI = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_ghi_actual]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[pr_ghi_actual])
    )
```

**PR GHI Monthly Avg**
```DAX
PR GHI Monthly Avg = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR CurrentMonth = SELECTEDVALUE('mart_site_performance_daily'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_ghi_actual]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_ghi_actual]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        )
    )
```

**PR GHI YTD Avg**
```DAX
PR GHI YTD Avg = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR MaxDate = MAX('mart_site_performance_daily'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_ghi_actual]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_ghi_actual]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        )
    )
```

**PR POA**
```DAX
PR POA = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_poa_actual]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[pr_poa_actual])
    )
```

**PR POA Monthly Avg**
```DAX
PR POA Monthly Avg = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR CurrentMonth = SELECTEDVALUE('mart_site_performance_daily'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_poa_actual]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_poa_actual]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        )
    )
```

**PR POA YTD Avg**
```DAX
PR POA YTD Avg = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR MaxDate = MAX('mart_site_performance_daily'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_poa_actual]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[pr_poa_actual]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        )
    )
```

#### 3.5 Availability Measures

**Availability % (Daily Average)**
```DAX
Availability % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[availability_percent]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[availability_percent])
    )
```

**Availability Monthly Avg %**
```DAX
Availability Monthly Avg % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR CurrentMonth = SELECTEDVALUE('mart_site_performance_daily'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[availability_percent]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[availability_percent]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        )
    )
```

**Availability YTD Avg %**
```DAX
Availability YTD Avg % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR MaxDate = MAX('mart_site_performance_daily'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[availability_percent]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[availability_percent]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        )
    )
```

#### 3.6 Target Comparison Measures

**Energy vs Target %**
```DAX
Energy vs Target % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[energy_actual_vs_target_pct]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[energy_actual_vs_target_pct])
    )
```

**Energy vs Target Monthly %**
```DAX
Energy vs Target Monthly % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR CurrentMonth = SELECTEDVALUE('mart_site_performance_daily'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[energy_actual_vs_target_pct]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[energy_actual_vs_target_pct]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[month] = CurrentMonth
        )
    )
```

**Energy vs Target YTD %**
```DAX
Energy vs Target YTD % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('mart_site_performance_daily'[year])
VAR MaxDate = MAX('mart_site_performance_daily'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[energy_actual_vs_target_pct]),
            'mart_site_performance_daily'[is_issue_date] = FALSE(),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[energy_actual_vs_target_pct]),
            'mart_site_performance_daily'[year] = CurrentYear,
            'mart_site_performance_daily'[date_key] <= MaxDate
        )
    )
```

**GHI vs Target %**
```DAX
GHI vs Target % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[ghi_actual_vs_target_pct]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[ghi_actual_vs_target_pct])
    )
```

**POA vs Target %**
```DAX
POA vs Target % = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('mart_site_performance_daily'[poa_actual_vs_target_pct]),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        AVERAGE('mart_site_performance_daily'[poa_actual_vs_target_pct])
    )
```

#### 3.7 Count Measures

**Total Days**
```DAX
Total Days = 
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            COUNTROWS('mart_site_performance_daily'),
            'mart_site_performance_daily'[is_issue_date] = FALSE()
        ),
        COUNTROWS('mart_site_performance_daily')
    )
```

**Issue Days Count**
```DAX
Issue Days Count = 
COUNTROWS(
    FILTER(
        'mart_site_performance_daily',
        'mart_site_performance_daily'[is_issue_date] = TRUE()
    )
)
```

**Adjusted Days Count**
```DAX
Adjusted Days Count = 
COUNTROWS(
    FILTER(
        'mart_site_performance_daily',
        'mart_site_performance_daily'[is_issue_date] = FALSE()
    )
)
```

**Issue Days %**
```DAX
Issue Days % = 
DIVIDE(
    [Issue Days Count],
    COUNTROWS('mart_site_performance_daily'),
    0
) * 100
```

---

### Step 4: Buat Slicer

1. Klik tab **Insert** > **Slicer**
2. Di Fields pane, drag `Parameter_ViewType[ViewType]` ke slicer
3. Format slicer:
   - **Style**: Pilih style yang diinginkan (misalnya "Dropdown" atau "List")
   - **Selection**: Single select
   - **Title**: "View Type" atau "Performance Type"
4. Slicer akan menampilkan:
   - ○ Actual
   - ○ Adjusted

---

### Step 5: Gunakan Measures di Visual

1. Buat visual (Table, Card, Chart, dll)
2. Drag measures yang sudah dibuat ke visual
3. Semua measures akan otomatis mengikuti pilihan slicer:
   - **Actual** = semua data termasuk issue dates
   - **Adjusted** = exclude issue dates

---

## Contoh Visual Layout

```
┌─────────────────────────────┐
│  View Type:                 │
│  ○ Actual                   │
│  ● Adjusted  ← Slicer       │
└─────────────────────────────┘

┌─────────────────────────────┐
│  Energy: 1250 MWh           │ ← Otomatis exclude issue dates
│  GHI: 5.2 kWh/m²            │ ← Otomatis exclude issue dates
│  PR GHI: 0.85               │ ← Otomatis exclude issue dates
│  Availability: 95.5%        │ ← Otomatis exclude issue dates
│  Energy vs Target: 102.3%   │ ← Otomatis exclude issue dates
└─────────────────────────────┘
```

---

## Tips & Best Practices

### 1. Organize Measures
- Group measures berdasarkan kategori (Energy, GHI, POA, PR, Availability)
- Gunakan folder di Fields pane untuk organization

### 2. Format Measures
- Set format number yang sesuai untuk setiap measure
- Energy: Decimal dengan 2 decimal places
- PR: Percentage dengan 2 decimal places
- Availability: Percentage dengan 1 decimal place

### 3. Default Selection
- Set default slicer ke "Actual" untuk consistency
- Atau set ke "Adjusted" jika itu yang lebih sering digunakan

### 4. Performance
- Measures menggunakan CALCULATE dengan filter, jadi performa tetap baik
- Jika dataset besar, pertimbangkan untuk membuat calculated table yang sudah filter

### 5. Custom Measures
- Untuk membuat measure custom baru, gunakan pattern yang sama:
  ```DAX
  Your Custom Measure = 
  VAR ExcludeIssues = [_ShouldExcludeIssueDates]
  RETURN
      IF(
          ExcludeIssues,
          CALCULATE(
              [Your Calculation],
              'mart_site_performance_daily'[is_issue_date] = FALSE()
          ),
          [Your Calculation]
      )
  ```

---

## Troubleshooting

### Issue: Measures tidak berubah ketika slicer diubah
**Solution**: 
- Pastikan slicer menggunakan field `Parameter_ViewType[ViewType]`
- Pastikan helper measure `_ShouldExcludeIssueDates` sudah dibuat
- Refresh visual atau reload data

### Issue: Measures return blank/null
**Solution**:
- Check apakah ada data untuk periode yang dipilih
- Check apakah filter lain tidak menghilangkan semua data
- Verify bahwa kolom `is_issue_date` ada di table

### Issue: Performance lambat
**Solution**:
- Pastikan ada index di kolom `is_issue_date` di database
- Pertimbangkan membuat calculated table yang sudah filter
- Gunakan DirectQuery jika dataset sangat besar

---

## File Reference

- **DAX Measures Lengkap**: `PowerBI_DAX_Measures_Actual_vs_Adjusted.txt`
- **Database Table**: `mart.mart_site_performance_daily`
- **Issue Dates Seed**: `dbt/seeds/seed_issue_dates.csv`

---

## Update Issue Dates

Ketika update issue dates:

1. Edit `dbt/seeds/seed_issue_dates.csv`
2. Jalankan: `dbt seed --select seed_issue_dates`
3. Jalankan: `dbt run --select mart_site_performance_daily`
4. Refresh PowerBI dataset
5. Measures akan otomatis menggunakan issue dates yang baru

---

**Last Updated**: 2025-01-XX

