# Excel ODBC Connection Guide

## Setup Excel Connection ke Materialized View

### Prerequisites

1. PostgreSQL ODBC Driver installed
2. Excel (2016 atau lebih baru)
3. Access ke database MMSR

---

## Step 1: Install PostgreSQL ODBC Driver

### Windows

1. Download PostgreSQL ODBC Driver dari: https://www.postgresql.org/ftp/odbc/versions/msi/
2. Install driver (psqlodbc_x64.msi untuk 64-bit)
3. Verify installation: Control Panel → Administrative Tools → ODBC Data Sources

---

## Step 2: Setup ODBC Data Source

### Windows ODBC Administrator

1. Buka **ODBC Data Source Administrator (64-bit)**
   - Windows Search: "ODBC Data Sources (64-bit)"
   - Atau: Control Panel → Administrative Tools → ODBC Data Sources (64-bit)

2. Pilih tab **"User DSN"** atau **"System DSN"**

3. Klik **"Add"**

4. Pilih **"PostgreSQL Unicode"** atau **"PostgreSQL ANSI"**

5. Configure connection:
   - **Data Source Name**: `MMSR_Performance_Monitoring` (atau nama lain)
   - **Database**: `MMSR`
   - **Server**: `10.101.4.88`
   - **Port**: `5432`
   - **User Name**: `juice` (atau user yang sesuai)
   - **Password**: (masukkan password)
   - **SSL Mode**: `prefer` atau `require` (sesuai konfigurasi server)

6. Klik **"Test"** untuk test connection

7. Klik **"Save"**

---

## Step 3: Connect Excel ke Database

### Method 1: From Existing Connections

1. Buka Excel
2. **Data** tab → **Get Data** → **From Other Sources** → **From Data Connection Wizard**
3. Pilih **"ODBC DSN"**
4. Pilih DSN yang sudah dibuat (`MMSR_Performance_Monitoring`)
5. Masukkan username dan password jika diminta
6. Pilih database: `MMSR`
7. Pilih schema: `mart`
8. Pilih table/view: `vw_performance_monitoring_5min_mmki1_2025` (atau MV yang diinginkan)
9. Klik **Finish**

### Method 2: Direct SQL Query

1. Buka Excel
2. **Data** tab → **Get Data** → **From Other Sources** → **From ODBC**
3. Pilih DSN: `MMSR_Performance_Monitoring`
4. Klik **"Advanced Options"**
5. Masukkan SQL query:
   ```sql
   SELECT * 
   FROM mart.vw_performance_monitoring_5min_mmki1_2025
   ORDER BY timestamp
   ```
6. Klik **OK**
7. Excel akan load data

---

## Step 4: Configure Refresh Settings

### Automatic Refresh

1. Klik kanan pada data table di Excel
2. Pilih **"Table Properties"** atau **"Data Range Properties"**
3. Configure:
   - **Refresh every**: `60 minutes` (atau sesuai kebutuhan)
   - **Refresh data when opening the file**: ✓ (check)
   - **Enable background refresh**: ✓ (check)

### Manual Refresh

- Klik kanan pada data table → **"Refresh"**
- Atau: **Data** tab → **Refresh All**

---

## Step 5: Optimize Excel Performance

### Tips untuk Data Besar

1. **Disable Auto-Format**
   - File → Options → Advanced
   - Uncheck "Extend data range formats and formulas"

2. **Use Excel Tables**
   - Convert data range to Excel Table (Ctrl+T)
   - Benefits: Better performance, easier refresh

3. **Limit Columns**
   - Jika tidak perlu semua kolom, query hanya kolom yang diperlukan:
   ```sql
   SELECT 
       timestamp,
       date_key,
       meter_positive_energy_kwh,
       sensor_ghi_w_m2,
       inverter_1_active_power_kw
   FROM mart.vw_performance_monitoring_5min_mmki1_2025
   ORDER BY timestamp
   ```

4. **Use Date Filters**
   - Query dengan date range untuk mengurangi data:
   ```sql
   SELECT * 
   FROM mart.vw_performance_monitoring_5min_mmki1_2025
   WHERE date_key >= '2025-12-01' 
     AND date_key <= '2025-12-31'
   ORDER BY timestamp
   ```

---

## Step 6: Create Multiple Connections (Multiple Sites)

Jika perlu data dari multiple sites:

1. **Data** tab → **Get Data** → **From Other Sources** → **From ODBC**
2. Buat connection untuk setiap site:
   - Connection 1: `vw_performance_monitoring_5min_mmki1_2025`
   - Connection 2: `vw_performance_monitoring_5min_mmki2_2025`
   - Connection 3: `vw_performance_monitoring_5min_mmki3_2025`
3. Load ke sheet terpisah atau combine dengan Power Query

---

## Troubleshooting

### Connection Failed

1. Check network connectivity ke database server
2. Verify username/password
3. Check firewall settings
4. Verify ODBC driver version compatibility

### Slow Query Performance

1. Check materialized view refresh status:
   ```sql
   SELECT * FROM mart.get_mv_performance_5min_status(2025);
   ```
2. Manually refresh MV if needed:
   ```sql
   SELECT * FROM mart.refresh_mv_performance_5min_site_year('MMKI 1', 2025);
   ```
3. Use date range filter in query
4. Limit columns in SELECT statement

### Data Not Updating

1. Check if MV was refreshed after ingestion:
   ```sql
   SELECT * FROM mart.get_mv_performance_5min_status(2025);
   ```
2. Manually refresh MV:
   ```sql
   SELECT * FROM mart.refresh_mv_performance_5min_site_year('MMKI 1', 2025);
   ```
3. Refresh Excel connection manually

### Excel Crashes with Large Data

1. Use date range filter (query per bulan atau per quarter)
2. Load data incrementally (append new data only)
3. Consider using Power BI instead for very large datasets

---

## Example Queries

### Full Year (Recommended)
```sql
SELECT * 
FROM mart.vw_performance_monitoring_5min_mmki1_2025
ORDER BY timestamp
```

### Last 30 Days
```sql
SELECT * 
FROM mart.vw_performance_monitoring_5min_mmki1_2025
WHERE date_key >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY timestamp
```

### Specific Date Range
```sql
SELECT * 
FROM mart.vw_performance_monitoring_5min_mmki1_2025
WHERE date_key >= '2025-12-01' 
  AND date_key <= '2025-12-31'
ORDER BY timestamp
```

### Selected Columns Only
```sql
SELECT 
    timestamp,
    date_key,
    meter_positive_energy_kwh,
    meter_active_power_kw,
    sensor_ghi_w_m2,
    inverter_1_active_power_kw,
    inverter_2_active_power_kw
FROM mart.vw_performance_monitoring_5min_mmki1_2025
WHERE date_key >= '2025-12-01'
ORDER BY timestamp
```

---

## Best Practices

1. **Refresh MV First**: Pastikan MV sudah di-refresh sebelum query dari Excel
2. **Use Date Filters**: Selalu gunakan date range filter jika memungkinkan
3. **Limit Columns**: Query hanya kolom yang diperlukan
4. **Schedule Refresh**: Setup auto-refresh untuk MV dan Excel connection
5. **Monitor Performance**: Check query execution time dan adjust jika perlu

---

## Support

Jika ada masalah:
1. Check materialized view status: `SELECT * FROM mart.get_mv_performance_5min_status(2025);`
2. Check refresh logs
3. Verify database connectivity
4. Check Excel connection settings

