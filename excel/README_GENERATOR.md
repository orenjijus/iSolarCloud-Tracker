# Materialized View Generator - Performance Monitoring

## Overview

Script Python untuk generate materialized views per site per tahun untuk performance monitoring data (meter, sensor, inverter) dalam format wide untuk Excel.

## Perbedaan dengan Approach Lama

### Approach Lama (Inefficient)
- Generate views langsung dari raw tables (`isolarcloud_historical_data`, `fusionsolar_historical_data`)
- Parse JSONB `measurement_data` untuk setiap query
- Generate banyak views terpisah per device type (meter, sensor, inverter)
- Query lambat karena JSONB parsing setiap kali
- Maintenance sulit (banyak views)

### Approach Baru (Efficient)
- Menggunakan mart tables yang sudah normalized:
  - `mart.mart_meter_performance_5min`
  - `mart.mart_sensor_measurements_5min`
  - `mart.mart_inverter_performance_5min`
- Generate satu materialized view per site per tahun (gabung semua device types)
- Data sudah di-precompute, query sangat cepat
- Refresh incremental (CONCURRENTLY)
- Maintenance mudah (satu MV per site per tahun)

## Usage

### Generate SQL Files (Tidak Execute)

```bash
# Generate untuk semua site, tahun 2024-2026
python excel/generate_mv_performance_monitoring.py --years 2024 2025 2026

# Generate untuk tahun tertentu saja
python excel/generate_mv_performance_monitoring.py --years 2025

# Generate untuk site tertentu
python excel/generate_mv_performance_monitoring.py --site "MMKI 1" --years 2025
```

### Generate dan Execute Langsung

```bash
# Generate dan create MV langsung di database
python excel/generate_mv_performance_monitoring.py --years 2025 --execute

# Untuk site tertentu
python excel/generate_mv_performance_monitoring.py --site "MMKI 1" --years 2025 --execute
```

### Custom Output Directory

```bash
python excel/generate_mv_performance_monitoring.py --years 2025 --output-dir excel/custom_output
```

## Output

### SQL Files
Script akan generate SQL files di `excel/generated_sql/` (default) dengan format:
- `mv_performance_monitoring_5min_{site_name}_{year}.sql`

Contoh:
- `mv_performance_monitoring_5min_mmki_1_2025.sql`
- `mv_performance_monitoring_5min_charoen_pokphand_madiun_2025.sql`

### Materialized Views
Jika `--execute` digunakan, akan create:
- Materialized view: `mart.mv_performance_monitoring_5min_{site_name}_{year}`
- View wrapper: `mart.vw_performance_monitoring_5min_{site_name}_{year}`
- Indexes: `idx_{mv_name}_unique_timestamp`, `idx_{mv_name}_date`, dll

## Features

1. **Auto-detect Inverters**: Query dari `dim_assets` untuk get semua inverter per site
2. **Dynamic Pivot**: Generate kolom inverter secara dinamis (support sampai 20 inverter)
3. **Wide Format**: Semua data dalam format wide (pivoted) untuk Excel
4. **Indexed**: Auto-create indexes untuk query cepat
5. **Permissions**: Auto-grant SELECT untuk public access

## Data Structure

### Meter Data
- `meter_positive_energy_kwh`
- `meter_negative_energy_kwh`
- `meter_active_power_kw`
- `meter_name`
- `meter_id`

### Sensor Data
- `sensor_ghi_w_m2`
- `sensor_poa_w_m2`
- `sensor_temperature_c`
- `sensor_name`
- `sensor_id`

### Inverter Data
- `inverter_1_active_power_kw` sampai `inverter_20_active_power_kw`
- `total_inverter_count`
- `inverter_names`

## Refresh Materialized Views

Setelah create MV, refresh dengan:

```sql
-- Refresh single site
SELECT * FROM mart.refresh_mv_performance_5min_site_year('MMKI 1', 2025);

-- Refresh all sites for year
SELECT * FROM mart.refresh_all_mv_performance_5min_year(2025);
```

## Workflow

1. **Generate SQL Files**:
   ```bash
   python excel/generate_mv_performance_monitoring.py --years 2025
   ```

2. **Review Generated SQL** (optional):
   - Check files di `excel/generated_sql/`
   - Verify SQL looks correct

3. **Execute SQL** (manual atau via script):
   ```bash
   # Option 1: Execute via script
   python excel/generate_mv_performance_monitoring.py --years 2025 --execute
   
   # Option 2: Execute SQL files manually
   psql -d MMSR -f excel/generated_sql/mv_performance_monitoring_5min_mmki_1_2025.sql
   ```

4. **Setup Auto-Refresh**:
   - Add refresh call di daily ingestion pipeline
   - Atau setup scheduled job (lihat `03_setup_auto_refresh.sql`)

5. **Connect Excel**:
   - Setup ODBC connection (lihat `06_excel_connection_guide.md`)
   - Query dari `mart.vw_performance_monitoring_5min_{site_name}_{year}`

## Performance

- **Generate Time**: ~1-5 detik per site per year
- **MV Size**: ~50-70 MB per site per year (105K rows)
- **Query Time**: < 1 detik untuk full year
- **Refresh Time**: 5-30 detik (incremental)

## Troubleshooting

### Error: Too Many Inverters
Jika site punya lebih dari 20 inverter, hanya 20 pertama yang akan muncul. Check `total_inverter_count` untuk validasi.

### Error: Site Not Found
Pastikan site name exact match dengan yang ada di `dim_assets`. Check dengan:
```sql
SELECT DISTINCT site_name FROM dimensions.dim_assets WHERE asset_level = 'Site';
```

### Error: Permission Denied
Pastikan user punya permission untuk create materialized views di schema `mart`.

## Next Steps

1. Run generator untuk semua site dan tahun yang diperlukan
2. Review generated SQL files
3. Execute SQL untuk create MVs
4. Setup auto-refresh schedule
5. Test query dari Excel
6. Monitor performance

