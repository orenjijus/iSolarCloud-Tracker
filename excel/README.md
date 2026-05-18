# Excel Performance Monitoring - Data Flow Plan

## Overview

Struktur data 5 menit untuk performance monitoring yang mudah di-load ke Excel via ODBC.

## Scope

- **Meter**: Data 5 menit (positive_active_energy, negative_active_energy, active_power)
- **Sensor**: Data 5 menit (GHI, POA, temperature)
- **Inverter**: Data 5 menit (active_power per inverter - semua nilai, bukan agregasi)
- **String**: Tidak termasuk (untuk file terpisah - string analysis)

## Format Data

- **Grain**: `timestamp × site` (satu row per timestamp per site)
- **Format**: Wide (pivoted) untuk Excel
- **Period**: Per tahun per site (misal: `mv_performance_monitoring_5min_mmki1_2025`)
- **Refresh**: Incremental (CONCURRENTLY) setelah daily ingestion

## File Structure

```
excel/
├── README.md                          # Dokumentasi ini
├── README_GENERATOR.md                # Dokumentasi Python generator
├── generate_mv_performance_monitoring.py  # Python script untuk generate MV (RECOMMENDED)
├── 01_create_materialized_view.sql   # Template: Script create MV per site per tahun
├── 02_refresh_functions.sql           # Function untuk refresh incremental
├── 03_setup_auto_refresh.sql          # Setup auto-refresh schedule
├── 04_generate_all_sites.sql          # SQL script generator (alternative)
├── 05_test_queries.sql                # Test queries untuk validasi
├── 06_excel_connection_guide.md       # Panduan setup Excel ODBC
├── 07_quick_start.sql                 # Quick start guide
└── generated_sql/                      # Output directory untuk generated SQL files
```

## Data Structure

### Columns dalam Materialized View

**Meter Data:**
- `meter_positive_energy_kwh` - Positive active energy (kWh)
- `meter_negative_energy_kwh` - Negative active energy (kWh)
- `meter_active_power_kw` - Active power (kW)
- `meter_name` - Meter device name
- `meter_id` - Meter asset ID

**Sensor Data:**
- `sensor_ghi_w_m2` - GHI irradiance (W/m²)
- `sensor_poa_w_m2` - POA irradiance (W/m²)
- `sensor_temperature_c` - Temperature (°C)
- `sensor_name` - Sensor device name
- `sensor_id` - Sensor asset ID

**Inverter Data (5 menit raw, per inverter):**
- `inverter_1_active_power_kw` sampai `inverter_20_active_power_kw` - Active power per inverter (kW)
- `total_inverter_count` - Jumlah inverter yang ada data
- `inverter_names` - Nama semua inverter (comma-separated)

**Note:** Support sampai 20 inverter per site. Jika lebih dari 20, hanya 20 pertama yang muncul.

## Data Flow

```
Daily Ingestion (00:00)
  ↓
Data masuk ke mart_*_5min tables
  ↓
Auto-refresh MV (02:00) - CONCURRENTLY
  ↓
Excel query full year dari MV (< 1 detik)
  ↓
Data Excel updated
```

## Performance Target

- **Data per tahun per site**: ~105K rows (365 hari × 288 intervals)
- **Materialized view size**: ~50-70 MB per site per tahun
- **Refresh time**: 5-30 detik (incremental)
- **Query time (full year)**: < 1 detik
- **Query time (1 bulan)**: < 100ms

## Quick Start

### Option 1: Python Generator (Recommended)

```bash
# Generate SQL files untuk semua site, tahun 2025
python excel/generate_mv_performance_monitoring.py --years 2025

# Generate dan execute langsung
python excel/generate_mv_performance_monitoring.py --years 2025 --execute

# Untuk site tertentu
python excel/generate_mv_performance_monitoring.py --site "MMKI 1" --years 2025 --execute
```

Lihat `README_GENERATOR.md` untuk detail lengkap.

### Option 2: Manual SQL

1. Review `01_create_materialized_view.sql`
2. Replace placeholders (`{SITE_NAME}`, `{YEAR}`)
3. Execute SQL manually

## Next Steps

1. **Generate Materialized Views**: 
   - Gunakan Python generator (recommended) atau manual SQL
2. **Setup Refresh Functions**: 
   - Run `02_refresh_functions.sql`
3. **Setup Auto-Refresh**: 
   - Configure di `03_setup_auto_refresh.sql` atau add ke daily pipeline
4. **Test Queries**: 
   - Run queries di `05_test_queries.sql` untuk validasi
5. **Connect Excel**: 
   - Follow guide di `06_excel_connection_guide.md`
6. **Monitor Performance**: 
   - Check MV status dan query performance
