# Shoetown POA Validation Steps

**Date**: 2025-01-XX  
**Status**: Ready for Validation

---

## ✅ Completed Steps

1. [x] ✅ Identified all sensors in raw database
2. [x] ✅ Found Meteo Station16 (replacement for SLI-IRR-3-F)
3. [x] ✅ Updated sensor config with correct capacities:
   - SLI-IRR-1-A: 452.4 kWp
   - SLI-IRR-2-A: 452.4 kWp
   - SLI-IRR-3-F: 928 kWp
   - SLI-IRR-4-F: 763.28 kWp
   - Meteo Station16: 928 kWp
4. [x] ✅ Created validation queries

---

## 📋 Next Steps

### Step 1: Reload Seed (if dbt error is fixed)

```bash
cd "C:\Users\Administrator\Documents\Code\MMSR API - Server MA\dbt"
dbt seed --select seed_sensor_config
```

**Note**: Jika ada error duplikasi schema.yml, fix dulu atau skip step ini jika seed sudah ter-load sebelumnya.

### Step 2: Re-run Models

```bash
cd "C:\Users\Administrator\Documents\Code\MMSR API - Server MA\dbt"
dbt run --select mart_sensor_daily mart_site_performance_daily
```

**Note**: Ini akan re-calculate POA weighted average dengan capacity yang baru.

### Step 3: Run Validation Query

Jalankan query untuk export data:
```sql
-- File: queries/validate_shoetown_poa_final.sql
```

Query ini akan menghasilkan:
- Daily POA per sensor (SLI_IRR_1_A, SLI_IRR_2_A, SLI_IRR_3_F, SLI_IRR_4_F)
- Weighted average POA
- Active sensors per day
- Debug columns untuk troubleshooting

### Step 4: Compare with Excel

1. **Export query results** ke CSV atau copy ke Excel
2. **Open Excel file**: `shoetown_poa_daily.xlsx`
3. **Compare columns**:
   - `SLI_IRR_1_A` - per sensor
   - `SLI_IRR_2_A` - per sensor
   - `SLI_IRR_3_F` - per sensor (menggunakan Meteo Station16 setelah Oct 3)
   - `SLI_IRR_4_F` - per sensor
   - `Weighted_Avg_POA` - weighted average (main comparison)

### Step 5: Analyze Differences

Jika ada perbedaan:

1. **Check active sensors**: Kolom `Active_Sensors` menunjukkan sensor mana yang digunakan per hari
2. **Check capacity**: Pastikan capacity di database match dengan Excel
3. **Check date ranges**: Pastikan sensor replacement dates benar
4. **Check calculation**: Bandingkan `Sum_POA_x_Capacity` dan `Sum_Capacity` dengan Excel

---

## 🔍 Expected Results

### Before Oct 3, 2025:
- Active sensors: SLI-IRR-1-Aold, SLI-IRR-2-Aold, SLI-IRR-3-F, SLI-IRR-4-F
- Total capacity: 452.4 + 452.4 + 928 + 763.28 = 2596.08 kWp

### Oct 3 - Nov 11, 2025:
- Active sensors: SLI-IRR-1-A, SLI-IRR-2-A, SLI-IRR-4-F
- Total capacity: 452.4 + 452.4 + 763.28 = 1668.08 kWp
- **Note**: SLI-IRR-3-F sudah diganti tapi Meteo Station16 belum aktif (aktif Nov 12)

### After Nov 12, 2025:
- Active sensors: SLI-IRR-1-A, SLI-IRR-2-A, Meteo Station16, SLI-IRR-4-F
- Total capacity: 452.4 + 452.4 + 928 + 763.28 = 2596.08 kWp

---

## 📊 Validation Checklist

- [ ] Seed reloaded (or verified current)
- [ ] Models re-run
- [ ] Query executed successfully
- [ ] Data exported to CSV/Excel
- [ ] Compared with Excel file
- [ ] Individual sensor values match
- [ ] Weighted average POA matches
- [ ] Active sensors per day correct
- [ ] Date ranges correct (Oct 1, Oct 3, Nov 12)

---

## 🐛 Troubleshooting

### If weighted average doesn't match:

1. **Check capacity values**: Run query to verify sensor capacities
2. **Check active sensors**: Verify which sensors are active per day
3. **Check date filtering**: Verify sensor replacement dates
4. **Check calculation**: Verify `Sum_POA_x_Capacity / Sum_Capacity`

### If individual sensor values don't match:

1. **Check device_id**: Verify device_id matches between database and Excel
2. **Check date ranges**: Verify sensor activation/deactivation dates
3. **Check data availability**: Verify sensor has data for that date

---

## 📁 Related Files

- Validation Query: `queries/validate_shoetown_poa_final.sql`
- Export Query: `queries/export_shoetown_poa_daily_for_excel_validation.sql`
- Sensor Config: `dbt/seeds/seed_sensor_config.csv`
- Documentation: `reports/SHOETOWN_SENSOR_REPOSITIONING_SUMMARY.md`

---

**Last Updated**: 2025-01-XX  
**Status**: Ready for validation

