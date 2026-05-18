# Shoetown POA Validation - Ready Status

**Date**: 2025-01-XX  
**Status**: ✅ **READY FOR VALIDATION**

---

## ✅ Completed Actions

1. [x] ✅ **Sensor Config Updated**:
   - SLI-IRR-1-A: 452.4 kWp ✅
   - SLI-IRR-2-A: 452.4 kWp ✅
   - SLI-IRR-3-F: 928 kWp ✅
   - SLI-IRR-4-F: 763.28 kWp ✅
   - Meteo Station16: 928 kWp ✅

2. [x] ✅ **Database Updated**:
   - Capacity values updated directly via SQL (bypass dbt error)
   - All sensors verified with correct capacity

3. [x] ✅ **Validation Queries Created**:
   - `queries/validate_shoetown_poa_final.sql` - Main validation query
   - `queries/export_shoetown_poa_daily_for_excel_validation.sql` - Export format

---

## 📋 Next Steps

### Step 1: Re-run Models (Required)

POA calculation perlu di-recalculate dengan capacity yang baru:

```bash
cd "C:\Users\Administrator\Documents\Code\MMSR API - Server MA\dbt"
dbt run --select mart_sensor_daily mart_site_performance_daily
```

**Note**: Jika masih ada dbt error, bisa skip dulu dan langsung ke Step 2 untuk cek apakah data sudah ter-update.

### Step 2: Run Validation Query

Jalankan query untuk export data:
```sql
-- File: queries/validate_shoetown_poa_final.sql
```

Query ini menghasilkan:
- **Date**: Tanggal
- **SLI_IRR_1_A**: Daily POA sensor 1
- **SLI_IRR_2_A**: Daily POA sensor 2
- **SLI_IRR_3_F**: Daily POA sensor 3 (Meteo Station16 setelah Nov 12)
- **SLI_IRR_4_F**: Daily POA sensor 4
- **Weighted_Avg_POA**: Weighted average (main comparison)
- **Active_Sensors**: Sensor yang aktif per hari
- **Debug columns**: Sum_POA_x_Capacity, Sum_Capacity, etc.

### Step 3: Compare with Excel

1. **Export query results** ke CSV atau copy ke Excel
2. **Open Excel**: `shoetown_poa_daily.xlsx`
3. **Compare**:
   - Individual sensor values (SLI_IRR_1_A, SLI_IRR_2_A, SLI_IRR_3_F, SLI_IRR_4_F)
   - **Weighted_Avg_POA** (main comparison)

### Step 4: Analyze Results

**Expected Behavior**:

| Period | Active Sensors | Total Capacity |
|--------|---------------|----------------|
| **Before Oct 1** | SLI-IRR-1-Aold, SLI-IRR-2-Aold, SLI-IRR-3-F, SLI-IRR-4-F | 2596.08 kWp |
| **Oct 1-2** | SLI-IRR-3-F, SLI-IRR-4-F | 1691.28 kWp |
| **Oct 3 - Nov 11** | SLI-IRR-1-A, SLI-IRR-2-A, SLI-IRR-4-F | 1668.08 kWp |
| **After Nov 12** | SLI-IRR-1-A, SLI-IRR-2-A, Meteo Station16, SLI-IRR-4-F | 2596.08 kWp |

---

## 🔍 Validation Checklist

- [x] ✅ Sensor config updated
- [x] ✅ Database capacity values updated
- [ ] ⏳ Models re-run (required for POA calculation)
- [ ] ⏳ Validation query executed
- [ ] ⏳ Data compared with Excel
- [ ] ⏳ Differences analyzed

---

## 📊 Key Metrics to Compare

1. **Individual Sensor Values**: 
   - SLI_IRR_1_A, SLI_IRR_2_A, SLI_IRR_3_F, SLI_IRR_4_F
   - Should match Excel per sensor per day

2. **Weighted Average POA**:
   - Formula: `Sum(POA × Capacity) / Sum(Capacity)`
   - Should match Excel weighted average

3. **Active Sensors**:
   - Check `Active_Sensors` column to verify correct sensors per period

---

## 🐛 If There Are Differences

1. **Check capacity**: Verify capacity values match Excel
2. **Check active sensors**: Verify which sensors Excel uses per day
3. **Check date ranges**: Verify sensor replacement dates
4. **Check calculation method**: Verify Excel uses weighted average (not simple average)

---

## 📁 Files Ready

- ✅ Validation Query: `queries/validate_shoetown_poa_final.sql`
- ✅ Export Query: `queries/export_shoetown_poa_daily_for_excel_validation.sql`
- ✅ Sensor Config: `dbt/seeds/seed_sensor_config.csv` (updated)
- ✅ Database: Capacity values updated

---

**Last Updated**: 2025-01-XX  
**Status**: ✅ Ready for validation - just need to re-run models and execute query

