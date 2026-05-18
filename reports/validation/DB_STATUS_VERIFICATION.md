# Database Status Verification

**Date**: 2025-01-XX  
**Status**: ✅ Sensor Config Updated, ⏳ Models Need Re-run

---

## ✅ Sensor Config Status (staging.seed_sensor_config)

**All sensors are in database with CORRECT capacity values:**

| Sensor | Device ID | Capacity | Status |
|--------|-----------|----------|--------|
| SLI-IRR-1-A | 1479456_5_24_1 | 452.4 kWp | ✅ CORRECT |
| SLI-IRR-2-A | 1479456_5_25_1 | 452.4 kWp | ✅ CORRECT |
| SLI-IRR-3-F | 1479456_5_17_1 | 928 kWp | ✅ CORRECT |
| SLI-IRR-4-F | 1479456_5_18_1 | 763.28 kWp | ✅ CORRECT |
| Meteo Station16 | 1479456_5_27_1 | 928 kWp | ✅ CORRECT |

**Conclusion**: ✅ **Sensor config sudah benar di database!**

---

## ⚠️ Models Status (mart_sensor_daily)

**Problem**: Models belum di-re-run, jadi:
- `mart_sensor_daily` masih menggunakan data lama (jika ada)
- POA calculation belum menggunakan capacity yang baru
- Meteo Station16 mungkin belum muncul di mart_sensor_daily

**Solution**: Perlu re-run models, tapi ada dbt error yang perlu di-fix dulu.

---

## 🔧 Fix dbt Error First

**Error**: 
```
dbt found two schema.yml entries for the same resource named stg_isolarcloud__perf_unpivoted
```

**Files involved**:
- `models\staging\staging.yml`
- `models\marts\_tags.yml`

**Action**: Remove duplicate entry for `stg_isolarcloud__perf_unpivoted` from one of these files.

---

## 📋 Options

### Option 1: Fix dbt Error Then Re-run (Recommended)

1. Fix schema.yml duplication
2. Run: `dbt seed --select seed_sensor_config` (optional, sudah diupdate via SQL)
3. Run: `dbt run --select mart_sensor_daily mart_site_performance_daily`

### Option 2: Skip Seed, Just Re-run Models

Karena sensor config sudah diupdate via SQL, bisa langsung re-run models:

```bash
dbt run --select mart_sensor_daily mart_site_performance_daily
```

**Note**: Masih perlu fix dbt error dulu.

### Option 3: Manual Verification (Temporary)

Bisa langsung query `staging.seed_sensor_config` untuk validasi, tapi POA calculation di `mart_sensor_daily` masih menggunakan capacity lama sampai models di-re-run.

---

## ✅ Summary

- ✅ **Sensor config**: Sudah benar di database (updated via SQL)
- ⏳ **Models**: Perlu re-run untuk update POA calculation
- ⚠️ **dbt Error**: Perlu di-fix sebelum bisa re-run models

---

**Last Updated**: 2025-01-XX

