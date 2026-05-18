# Seed Reload Issue - Manual Fix Required

**Date**: 2025-01-XX  
**Issue**: dbt seed command failed due to schema.yml duplication error

---

## ⚠️ Current Status

**Problem**: Seed belum ter-reload, jadi capacity di database masih nilai lama:
- SLI-IRR-1-A: 928.0 (seharusnya 452.4) ❌
- SLI-IRR-2-A: 763.28 (seharusnya 452.4) ❌
- SLI-IRR-3-F: 928.0 ✅ (sudah benar)
- SLI-IRR-4-F: 763.28 ✅ (sudah benar)

**Root Cause**: dbt error karena duplikasi schema.yml:
```
dbt found two schema.yml entries for the same resource named stg_isolarcloud__perf_unpivoted
```

---

## 🔧 Solutions

### Option 1: Fix dbt Error First (Recommended)

1. **Fix schema.yml duplication**:
   - Check `models\staging\staging.yml`
   - Check `models\marts\_tags.yml`
   - Remove duplicate entry for `stg_isolarcloud__perf_unpivoted`

2. **Then reload seed**:
   ```bash
   cd "C:\Users\Administrator\Documents\Code\MMSR API - Server MA\dbt"
   dbt seed --select seed_sensor_config
   ```

### Option 2: Manual Update via SQL (Quick Fix)

Jika tidak bisa fix dbt error sekarang, bisa update langsung via SQL:

```sql
-- Update capacity untuk SLI-IRR-1-A
UPDATE staging.seed_sensor_config
SET sensor_capacity = '452.4'
WHERE device_id = '1479456_5_24_1'
    AND dev_name = 'SLI-IRR-1-A';

-- Update capacity untuk SLI-IRR-2-A
UPDATE staging.seed_sensor_config
SET sensor_capacity = '452.4'
WHERE device_id = '1479456_5_25_1'
    AND dev_name = 'SLI-IRR-2-A';

-- Verify updates
SELECT device_id, dev_name, sensor_capacity
FROM staging.seed_sensor_config
WHERE device_id IN ('1479456_5_24_1', '1479456_5_25_1')
ORDER BY device_id;
```

**Note**: Setelah update via SQL, perlu re-run models:
```bash
dbt run --select mart_sensor_daily mart_site_performance_daily
```

---

## ✅ After Fix

Setelah capacity diupdate, verify dengan query:

```sql
SELECT 
    device_id,
    dev_name,
    sensor_capacity,
    CASE 
        WHEN device_id = '1479456_5_24_1' AND sensor_capacity::numeric = 452.4 THEN '✅ CORRECT'
        WHEN device_id = '1479456_5_25_1' AND sensor_capacity::numeric = 452.4 THEN '✅ CORRECT'
        ELSE '❌ WRONG'
    END as status
FROM staging.seed_sensor_config
WHERE device_id IN ('1479456_5_24_1', '1479456_5_25_1');
```

---

## 📋 Next Steps

1. [ ] Fix dbt error OR update via SQL
2. [ ] Verify capacity values
3. [ ] Re-run models
4. [ ] Run validation query
5. [ ] Compare with Excel

---

**Last Updated**: 2025-01-XX

