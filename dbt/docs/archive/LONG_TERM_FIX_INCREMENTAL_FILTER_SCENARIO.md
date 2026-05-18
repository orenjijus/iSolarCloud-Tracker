# Skenario Implementasi: Long-term Fix untuk Incremental Filter

## 🎯 Tujuan

Memperbaiki filter incremental di `mart_site_performance_daily` agar menggunakan **per-system filter** instead of **global filter**, sehingga data dari sistem yang berbeda (iSolarCloud vs FusionSolar) tidak saling mempengaruhi.

## 📊 Masalah Saat Ini

### Current Implementation (Problematic)

Filter incremental saat ini menggunakan `MAX(date_key)` **global** (semua sistem):

```sql
-- Line 75, 236, 347, 413, 478, 866
AND m.date_key > (SELECT MAX(date_key) FROM {{ this }})
```

**Masalah:**
- Jika sudah ada data iSolarCloud dengan `date_key = 2025-12-18`, maka `MAX(date_key)` = 2025-12-18
- Filter akan skip semua data dengan `date_key <= 2025-12-18`, termasuk FusionSolar untuk 2025-12-17
- Data dari sistem yang berbeda saling mempengaruhi

## 🔧 Solusi: Per-System Filter

### Option 1: Per-System Filter (Recommended)

Menggunakan filter terpisah untuk setiap sistem:

```sql
-- Instead of:
AND m.date_key > (SELECT MAX(date_key) FROM {{ this }})

-- Use:
AND (
    (m.system = 'fusionsolar' AND m.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'fusionsolar'
    ))
    OR
    (m.system = 'isolarcloud' AND m.date_key > (
        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
        FROM {{ this }} 
        WHERE system = 'isolarcloud'
    ))
)
```

**Keuntungan:**
- ✅ Setiap sistem memiliki filter sendiri
- ✅ Data dari sistem yang berbeda tidak saling mempengaruhi
- ✅ Jika iSolarCloud punya data 2025-12-18, FusionSolar tetap bisa insert 2025-12-17

**Kekurangan:**
- ⚠️ Query sedikit lebih kompleks
- ⚠️ Perlu update di 6 tempat di model

### Option 2: Timestamp-Based Filter

Menggunakan `timestamp` instead of `date_key`:

```sql
-- Instead of:
AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})

-- Use:
AND fsc.timestamp > (SELECT MAX(timestamp) FROM {{ this }} WHERE system = fsc.system)
```

**Keuntungan:**
- ✅ Lebih granular (timestamp vs date)
- ✅ Per-system filter built-in

**Kekurangan:**
- ⚠️ Perlu pastikan `timestamp` ada di semua CTEs
- ⚠️ Perlu update di semua CTEs yang tidak punya `timestamp`

## 📝 Skenario Implementasi Lengkap

### Step 1: Identifikasi Semua Filter Incremental

Ada **6 tempat** di `mart_site_performance_daily.sql` yang perlu di-update:

1. **Line 75** - `meter_daily_stats` CTE
2. **Line 236** - `daily_energy` CTE  
3. **Line 347** - `daily_ghi` CTE
4. **Line 413** - `daily_poa_per_sensor` CTE
5. **Line 478** - `daily_availability` CTE
6. **Line 866** - Final SELECT

### Step 2: Update Setiap Filter

#### 2.1. Filter di `meter_daily_stats` (Line 75)

**Current:**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: re-process same date range
        AND m.date_key >= '{{ var("reingest_start_date") }}'::date
        AND m.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Default incremental: only new data
        AND m.date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
{% endif %}
```

**Updated (Per-System):**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: re-process same date range
        AND m.date_key >= '{{ var("reingest_start_date") }}'::date
        AND m.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Default incremental: per-system filter
        AND (
            (m.system = 'fusionsolar' AND m.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            ))
            OR
            (m.system = 'isolarcloud' AND m.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            ))
        )
    {% endif %}
{% endif %}
```

#### 2.2. Filter di `daily_energy` (Line 236)

**Current:**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        AND "date_key"::date >= '{{ var("reingest_start_date") }}'::date
        AND "date_key"::date <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        AND date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
{% endif %}
```

**Updated (Per-System):**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        AND "date_key"::date >= '{{ var("reingest_start_date") }}'::date
        AND "date_key"::date <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Per-system filter: need to get system from site_name
        AND (
            (e.site_name IN (SELECT site_name FROM {{ ref('dim_assets') }} WHERE system = 'fusionsolar' AND asset_level = 'Site')
             AND e.date_key > (
                 SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                 FROM {{ this }} 
                 WHERE system = 'fusionsolar'
             ))
            OR
            (e.site_name IN (SELECT site_name FROM {{ ref('dim_assets') }} WHERE system = 'isolarcloud' AND asset_level = 'Site')
             AND e.date_key > (
                 SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                 FROM {{ this }} 
                 WHERE system = 'isolarcloud'
             ))
        )
    {% endif %}
{% endif %}
```

**Note:** Untuk CTEs yang tidak punya `system` column, perlu join dengan `dim_assets` atau pass `system` dari upstream CTE.

#### 2.3. Filter di `daily_ghi` (Line 347)

**Current:**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        AND msd.date_key >= '{{ var("reingest_start_date") }}'::date
        AND msd.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        AND msd.date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
{% endif %}
```

**Updated (Per-System):**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        AND msd.date_key >= '{{ var("reingest_start_date") }}'::date
        AND msd.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Per-system filter: join with dim_assets to get system
        AND EXISTS (
            SELECT 1 
            FROM {{ ref('dim_assets') }} da 
            WHERE da.site_name = g.site_name 
                AND da.asset_level = 'Site'
                AND (
                    (da.system = 'fusionsolar' AND g.date_key > (
                        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                        FROM {{ this }} 
                        WHERE system = 'fusionsolar'
                    ))
                    OR
                    (da.system = 'isolarcloud' AND g.date_key > (
                        SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                        FROM {{ this }} 
                        WHERE system = 'isolarcloud'
                    ))
                )
        )
    {% endif %}
{% endif %}
```

#### 2.4. Filter di `daily_poa_per_sensor` (Line 413)

Similar approach seperti `daily_ghi`.

#### 2.5. Filter di `daily_availability` (Line 478) - **MOST IMPORTANT**

**Current:**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: re-process same date range
        AND fsc.date_key >= '{{ var("reingest_start_date") }}'::date
        AND fsc.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Default incremental: only new data
        AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
{% endif %}
```

**Updated (Per-System):**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        -- Override window: re-process same date range
        AND fsc.date_key >= '{{ var("reingest_start_date") }}'::date
        AND fsc.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Default incremental: per-system filter
        AND (
            (fsc.system = 'fusionsolar' AND fsc.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            ))
            OR
            (fsc.system = 'isolarcloud' AND fsc.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            ))
        )
    {% endif %}
{% endif %}
```

**Note:** Ini adalah filter yang paling penting karena `fsc` sudah punya `system` column dari `fact_site_calculations_5min`.

#### 2.6. Filter di Final SELECT (Line 866)

**Current:**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        AND sm.date_key >= '{{ var("reingest_start_date") }}'::date
        AND sm.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        AND sm.date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
{% endif %}
```

**Updated (Per-System):**
```sql
{% if is_incremental() %}
    {% if var('reingest_start_date', none) and var('reingest_end_date', none) %}
        AND sm.date_key >= '{{ var("reingest_start_date") }}'::date
        AND sm.date_key <= '{{ var("reingest_end_date") }}'::date
    {% else %}
        -- Per-system filter: da.system is available in final SELECT
        AND (
            (da.system = 'fusionsolar' AND sm.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'fusionsolar'
            ))
            OR
            (da.system = 'isolarcloud' AND sm.date_key > (
                SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
                FROM {{ this }} 
                WHERE system = 'isolarcloud'
            ))
        )
    {% endif %}
{% endif %}
```

## 🧪 Testing Skenario

### Test Case 1: Data Baru dari Sistem yang Berbeda

**Setup:**
- Sudah ada data iSolarCloud untuk 2025-12-18
- Ingest data baru: FusionSolar untuk 2025-12-17

**Expected Behavior:**
- ✅ Dengan per-system filter: FusionSolar 2025-12-17 **masuk**
- ❌ Dengan global filter: FusionSolar 2025-12-17 **di-skip**

### Test Case 2: Data Baru dari Sistem yang Sama

**Setup:**
- Sudah ada data iSolarCloud untuk 2025-12-18
- Ingest data baru: iSolarCloud untuk 2025-12-17

**Expected Behavior:**
- ❌ Dengan per-system filter: iSolarCloud 2025-12-17 **di-skip** (karena 2025-12-17 < 2025-12-18)
- ❌ Dengan global filter: iSolarCloud 2025-12-17 **di-skip**

**Note:** Ini adalah behavior yang benar - data lama tidak boleh masuk jika sudah ada data baru.

### Test Case 3: Data Baru dari Kedua Sistem

**Setup:**
- Tidak ada data existing
- Ingest data baru: iSolarCloud dan FusionSolar untuk 2025-12-17

**Expected Behavior:**
- ✅ Dengan per-system filter: Kedua sistem **masuk**
- ✅ Dengan global filter: Kedua sistem **masuk**

## ⚠️ Considerations

### 1. Performance Impact

**Per-System Filter:**
- Setiap filter perlu 2 subqueries (satu per sistem)
- Total: 6 filter × 2 subqueries = 12 subqueries tambahan
- **Impact:** Query mungkin sedikit lebih lambat, tapi masih acceptable

**Mitigation:**
- Index pada `(system, date_key)` di tabel `mart_site_performance_daily`
- Subquery akan cepat karena menggunakan index

### 2. Backward Compatibility

**Re-ingest dengan --vars:**
- Tetap bekerja seperti sebelumnya
- Filter per-system hanya aktif saat normal incremental run

### 3. Edge Cases

**Jika sistem baru ditambahkan:**
- Perlu update filter untuk include sistem baru
- Atau gunakan dynamic approach:

```sql
-- Dynamic approach (lebih fleksibel)
AND fsc.date_key > (
    SELECT COALESCE(MAX(date_key), '1900-01-01'::date) 
    FROM {{ this }} 
    WHERE system = fsc.system
)
```

**Jika sistem dihapus:**
- Filter tetap bekerja (hanya tidak ada data untuk sistem tersebut)

## 📋 Implementation Checklist

- [ ] Backup model file saat ini
- [ ] Update filter di `meter_daily_stats` (Line 75)
- [ ] Update filter di `daily_energy` (Line 236)
- [ ] Update filter di `daily_ghi` (Line 347)
- [ ] Update filter di `daily_poa_per_sensor` (Line 413)
- [ ] Update filter di `daily_availability` (Line 478) - **PRIORITY**
- [ ] Update filter di Final SELECT (Line 866)
- [ ] Test dengan data existing
- [ ] Test dengan data baru dari sistem berbeda
- [ ] Test dengan re-ingest vars
- [ ] Monitor performance
- [ ] Update dokumentasi

## 🚀 Rollout Strategy

### Phase 1: Testing
1. Implement di development environment
2. Test dengan berbagai skenario
3. Monitor performance

### Phase 2: Staging
1. Deploy ke staging environment
2. Run dengan data production-like
3. Verify hasil

### Phase 3: Production
1. Deploy ke production
2. Monitor untuk beberapa hari
3. Verify tidak ada regresi

## 📚 Related Documentation

- [INCREMENTAL_FILTER_ISSUE_EXPLANATION.md](./INCREMENTAL_FILTER_ISSUE_EXPLANATION.md)
- [TROUBLESHOOTING_FUSIONSOLAR_DATA.md](./TROUBLESHOOTING_FUSIONSOLAR_DATA.md)
- [REINGESTION_WORKFLOW.md](./REINGESTION_WORKFLOW.md)

