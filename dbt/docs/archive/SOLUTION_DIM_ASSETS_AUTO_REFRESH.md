# Solusi: Agar dim_assets Otomatis Refresh Setelah Sync Plant/Devices

## 🎯 Masalah

Saat ini:
- `stg_*__sites` dan `stg_*__devices` adalah **VIEW**
- `dim_assets` adalah **TABLE** yang depend pada VIEW tersebut
- Saat sync plant/devices, raw data berubah, tapi **VIEW tidak dianggap "perubahan"** oleh dbt
- Jadi `dim_assets` **tidak otomatis refresh**

## 🔍 Analisis: VIEW vs TABLE untuk Sites/Devices

### Current: VIEW

**Karakteristik:**
- ✅ Selalu fresh (query-time, langsung dari raw)
- ✅ Tidak consume storage
- ✅ Tidak perlu refresh
- ❌ **Tidak dianggap "perubahan" oleh dbt**
- ❌ `dim_assets` tidak otomatis refresh

**Data Volume:**
- Sites: ~19 rows (sangat kecil)
- Devices: ~500 rows (sangat kecil)
- Total: ~519 rows (sangat kecil)

### Jika Diubah ke TABLE

**Karakteristik:**
- ✅ dbt akan detect perubahan di source
- ✅ `dim_assets` akan otomatis refresh
- ✅ Bisa full refresh atau incremental
- ❌ Consume storage (tapi sangat kecil, ~KB)
- ❌ Perlu refresh setiap run (tapi cepat, <1s)

## ✅ Solusi: Ubah ke TABLE

### Rekomendasi: TABLE dengan Full Refresh

**Alasan:**
1. Data sangat kecil (~519 rows)
2. Full refresh sangat cepat (<1s)
3. Simple, tidak perlu logic incremental
4. Otomatis refresh `dim_assets`

### Implementasi

#### 1. Ubah Materialization di Model Files

**File: `dbt/models/staging/stg_fusionsolar__sites.sql`**
```sql
{{ config(materialized='table') }}  -- Ubah dari 'view' ke 'table'

SELECT 
    plant_code,
    plant_name,
    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(plant_name, ' ', '_'), '(', ''), ')', ''), ' ', '_')) as plant_name_clean,
    grid_connection_date,
    latitude,
    longitude,
    capacity
FROM {{ source('raw', 'fusionsolar_plants') }}
```

**File: `dbt/models/staging/stg_fusionsolar__devices.sql`**
```sql
{{ config(materialized='table') }}  -- Ubah dari 'view' ke 'table'

SELECT 
    dev_id,
    plant_code,
    dev_type_id,
    dev_name,
    inv_type,
    CASE 
        WHEN dev_type_id = 1 THEN 'Inverter'
        WHEN dev_type_id = 17 THEN 'Meter'
        WHEN dev_type_id = 10 THEN 'Meteo Station'
        ELSE 'Unknown'
    END as device_category
FROM {{ source('raw', 'fusionsolar_devices') }}
```

**File: `dbt/models/staging/stg_isolarcloud__sites.sql`**
```sql
{{ config(materialized='table') }}  -- Ubah dari 'view' ke 'table'

SELECT 
    ps_id,
    ps_name,
    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(ps_name, ' ', '_'), '(', ''), ')', ''), ' ', '_')) as ps_name_clean,
    install_date,
    latitude,
    longitude
FROM {{ source('raw', 'isolarcloud_power_stations') }}
```

**File: `dbt/models/staging/stg_isolarcloud__devices.sql`**
```sql
{{ config(materialized='table') }}  -- Ubah dari 'view' ke 'table'

SELECT 
    device_ps_key,
    ps_id,
    device_type,
    type_name,
    device_name,
    device_sn,
    CASE 
        WHEN device_type = 1 THEN 'Inverter'
        WHEN device_type = 7 THEN 'Meter'
        WHEN device_type = 5 THEN 'Meteo Station'
        ELSE 'Unknown'
    END as device_category
FROM {{ source('raw', 'isolarcloud_devices') }}
```

#### 2. Update dbt_project.yml (Optional)

Jika ingin override default untuk staging:

```yaml
models:
  mmsr_solar_data:
    staging:
      +materialized: view  # Default untuk perf_unpivoted (akan override dengan incremental)
      # Sites/devices akan menggunakan materialized='table' dari model file
```

**Atau lebih eksplisit:**

```yaml
models:
  mmsr_solar_data:
    staging:
      +materialized: view  # Default
      stg_fusionsolar__sites:
        +materialized: table
      stg_fusionsolar__devices:
        +materialized: table
      stg_isolarcloud__sites:
        +materialized: table
      stg_isolarcloud__devices:
        +materialized: table
```

## 📊 Perbandingan: VIEW vs TABLE

| Aspek | VIEW (Current) | TABLE (Recommended) |
|-------|----------------|----------------------|
| **Storage** | 0 KB | ~KB (sangat kecil) |
| **Refresh Time** | N/A (always fresh) | <1s (full refresh) |
| **Auto Refresh dim_assets** | ❌ Tidak | ✅ Ya |
| **Complexity** | Simple | Simple |
| **Data Volume** | ~519 rows | ~519 rows |
| **Query Performance** | Sama (data kecil) | Sama (data kecil) |
| **Maintenance** | Manual refresh dim_assets | Otomatis |

## 🎯 Workflow Setelah Perubahan

### Setelah Sync Plant/Devices

**Sebelum (dengan VIEW):**
```bash
# 1. Sync plant/devices (Python script)
# ... sync script dijalankan ...

# 2. Manual refresh dim_assets
dbt run --select dimensions.dim_assets

# 3. Refresh downstream
dbt run --select dimensions.dim_assets+
```

**Sesudah (dengan TABLE):**
```bash
# 1. Sync plant/devices (Python script)
# ... sync script dijalankan ...

# 2. Run dbt - dim_assets otomatis refresh!
dbt run
# atau
dbt run --select staging dimensions marts
```

**Keuntungan:**
- ✅ Tidak perlu manual refresh `dim_assets`
- ✅ Otomatis refresh saat staging sites/devices berubah
- ✅ Workflow lebih simple

## ⚠️ Pertimbangan

### 1. Storage Impact

**Sangat Minimal:**
- Sites: ~19 rows × ~200 bytes = ~4 KB
- Devices: ~500 rows × ~300 bytes = ~150 KB
- **Total: ~154 KB** (sangat kecil, negligible)

### 2. Performance Impact

**Tidak Signifikan:**
- Full refresh: <1s (data sangat kecil)
- Query performance: Sama (data kecil, tidak ada perbedaan)

### 3. Maintenance

**Lebih Simple:**
- Tidak perlu manual refresh `dim_assets`
- Otomatis refresh saat raw data berubah
- Workflow lebih predictable

## 🔄 Alternatif: Incremental TABLE (Not Recommended)

Jika ingin lebih efisien (tapi lebih complex):

```sql
{{ config(
    materialized='incremental',
    unique_key='plant_code',  -- atau ps_id untuk iSolarCloud
    schema='staging'
) }}

SELECT ...
FROM {{ source('raw', 'fusionsolar_plants') }}
{% if is_incremental() %}
    WHERE plant_code NOT IN (SELECT plant_code FROM {{ this }})
{% endif %}
```

**Tapi ini tidak recommended karena:**
- Data sangat kecil (~519 rows)
- Full refresh sangat cepat (<1s)
- Incremental logic tidak perlu untuk data kecil
- Full refresh lebih simple dan predictable

## ✅ Rekomendasi Final

### **Ubah Sites/Devices ke TABLE dengan Full Refresh**

**Alasan:**
1. ✅ Data sangat kecil (~519 rows, ~154 KB)
2. ✅ Full refresh sangat cepat (<1s)
3. ✅ Otomatis refresh `dim_assets`
4. ✅ Workflow lebih simple
5. ✅ Tidak ada downside yang signifikan

**Implementasi:**
- Ubah `materialized='view'` menjadi `materialized='table'` di 4 file:
  - `stg_fusionsolar__sites.sql`
  - `stg_fusionsolar__devices.sql`
  - `stg_isolarcloud__sites.sql`
  - `stg_isolarcloud__devices.sql`

**Setelah perubahan:**
- `dim_assets` akan otomatis refresh saat staging sites/devices berubah
- Tidak perlu manual refresh lagi
- Workflow lebih predictable

## 📝 Migration Steps

1. **Backup current state:**
   ```bash
   # Cek current data
   SELECT COUNT(*) FROM staging.stg_fusionsolar__sites;
   SELECT COUNT(*) FROM staging.stg_isolarcloud__sites;
   ```

2. **Ubah materialization di 4 file:**
   - `stg_fusionsolar__sites.sql`: `materialized='table'`
   - `stg_fusionsolar__devices.sql`: `materialized='table'`
   - `stg_isolarcloud__sites.sql`: `materialized='table'`
   - `stg_isolarcloud__devices.sql`: `materialized='table'`

3. **Run dbt untuk create tables:**
   ```bash
   dbt run --select stg_fusionsolar__sites stg_fusionsolar__devices stg_isolarcloud__sites stg_isolarcloud__devices
   ```

4. **Verify:**
   ```bash
   # Cek apakah tables created
   SELECT COUNT(*) FROM staging.stg_fusionsolar__sites;
   SELECT COUNT(*) FROM staging.stg_isolarcloud__sites;
   
   # Cek apakah dim_assets refresh
   dbt run --select dimensions.dim_assets
   ```

5. **Test workflow:**
   ```bash
   # Simulate sync: update raw data
   # Then run dbt
   dbt run
   
   # Verify dim_assets otomatis refresh
   ```

## 🎯 Kesimpulan

**Ya, perlu ubah staging sites/devices ke TABLE** agar `dim_assets` otomatis refresh setelah sync plant/devices.

**Benefits:**
- ✅ Otomatis refresh `dim_assets`
- ✅ Tidak perlu manual refresh
- ✅ Workflow lebih simple
- ✅ Storage impact minimal (~154 KB)
- ✅ Performance impact tidak signifikan (<1s refresh)

**Trade-off:**
- ❌ Consume sedikit storage (tapi sangat kecil, negligible)
- ❌ Perlu refresh setiap run (tapi sangat cepat, <1s)

**Rekomendasi: ✅ Lakukan perubahan ini**

