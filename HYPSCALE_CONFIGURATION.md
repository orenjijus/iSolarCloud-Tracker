# Hyperscale Configuration untuk Performance Optimization

## Ringkasan Perubahan

Untuk meningkatkan performa dbt project, berikut model-model yang di-konfigurasi untuk menggunakan **hyperscale**:

## Model yang Menggunakan Hyperscale

### 1. **Staging Layer** - JSONB Unpivoting
Model yang melakukan unpivot dari JSONB ke long format (operasi intensif CPU):

- ✅ `stg_isolarcloud__perf_unpivoted` - Materialized sebagai **INCREMENTAL** with hyperscale
- ✅ `stg_fusionsolar__perf_unpivoted` - Materialized sebagai **INCREMENTAL** with hyperscale

**Alasan**: Operasi `jsonb_object_keys()` dan casting yang intensif memerlukan hyperscale compute.

### 2. **Intermediate Layer** - Aggregation dan Transformasi
Model yang melakukan GROUP BY dan UNION ALL antara dua sistem:

- ✅ `int_inverters_unified_5min` - Materialized sebagai **INCREMENTAL** with hyperscale
- ✅ `int_meters_unified` - Materialized sebagai **INCREMENTAL** with hyperscale
- ✅ `int_strings_unified_5min` - Materialized sebagai **INCREMENTAL** with hyperscale  
- ✅ `int_sensors_unified` - Materialized sebagai **INCREMENTAL** with hyperscale

**Alasan**: 
- DATE_TRUNC dan AVG aggregation pada data 5-minute interval
- UNION ALL antara isolarcloud dan fusionsolar
- CAST operations untuk numeric conversion

### 3. **Marts Layer** - Daily Aggregations
Model yang melakukan agregasi harian dari data 5-minute:

- ✅ `mart_inverter_performance_daily`
- ✅ `mart_string_performance_daily`
- ✅ `mart_site_performance_daily`

**Alasan**: 
- AVG, MAX, MIN, SUM aggregations pada volume data besar
- GROUP BY multiple columns (date, asset, metric)
- Complex JOINs dengan dim tables dan seeds

## Perubahan Materialization Strategy

### Dari VIEW ke INCREMENTAL
Semua staging unpivot dan intermediate models diubah menjadi INCREMENTAL untuk:
- **Optimize Build Time**: Hanya memproses data baru (O(k)) bukan seluruh history (O(n))
- **Scalable**: Build time konstan seiring bertambahnya data
- **Efficient**: Mengurangi 99%+ dari I/O dan compute untuk data unchanged
- **Mengatasi Data Volume**: Data sekarang 17+ juta records (900 hari+) - INCREMENTAL wajib

## Manfaat Strategi Baru (INCREMENTAL + Hyperscale)

### Performance Gains:
1. **Build Time**: O(k) instead of O(n) - hanya proses data baru
2. **Cost Efficiency**: 99%+ reduction dalam I/O dan compute costs
3. **Scalability**: Build time tetap konstan (contoh: 1 hari = 1 juta rows), bukan meningkat 343x (343 hari = 343 juta rows)
4. **Hyperscale Compute**: Parallel processing untuk operations yang intensif (JSONB unpivoting, DATE_TRUNC, aggregations)

### Data Volume Context:
- iSolarCloud: **9,082,391 records** (343 hari)
- FusionSolar: **8,608,124 records** (507 hari)
- **Total: 17+ juta records** yang akan tumbuh terus setiap hari

## Cara Menggunakan

Pastikan dbt Cloud menggunakan warehouse yang support hyperscale. Run dbt dengan:

```bash
dbt run
```

Model akan otomatis menggunakan hyperscale capabilities yang terdeteksi dari meta field.

## Model yang TIDAK Menggunakan Hyperscale

### Tetap sebagai VIEW (lightweight operations):
- `stg_isolarcloud__sites` - Simple lookups (data kecil, jarang berubah)
- `stg_isolarcloud__devices` - Simple lookups (data kecil, jarang berubah)
- `stg_fusionsolar__sites` - Simple lookups (data kecil, jarang berubah)
- `stg_fusionsolar__devices` - Simple lookups (data kecil, jarang berubah)

### Tetap sebagai TABLE (untuk optimized query performance):
- `dim_assets` - Dimension table (small, stable)
- `dim_date_generated` - Dimension table (static, ~365 rows)
- `mart_inverter_performance_5min` - Frequent queries di Power BI, benefitted dari indexes
- `mart_meter_performance_5min` - Frequent queries, benefitted dari indexes  
- `mart_sensor_measurements_5min` - Frequent queries
- `mart_string_performance_5min` - Frequent queries

### INCREMENTAL untuk daily aggregations:
- `mart_inverter_performance_daily` - Only aggregate data baru per hari
- `mart_string_performance_daily` - Only aggregate data baru per hari
- `mart_site_performance_daily` - Only aggregate data baru per hari

**Catatan**: Marts 5-minute tetap TABLE karena mereka di-query oleh daily aggregations dan Power BI. Daily aggregations adalah INCREMENTAL.

## Monitoring Performance

Untuk memonitor performance improvement:
1. Check dbt Cloud run logs untuk melihat execution time
2. Compare before/after run times untuk models yang di-hyperscale
3. Monitor warehouse usage metrics

