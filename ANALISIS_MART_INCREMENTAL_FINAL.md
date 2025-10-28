# Final Analysis: Apakah MART Harus INCREMENTAL?

## Temuan Krusial: MART Layer TIDAK Re-Pivot!

Setelah membaca kode MART layer:

```sql
-- mart_inverter_performance_5min.sql
FROM {{ ref('int_inverters_unified_5min') }} i
LEFT JOIN {{ ref('dim_assets') }} da 
LEFT JOIN {{ ref('dim_date_generated') }} dd 
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
WHERE i.metric_value IS NOT NULL
```

**Hasil:**
- ✅ TIDAK ada `GROUP BY`
- ✅ TIDAK ada `MAX(CASE...)` 
- ✅ TIDAK ada re-pivot operation!

MART layer hanya:
1. SELECT columns dari intermediate
2. JOIN dengan dim_assets, dim_date, mapper
3. Filter NULL values

**Format data tetap LONG** (metric_id, metric_value) seperti intermediate!

---

## Evaluasi Rekomendasi Arsitek

### Klaim Arsitek

> "MART 5min sebagai TABLE akan lambat karena re-pivot miliaran baris"

**Counter-Argument:** ❌ SALAH. Tidak ada re-pivot di MART layer!

### Analisis Alternatif: Apakah MART Incremental Masih Worth It?

Meskipun tidak ada re-pivot, mari evaluasi:

#### Current: MART as TABLE

**Daily dbt run:**
```sql
-- mart_inverter_performance_5min (TABLE)
SELECT 
    i.*,
    da.*,
    dd.*,
    m.*
FROM int_inverters_unified_5min i  -- INCREMENTAL
LEFT JOIN dim_assets da
LEFT JOIN dim_date dd
LEFT JOIN seed_metric_mapper m
WHERE i.metric_value IS NOT NULL
```

**Operations:**
- Read: ALL data from int_* (bisa besar jika full-refresh sebelumnya)
- JOIN: 4 table JOINs
- Write: Full table overwrite

**Cost:** O(n) untuk writes (overwrite entire table)

#### Proposed: MART as INCREMENTAL

```sql
-- mart_inverter_performance_5min (INCREMENTAL)
SELECT 
    i.*,
    da.*,
    dd.*,
    m.*
FROM int_inverters_unified_5min i  -- INCREMENTAL
{% if is_incremental() %}
    WHERE i.timestamp_5min > (SELECT MAX(timestamp_5min) FROM {{ this }})
{% endif %}
LEFT JOIN dim_assets da
LEFT JOIN dim_date dd
LEFT JOIN seed_metric_mapper m
WHERE i.metric_value IS NOT NULL
```

**Operations:**
- Read: Only NEW data from int_*
- JOIN: 4 table JOINs (smaller dataset)
- Write: Insert only new rows

**Cost:** O(k) untuk writes (append only)

---

## Comparative Analysis

### Scenario 1: Daily Incremental Run (Process 50K rows)

| Aspect | MART = TABLE | MART = INCREMENTAL |
|--------|---------------|-------------------|
| Read from int | 50K rows | 50K rows ✅ |
| JOIN operations | 50K rows | 50K rows ✅ |
| Write operation | **Overwrite ALL** | **Insert 50K** ✅ |
| Index maintenance | Rebuild ALL | Update 50K ✅ |
| Runtime | ~10-30s | ~5-10s ✅ |
| Storage cost | Fixed | Growing |

### Scenario 2: After Full-Refresh (500M rows in MART)

| Aspect | MART = TABLE | MART = INCREMENTAL |
|--------|---------------|-------------------|
| Next daily run | Overwrite 500M | Append 50K ✅ |
| Write cost | 500M rows | 50K rows ✅ |
| Runtime | ~2-5 min | ~10s ✅ |

### Scenario 3: Query Performance

| Aspect | MART = TABLE | MART = INCREMENTAL |
|--------|---------------|-------------------|
| Query speed | Fast (indexes) | Fast (indexes) ✅ |
| Table size | Optimized | Slightly larger |
| Vacuum needs | Auto | Auto |

**Verdict:** INCREMENTAL MART akan lebih efisien untuk daily runs! ✅

---

## Trade-offs: INCREMENTAL vs TABLE untuk MART

### PROs INCREMENTAL MART

1. ✅ **Daily run performance**: O(k) write instead of O(n)
2. ✅ **Reduced I/O**: Append vs full overwrite
3. ✅ **Faster build time**: 5-10s vs 30s-2min
4. ✅ **Scalable**: Build time constant seiring data growth

### CONs INCREMENTAL MART

1. ❌ **Storage**: Will grow over time (bukan concern untuk modern storage)
2. ❌ **Complexity**: Need unique_key + incremental filter logic
3. ⚠️ **Vacuum**: May need periodic FULL refresh untuk cleanup

---

## Rekomendasi Final

### ✅ REKOMENDASI: UBAH MART KE INCREMENTAL

**Alasan:**
1. ✅ Performance improvement untuk daily runs (5-10x faster writes)
2. ✅ Scalable architecture (build time konstan)
3. ✅ JOIN operations tetap sama (tidak berubah complexity)
4. ✅ Query performance tetap sama (indexes tetap effective)

**Implementation Strategy:**

```sql
-- mart_inverter_performance_5min.sql
{{ config(
    materialized='incremental',
    unique_key=['timestamp_5min', 'asset_id', 'metric_id'],
    indexes=[
        {'columns': ['timestamp_5min', 'asset_id'], 'type': 'btree'},
        {'columns': ['asset_id'], 'type': 'btree'},
        {'columns': ['timestamp_5min'], 'type': 'btree'}
    ]
) }}

SELECT 
    i.timestamp_5min,
    da.asset_id,
    da.asset_name,
    da.site_name,
    da.system,
    dd.date_key,
    i.metric_id,
    m.unified_name as metric_name,
    m.metric_group,
    m.metric_unit,
    i.metric_value
FROM {{ ref('int_inverters_unified_5min') }} i
LEFT JOIN {{ ref('dim_assets') }} da 
    ON CONCAT(UPPER(LEFT(i.system, 3)), '_', i.device_ps_key) = da.asset_id
LEFT JOIN {{ ref('dim_date_generated') }} dd 
    ON dd.date_key = DATE(i.timestamp_5min)
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON i.metric_id = m.metric_id 
    AND m.used = 'yes'
WHERE i.metric_value IS NOT NULL
{% if is_incremental() %}
    AND i.timestamp_5min > (SELECT MAX(timestamp_5min) FROM {{ this }})
{% endif %}
```

---

## Kesimpulan

### Arsitek Awal: ❌ Salah (VIEW proposal)
### Arsitek Revisi: ✅ Benar (INCREMENTAL MART)

**Perbedaan:**
- Arsitek pertama salah asumsi tentang re-pivot di MART
- TAPI analisis performa untuk INCREMENTAL MART tetap valid
- INCREMENTAL MART akan improve daily run performance

### Verdict

✅ **Implementasi: Ubah MART 5min layer menjadi INCREMENTAL**

Benefits:
- 5-10x faster daily runs
- Scalable architecture
- No downside for query performance
- Standard dbt pattern untuk time-series data

---

## Final Architecture Recommendation

```yaml
Raw Tables (17M+ rows)
 ↓
Staging: stg_*_perf_unpivoted (INCREMENTAL) ✅
 ↓
Intermediate: int_*_unified (INCREMENTAL) ✅
 ↓
MART 5min: mart_*_performance_5min (INCREMENTAL) ← CHANGE THIS ✅
 ↓
MART Daily: mart_*_performance_daily (INCREMENTAL) ✅
```

**All layers incremental = Optimal architecture for time-series data**

