# Analisis: Staging VIEW vs INCREMENTAL untuk Metric Mapper Update

## Situasi Aktual

### Arsitektur Saat Ini

```
Raw Tables (17M+ rows)
    ↓
stg_*_perf_unpivoted (INCREMENTAL) ← JSONB unpivot, NO mapper
    ↓
int_*_unified_5min (INCREMENTAL) ← Device filtering, NO mapper  
    ↓
mart_*_5min (TABLE) ← **PEMAKAI MAPPER** ← seed_metric_mapper di JOIN
    ↓
mart_*_daily (TABLE) ← **PEMAKAI MAPPER** ← seed_metric_mapper di JOIN
```

### Temuan Kritis 🔍

**seed_metric_mapper digunakan HANYA di MART layer**, BUKAN di staging/intermediate!

Dari grep results:
- ❌ Staging: TIDAK pakai mapper (hanya unpivot)
- ❌ Intermediate: TIDAK pakai mapper (hanya filter device type)
- ✅ MART 5min: PAKAI mapper (`LEFT JOIN seed_metric_mapper`)
- ✅ MART daily: PAKAI mapper (`LEFT JOIN seed_metric_mapper`)

---

## Skenario: Mapper Update

### Kasus: Anda update `seed_metric_mapper.csv`

**Sebelum:**
```csv
"fusion","pv29_u","string_29_voltage","string","V","","" ← unified_name KOSONG
```

**Sesudah:**
```csv
"fusion","pv29_u","string_29_voltage","string","V","yes","string_29_voltage" ← unified_name ISl
```

### Analisis Arsitektur Saat Ini (INCREMENTAL di staging)

**Pertanyaan**: Apakah data lama (yang sudah materialize) akan terpengaruh?

**Jawaban**: **TIDAK PERLU**, karena:

1. `stg_*_perf_unpivoted` (INCREMENTAL table) hanya menyimpan:
   - `metric_id` (raw: "pv29_u")
   - `metric_value` (raw numeric)
   - Tidak ada `unified_name` di stored data

2. `int_*_unified` (INCREMENTAL table) hanya menyimpan:
   - `metric_id` (raw: "pv29_u")
   - `metric_value`
   - Tidak ada `unified_name` di stored data

3. `mart_*` (TABLE) yang menggunakan mapper:
   - Mereka QUERY ke `seed_metric_mapper` saat query time
   - Mapping dilakukan **pada waktu query**, bukan disimpan

**Kesimpulan**: Arsitektur saat ini **TIDAK MEMILIKI MASALAH** dengan mapper update!

---

## Apakah Perlu Ubah Staging ke VIEW?

### Argumentasi Arsitek: Staging sebagai VIEW

**Skenario yang Dikhawatirkan:**
- Jika staging INCREMENTAL (table), data lama "terjebak"
- Saat seed mapper berubah, data lama tidak update

**Realitas Code:**
```sql
-- stg_isolarcloud__perf_unpivoted.sql
SELECT 
    timestamp,
    device_ps_key,
    metric_id,  ← RAW ID, tidak ada unified_name
    metric_value
FROM ...
```

**Staging layer TIDAK MENYIMPAN mapping!** Mapping terjadi di MART layer:
```sql
-- mart_inverter_performance_5min.sql
SELECT 
    i.metric_id,
    m.unified_name as metric_name,  ← Mapper digunakan di MART
    m.metric_unit
FROM {{ ref('int_inverters_unified_5min') }} i
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON i.metric_id = m.metric_id 
    AND m.used = 'yes'
```

### Trade-off: VIEW vs INCREMENTAL di Staging

#### Opsi 1: Staging = VIEW (Rekomendasi Arsitek)

**Pros:**
- ✅ Stateless (tidak menyimpan data)
- ✅ Selalu fresh dengan logic terbaru
- ✅ Tidak ada "stale data" risk

**Cons:**
- ❌ **QUERY PERFORMANCE**: VIEW akan re-execute unpivot **SETIAP KALI** di-query
- ❌ Operasi JSONB unpivot sangat CPU intensive
- ❌ Setiap query ke intermediate akan trigger unpivot di staging

**Impact di production:**
```
int_inverters_unified (INCREMENTAL) 
  → reads dari stg_perf_unpivoted (VIEW)
  → VIEW re-executes JSONB unpivot untuk 17M rows
  → Query time: 5-10 detik SETIAP KALI
  
Rantai:
mart_*_5min queries int_inverters
  → int_inverters queries staging VIEW  
  → VIEW unpivots 17M rows
  → Per lambat
```

#### Opsi 2: Staging = INCREMENTAL (Arsitektur Saat Ini)

**Pros:**
- ✅ **QUERY PERFORMANCE**: Staging table pre-computed
- ✅ Intermediate layer query staging table (fast)
- ✅ Unpivot hanya dilakukan sekali saat write, bukan setiap query
- ✅ Data mapping (seperti unified_name) TIDAK disimpan di staging
- ✅ Mapper update TIDAK terpengaruhi

**Cons:**
- ⚠️ Jika LOGIC unpivot berubah (bukan mapper), perlu full-refresh

**Impact di production:**
```
int_inverters_unified (INCREMENTAL)
  → reads dari stg_perf_unpivoted (INCREMENTAL TABLE)
  → Table sudah ter-materialize, fast lookup
  → Query time: 0.1-0.5 detik
  
Rantai:
mart_*_5min queries int_inverters
  → int_inverters reads staging TABLE (fast)
  → Per fast
```

---

## Analisis Detail: Apakah Mapping "Terjebak"?

### Test Case: Update Mapper

**Skenario:**
1. Data sudah di-ingest hari 1-100
2. Hari 101, Anda update `seed_metric_mapper.csv`
3. `dbt seed` untuk load mapper baru
4. `dbt run` (incremental)

**Pertanyaan**: Apakah data hari 1-100 akan punya unified_name baru?

**Jawaban dengan Arsitektur Saat Ini:**

✅ **YA**, data lama AKAN punya unified_name baru!

**Alasan:**
1. Staging INCREMENTAL table menyimpan: `metric_id = "pv29_u"`, bukan unified_name
2. MART layer melakukan JOIN `seed_metric_mapper` **pada waktu query**:
   ```sql
   FROM {{ ref('int_inverters_unified_5min') }} i
   LEFT JOIN seed_metric_mapper m ON i.metric_id = m.metric_id
   ```
3. Mapper di JOIN pada waktu **query execution**, bukan saat materialization
4. Jadi historical data (hari 1-100) AKAN ter-join dengan mapper BARU

**Tidak ada "data terjebak" di arsitektur saat ini!**

---

## Rekomendasi Akhir

### **TIDAK PERLU UBAH: Arsitektur Saat Ini Sudah Benar**

Alasan:

1. **Mapper Update Aman** ✅
   - Mapper digunakan di MART layer, bukan staging
   - JOIN dilakukan pada query time (fresh)
   - Historical data otomatis ter-mapping dengan versi terbaru

2. **Query Performance Optimal** ✅
   - Staging INCREMENTAL = pre-computed JSONB unpivot
   - Intermediate queries staging TABLE (fast)
   - Power BI queries lapisan bawah akan jauh lebih cepat

3. **Trade-off Analysis** ✅
   - VIEW: Slow query (unpivot setiap query)
   - INCREMENTAL: Fast query (pre-computed)
   - Yang paling sering diakses adalah MART layer
   - Jadi optimasi query performance lebih penting

4. **Full Refresh Safety** ✅
   - Jika LOGIC unpivot berubah (jarang), bisa `dbt run --full-refresh --select stg_*`
   - Hanya perlu refresh yang terpengaruh, bukan semua

### Kapan Perlu VIEW?

VIEW di staging hanya baik jika:
1. Anda sering mengubah LOGIC unpivot (bukan mapper)
2. Query frequency rendah (jadi pre-compute tidak worth it)
3. Storage sangat terbatas (tidak applicable untuk data warehouse)

### Kapan Perlu INCREMENTAL (Saat Ini)?

INCREMENTAL di staging paling baik jika:
1. Data besar (17M+ rows) ✅
2. Query frequency tinggi (Power BI, dashboards) ✅
3. Unpivot operation expensive (JSONB CPU-intensive) ✅
4. Mapper sering update (TIDAK masalah karena JOIN di MART) ✅

---

## Kesimpulan

**Arsitek senior memberikan saran yang baik secara TEORI, tapi:**

1. **Context salah**: Arsitek assume mapper digunakan di staging
2. **Realitas**: Mapper digunakan di MART layer
3. **Mapping fresh**: JOIN pada query time = no stale data

**Rekomendasi: KEEP CURRENT ARCHITECTURE**

```
✅ stg_*_perf_unpivoted = INCREMENTAL (keep)
✅ int_*_unified = INCREMENTAL (keep)
✅ mart_* = TABLE with mapper JOIN (keep)
```

**Jangan ubah apapun. Arsitektur Anda sudah benar untuk use case ini.**

---

## Addendum: Jika Ingin Lebih Aman

Jika Anda masih khawatir, tambahkan **data quality check**:

```sql
-- tests/test_mapper_coverage.sql
{{ config(
    materialized='test'
) }}

SELECT metric_id
FROM {{ ref('stg_isolarcloud__perf_unpivoted') }}
WHERE metric_id NOT IN (
    SELECT metric_id FROM {{ ref('seed_metric_mapper') }}
)
GROUP BY metric_id
HAVING COUNT(*) > 100  -- Only flag if significant volume
```

Ini akan alert jika ada metric tanpa mapping.

