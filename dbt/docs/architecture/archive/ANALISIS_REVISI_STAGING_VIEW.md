# Revisi Analisis: Staging VIEW vs INCREMENTAL

## Koreksi Analisis Sebelumnya

### Temuan Baru: MART layer = TABLE (Not Incremental)

**Arsitektur Aktual:**
```
Raw Tables (17M+ rows)
    ↓
stg_*_perf_unpivoted (INCREMENTAL)
    ↓
int_*_unified (INCREMENTAL)
    ↓
mart_*_5min (TABLE) ← **JOIN dengan mapper saat BUILD**
    ↓
mart_*_daily (TABLE) ← **JOIN dengan mapper saat BUILD**
```

### Masalah yang Mungkin Ada

**Sebelum (Analisis Salah):**
Saya assume JOIN mapper di query-time, jadi data selalu fresh.

**Sesudah (Faktual):**
MART layer adalah TABLE, jadi JOIN dengan mapper terjadi saat **BUILD TIME**, bukan query time.

```sql
-- mart_inverter_performance_5min (TABLE)
FROM {{ ref('int_inverters_unified_5min') }} i
LEFT JOIN {{ ref('seed_metric_mapper') }} m 
    ON i.metric_id = m.metric_id 
```

Ini berarti:
- ❌ Data lama di MART table sudah ter-materialize dengan mapper LAMA
- ❌ Jika mapper update, data lama TIDAK otomatis update (kecuali full-refresh)

### Apakah Arsitek Benar?

**Arsitek bilang**: "Staging sebagai VIEW agar mapper update otomatis ter-refresh"

**Realitas**:
- Mapper tidak digunakan di staging! Staging hanya unpivot.
- Mapper digunakan di MART layer.
- Masalah ada di MART layer (TABLE), bukan staging.

**Jadi saran arsitek tidak applicable karena:**
1. Mapper tidak ada di staging
2. Masalah ada di MART (yang masih TABLE, bukan INCREMENTAL)

---

## Analisis Ulang: Apakah Perlu Ubah Staging ke VIEW?

### Temuan: TIDAK ADA MASALAH DI STAGING

**Staging layer** (`stg_*_perf_unpivoted`) adalah INCREMENTAL table yang:
- ✅ Hanya menyimpan `metric_id` RAW (contoh: "pv29_u")
- ✅ TIDAK menyimpan `unified_name` atau mapping apapun
- ✅ Mapper DILAKUKAN di MART layer, bukan staging

**Jika mapper update:**
- Staging layer TIDAK terpengaruh ✅
- Intermediate layer TIDAK terpengaruh ✅ (tidak pakai mapper)
- Hanya MART layer yang perlu update ✅

### Perbandingan

#### Kasus A: Staging = VIEW
```
Raw (17M) 
  → VIEW unpivot (every query)
    → int (INCREMENTAL reads VIEW)
      → mart (TABLE with mapper)
```

**Mapper update di mart layer:**
- ✅ Full-refresh mart → gunakan mapper baru
- ✅ Data lama akan ter-mapping dengan versi terbaru

**Masalah:**
- ❌ VIEW unpivot 17M rows SETIAP QUERY ke intermediate
- ❌ Query performance lambat

#### Kasus B: Staging = INCREMENTAL (Saat Ini)
```
Raw (17M)
  → stg (INCREMENTAL table)
    → int (INCREMENTAL reads table)
      → mart (TABLE with mapper)
```

**Mapper update di mart layer:**
- ✅ Full-refresh mart → gunakan mapper baru
- ✅ Data lama akan ter-mapping dengan versi terbaru

**Masalah:**
- ❌ Query performance lebih lambat dari pre-computed
- ✅ Tapi MASIH LEBIH CEPAT dari VIEW

---

## Solusi yang Sesungguhnya Dibutuhkan

### Masalah Sebenarnya: MART Layer Masih TABLE

Jika mapper update dan ingin data historical ter-mapping dengan versi baru:

**Solusi 1: Full Refresh (Cara Standard dbt)**
```bash
dbt run --full-refresh --select mart_*
```
- Pros: Aman, idempotent, standard
- Cons: Lambat untuk data besar

**Solusi 2: Buat MART Incremental (Recommended)**

Ubch MART layer menjadi INCREMENTAL juga:
```sql
{{ config(
    materialized='incremental',
    unique_key=['timestamp_5min', 'asset_id', 'metric_id']
) }}
```

**Kemudian saat mapper update:**
```bash
dbt run --full-refresh --select mart_inverter_performance_5min
```

Hanya refresh model yang terpengaruh, tidak semua.

### Kesimpulan: Apakah Perlu Ubah Staging?

**Jawaban: TIDAK PERLU**

Alasan:
1. ✅ Staging layer tidak pakai mapper
2. ✅ Masalah ada di MART layer (TABLE vs INCREMENTAL)
3. ✅ VIEW di staging akan **MEMPERBURUK** query performance

**Yang BENAR perlu diubah:**
- Pertimbangkan MART layer jadi INCREMENTAL
- Atau keep MART sebagai TABLE dan full-refresh saat mapper update

---

## Rekomendasi Final

### Keep Current Architecture (Staging INCREMENTAL)

```yaml
stg_*_perf_unpivoted: INCREMENTAL ✅
int_*_unified: INCREMENTAL ✅
mart_*: TABLE ✅ (atau bisa INCREMENTAL)
```

### Workflow untuk Mapper Update

**Jika mapper update:**
1. Update `seed_metric_mapper.csv`
2. `dbt seed` untuk load mapper baru
3. `dbt run --full-refresh --select mart_*` untuk rebuild MART
4. Data lama akan ter-mapping dengan versi baru

**Alternatif: Pertimbangkan MART Incremental**
- MART layer juga jadi INCREMENTAL
- Saat mapper update, full-refresh MART yang terpengaruh
- Lebih granular control

---

## Summary: Respons kepada Arsitek

### Arsitek benar bahwa:
- Data lama bisa "terjebak" jika logic/materialization salah

### Tapi konteks yang arsitek berikan:
- **TIDAK applicable** karena mapper ada di MART layer, bukan staging
- Saran VIEW di staging akan memperburuk performa query

### Solusi yang benar:
- Keep staging INCREMENTAL (optimal untuk query performance)
- MART layer full-refresh saat mapper update (standard dbt)
- Atau pertimbangkan MART jadi INCREMENTAL untuk granular control

**Arsitektur saat ini SUDAH BENAR untuk query performance.**

**Yang perlu:**
- Buat MART incremental (opsional, untuk efficiency)
- Atau keep MART as TABLE dan full-refresh saat mapper update

**Intinya: Tidak perlu ubah staging jadi VIEW.**

