# Incremental Filter — Issue & Solusi

Penjelasan kenapa data FusionSolar bisa tidak masuk ke mart_site_performance_daily saat dbt run normal, dan solusi jangka pendek/long-term. Digabung dari: INCREMENTAL_FILTER_ISSUE_EXPLANATION, LONG_TERM_FIX_INCREMENTAL_FILTER_SCENARIO.

---

## 1. Penjelasan Issue

### Gejala

- `dbt run` normal: data **iSolarCloud** untuk suatu tanggal masuk ke `mart_site_performance_daily`, data **FusionSolar** untuk tanggal yang sama **tidak masuk**.
- Setelah re-run dengan `--vars` (reingest_start_date / reingest_end_date): data **kedua sistem** masuk.

### Root cause

Filter incremental di `mart_site_performance_daily` memakai **MAX(date_key) global** (semua sistem):

```sql
AND fsc.date_key > (SELECT MAX(date_key) FROM {{ this }})
```

- Jika sudah ada baris dengan `date_key` lebih besar (mis. 2025-12-18), maka semua data dengan `date_key <= 2025-12-18` di-**skip**, termasuk FusionSolar 2025-12-17.
- **Execution order**: iSolarCloud bisa di-run dulu → MAX(date_key) masih kecil → data iSolarCloud masuk; lalu FusionSolar di-run → MAX(date_key) sudah naik → data FusionSolar di-skip.

Jadi data dari dua sistem **saling mempengaruhi** karena satu filter global.

---

## 2. Solusi Jangka Pendek (Recommended)

Force re-process tanggal tertentu dengan vars:

```bash
dbt run --select mart_site_performance_daily+ \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

Override jendela incremental sehingga tanggal tersebut di-process ulang untuk semua sistem.

---

## 3. Solusi Long-term: Per-System Filter

Ganti filter global dengan **filter per sistem** agar iSolarCloud dan FusionSolar tidak saling mempengaruhi.

**Saat ini (problematic):**
```sql
AND m.date_key > (SELECT MAX(date_key) FROM {{ this }})
```

**Diperbaiki (per-system):**
```sql
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

Penerapan: update **semua** pemakaian filter incremental di `mart_site_performance_daily.sql` (beberapa CTE + final SELECT) dengan pola di atas. Vars reingest_start_date/reingest_end_date tetap dipakai untuk override saat reingest.

---

## 4. Checklist Debug (jika data tidak masuk)

1. **Staging**: `SELECT COUNT(*) FROM staging.stg_fusionsolar__perf_unpivoted WHERE DATE(timestamp) = 'YYYY-MM-DD';`
2. **Mart 5min**: `SELECT COUNT(*) FROM mart.mart_meter_performance_5min WHERE system = 'fusionsolar' AND date_key = 'YYYY-MM-DD';`
3. **Fact**: `SELECT COUNT(*) FROM mart.fact_site_calculations_5min WHERE system = 'fusionsolar' AND date_key = 'YYYY-MM-DD';`
4. **Final**: `SELECT COUNT(*) FROM mart.mart_site_performance_daily WHERE system = 'fusionsolar' AND date_key = 'YYYY-MM-DD';`
5. **MAX per system**: `SELECT system, MAX(date_key), COUNT(*) FROM mart.mart_site_performance_daily GROUP BY system;`

---

*Dokumen master incremental filter. File asli: INCREMENTAL_FILTER_ISSUE_EXPLANATION, LONG_TERM_FIX_INCREMENTAL_FILTER_SCENARIO — diarsipkan di dbt/docs/archive/. Lihat juga FUSIONSOLAR_TROUBLESHOOTING.md, REINGESTION_WORKFLOW.md.*
