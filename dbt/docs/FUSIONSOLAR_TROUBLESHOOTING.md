# FusionSolar — Troubleshooting

Panduan troubleshooting ketika data FusionSolar tidak ter-update atau tidak sampai ke mart (mis. `mart_site_performance_daily`). Konsolidasi dari: TROUBLESHOOTING_FUSIONSOLAR_DATA, TROUBLESHOOTING_FUSIONSOLAR_NOT_UPDATING, QUICK_FIX_FUSIONSOLAR_NOT_UPDATING, ISSUE_INCREMENTAL_FUSIONSOLAR_SKIPPED.

---

## 1. Gejala umum

- **INSERT 0 rows** pada `stg_fusionsolar__perf_unpivoted` saat `dbt run`.
- Data FusionSolar untuk tanggal tertentu tidak muncul di `mart_site_performance_daily` padahal iSolarCloud muncul.
- Data FusionSolar baru muncul setelah re-run staging FusionSolar secara manual.

---

## 2. Quick check

### Cek MAX timestamp: Raw vs Staging

```sql
SELECT 'Raw MAX' as source, MAX(collect_time) as max_ts
FROM "MMSR"."raw"."fusionsolar_historical_data"
UNION ALL
SELECT 'Staging MAX', MAX(timestamp)
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted";
```

- **Raw MAX = Staging MAX** → Tidak ada data baru; normal atau harvester belum jalan.
- **Raw MAX > Staging MAX** → Ada data baru yang belum di-pickup; lanjut ke solusi 3 (force re-ingest).

---

## 3. Penyebab & solusi

### 3.1 Tidak ada data baru di raw

- Pastikan Python harvester FusionSolar sudah dijalankan.
- Cek `raw.fusionsolar_historical_data` untuk tanggal yang dimaksud.

### 3.2 Data sudah ada di staging (duplicate)

- Unique key mencegah insert duplikat; tidak perlu tindakan jika memang sudah ter-process.

### 3.3 Incremental filter skip data

**Solusi: force re-ingest dengan vars**

```bash
dbt run --select stg_fusionsolar__perf_unpivoted+ \
  --vars '{"reingest_start_date": "2025-01-18", "reingest_end_date": "2025-01-18"}'
```

Ganti tanggal sesuai kebutuhan. Untuk mart daily saja:

```bash
dbt run --select mart_site_performance_daily+ \
  --vars '{"reingest_start_date": "2025-12-17", "reingest_end_date": "2025-12-17"}'
```

### 3.4 Data tidak sampai ke mart (layer intermediate / mart)

- Jalankan script diagnostic (mis. `dbt/scripts/check_fusionsolar_data_*.sql`) atau cek setiap layer:
  - `stg_fusionsolar__perf_unpivoted` → `mart_*_performance_5min` → `fact_*` → `mart_site_performance_daily`
- Cek **revenue meters** untuk site FusionSolar (mart daily butuh revenue meter untuk energy).
- Cek **sensor data** (GHI/POA); jika tidak ada, kolom GHI/POA bisa NULL tapi baris tetap bisa ada.

### 3.5 Execution order / data belum tersedia saat dbt run

- Pastikan **sebelum** `dbt run`, data FusionSolar untuk tanggal tersebut sudah ada di raw.
- Jika ETL FusionSolar jalan setelah dbt, jadwal perlu diatur (dbt setelah harvester) atau gunakan force re-ingest setelah data masuk.

---

## 4. Diagnosa per layer

Contoh cek per layer untuk satu tanggal (ganti schema/nama jika perlu):

```sql
SELECT 'stg_fusionsolar__perf_unpivoted' as layer, COUNT(*) FROM staging.stg_fusionsolar__perf_unpivoted WHERE DATE(timestamp) = '2025-12-17';
SELECT 'mart_meter_performance_5min' as layer, COUNT(*) FROM mart.mart_meter_performance_5min WHERE system = 'fusionsolar' AND date_key = '2025-12-17';
SELECT 'mart_site_performance_daily' as layer, COUNT(*) FROM mart.mart_site_performance_daily WHERE system = 'fusionsolar' AND date_key = '2025-12-17';
```

---

## 5. Ringkasan

| Gejala | Tindakan |
|--------|----------|
| INSERT 0 di staging | Cek raw vs staging MAX; jalankan harvester atau force re-ingest |
| Data tidak di mart daily | Cek layer per layer; revenue meter; vars reingest untuk mart |
| Data “ter-skip” | Pastikan data raw sudah ada sebelum dbt run; gunakan vars reingest |

---

*Untuk skrip diagnostik dan contoh query lengkap, lihat file asli: TROUBLESHOOTING_FUSIONSOLAR_*.md, QUICK_FIX_*, ISSUE_INCREMENTAL_*. File asli dapat diarsipkan setelah konfirmasi.*
