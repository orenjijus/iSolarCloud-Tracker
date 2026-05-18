# Keputusan Arsitektur dbt — Solar Data

Dokumen ini menggabungkan ringkasan keputusan arsitektur dbt setelah review dengan arsitek data senior. Untuk konteks lengkap dan bukti teknis, lihat file-file di [archive/](./archive/).

---

## 1. Executive summary

- **Performance**: O(k) untuk daily run (hanya data baru), bukan O(n).
- **Scalability**: Data 17M+ rows, tumbuh ~50K/hari; INCREMENTAL menjaga runtime tetap rendah.
- **Flexibility**: Metric/mapper update via full-refresh hanya saat diperlukan.
- **Cost**: Pengurangan signifikan I/O dan waktu run (orde 6–24x lebih cepat).

---

## 2. Final architecture (layer)

```yaml
Raw Tables (17M+ rows, growing daily)
 ↓
Staging Layer: stg_*_perf_unpivoted (INCREMENTAL) ✅
  - JSONB unpivoting
  - Filtered by timestamp

 ↓
Intermediate Layer: int_*_unified (INCREMENTAL) ✅
  - Device type filtering
  - 5-minute aggregations
  - UNION between iSolarCloud & FusionSolar

 ↓
MART 5min Layer: mart_*_performance_5min (INCREMENTAL) ✅
  - JOIN with dim_assets, dim_date, seed_metric_mapper
  - Filter NULLs

 ↓
MART Daily Layer: mart_*_performance_daily (INCREMENTAL) ✅
  - Aggregations from 5min marts
  - Site/asset level summaries

Dimensions: TABLE (small, static)
```

---

## 3. Materialization strategy

| Layer            | Materialization | Unique Key (contoh)                    | Hyperscale |
|------------------|-----------------|----------------------------------------|------------|
| Staging          | INCREMENTAL     | `[timestamp, device_ps_key, metric_id]` | ✅         |
| Intermediate     | INCREMENTAL     | `[timestamp, device_ps_key, system, metric_id]` | ✅ |
| MART 5min        | INCREMENTAL     | `[timestamp, asset_id, metric_id]`    | ✅         |
| MART Daily       | INCREMENTAL     | `[date_key, asset_id, metric_id]`      | ✅         |
| Dimensions       | TABLE           | -                                      | ❌         |

---

## 4. Mengapa VIEW staging ditolak (ringkasan)

- Predicate pushdown **tidak berlaku** untuk query dengan JSONB + LATERAL (unpivot).
- EXPLAIN menunjukkan: filter `timestamp` diterapkan **setelah** unpivot; Postgres tetap scan banyak baris sebelum filter.
- Dengan staging sebagai VIEW: daily run memproses jauh lebih banyak data → runtime 30–60x lebih lambat daripada INCREMENTAL.
- **Kesimpulan**: Staging harus INCREMENTAL (table), bukan VIEW.

Detail bukti query plan dan perbandingan ada di [archive/FINAL_VERDICT_ARCHITECT_PROPOSAL.md](./archive/FINAL_VERDICT_ARCHITECT_PROPOSAL.md).

---

## 5. Performance impact

- **Daily run (INCREMENTAL)**: ~5–10 detik, hanya ~50K baris baru.
- **Full-refresh**: Diperlukan hanya saat mapper/dimensi/logika berubah; durasi orde jam untuk rebuild penuh.
- **Before (TABLE MART)**: 30s–2 min, overwrite seluruh tabel.
- **After (INCREMENTAL MART)**: Append saja, I/O rendah.

---

## 6. Data flow (singkat)

- **Staging**: Raw → unpivot (JSONB) per sistem (iSolarCloud, FusionSolar).
- **Intermediate**: Filter device type, agregasi 5 menit, UNION antarsistem.
- **Mart 5min**: JOIN dengan dim_assets, dim_date, seed_metric_mapper → mart inverter/sensor/meter 5min.
- **Mart daily**: Agregasi dari mart 5min per date_key, asset/site.

Angka volume (row counts) per layer bisa berubah; referensi terbaru ada di [archive/DATA_FLOW_SUMMARY.md](./archive/DATA_FLOW_SUMMARY.md).

---

## 7. Hyperscale

Hyperscale diaktifkan untuk model yang compute-heavy:

- Staging unpivot (JSONB).
- Intermediate (DATE_TRUNC, AVG, UNION).
- Mart 5min (JOIN time-series).
- Mart daily (GROUP BY, agregasi).

---

## 8. Workflow

### Daily production run

```bash
dbt run
# ~5–10 detik, proses hanya data baru
```

### Mapper / logic update

```bash
dbt seed
dbt run --full-refresh --select mart_inverter_performance_5min+
dbt run --select mart_inverter_performance_daily
```

### Backfill

```bash
dbt run --full-refresh --select int_meters_unified mart_meter_performance_5min mart_meter_performance_daily
```

---

## 9. Best practices

- **Incremental filter**: Gunakan `{% if is_incremental() %} AND timestamp > (SELECT MAX(timestamp) FROM {{ this }}) {% endif %}`.
- **Unique key**: Definisikan composite unique key (mis. `['timestamp', 'asset_id', 'metric_id']`).
- **Full-refresh**: Saat mapper, dimensi, atau logika SQL berubah.

---

*Keputusan final: semua layer time-series INCREMENTAL; dimensi TABLE. Referensi lengkap di folder [archive/](./archive/).*
