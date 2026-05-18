# Arsitektur dbt — Ringkasan

Keputusan arsitektur dbt untuk pipeline solar data: staging → intermediate → mart, materialisasi INCREMENTAL, dan alasan teknis (mengapa VIEW staging ditolak, mengapa INCREMENTAL dipilih).

---

## 📄 Dokumen utama

| Dokumen | Isi |
|---------|-----|
| **[ARCHITECTURE_DECISIONS.md](./ARCHITECTURE_DECISIONS.md)** | **Baca ini** — Ringkasan final, diagram layer, materialisasi, performa, workflow, best practices |
| [docs/architecture.md](../../../docs/architecture.md) | Arsitektur sistem secara keseluruhan (ingestion → DB → dbt → BI) |

---

## Kesimpulan singkat

- **Semua layer time-series**: INCREMENTAL (staging, intermediate, mart 5min, mart daily).
- **Dimensi**: TABLE (kecil, statis).
- **Alasan**: VIEW staging tidak dapat predicate pushdown pada JSONB + LATERAL → full scan; INCREMENTAL memproses hanya data baru (O(k)) → daily run 5–10 detik.
- **Hyperscale**: Diaktifkan untuk model compute-heavy (unpivot, agregasi).

Detail lengkap, bukti query plan, dan workflow ada di [ARCHITECTURE_DECISIONS.md](./ARCHITECTURE_DECISIONS.md).

---

## Arsip analisis awal

Analisis dan counter-argument yang mendahului keputusan final (staging vs VIEW, incremental vs TABLE) disimpan di **[archive/](./archive/)** untuk referensi:

- ANALISIS_STAGING_VIEW_VS_INCREMENTAL.md  
- ANALISIS_REVISI_STAGING_VIEW.md  
- ANALISIS_MART_INCREMENTAL_FINAL.md  
- COUNTER_ARGUMENT_ARCHITECT.md  
- DATA_FLOW_SUMMARY.md  
- FINAL_ARCHITECTURE_SUMMARY.md  
- FINAL_VERDICT_ARCHITECT_PROPOSAL.md  
- RESPONSE_TO_ARCHITECT_FEEDBACK.md  
