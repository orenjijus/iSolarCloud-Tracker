# Weekly Log — Dokumentasi

Entry point untuk fitur Weekly Log: seed, mart, dan integrasi Power BI.

---

## 📄 Dokumen di folder ini

| Dokumen | Isi |
|---------|-----|
| **[WEEKLY_LOG_SUMMARY.md](./WEEKLY_LOG_SUMMARY.md)** | Ringkasan implementasi: file yang dibuat, langkah implementasi, struktur tabel |
| **[WEEKLY_LOG_POWER_BI_IMPLEMENTATION.md](./WEEKLY_LOG_POWER_BI_IMPLEMENTATION.md)** | Panduan lengkap Power BI: koneksi, relationship, visual |
| **[WEEKLY_LOG_QUICK_START.md](./WEEKLY_LOG_QUICK_START.md)** | Quick start: load data, setup Power BI |
| **[SEED_VS_MART_WEEKLY_LOG.md](./SEED_VS_MART_WEEKLY_LOG.md)** | Perbedaan seed vs mart weekly log |

---

## 🚀 Quick start

```bash
cd dbt
dbt seed --select seed_weekly_log
dbt run --select mart_weekly_log
```

Lalu di Power BI: connect ke PostgreSQL → import `marts.mart_weekly_log`, `dimensions.dim_assets`, `dimensions.dim_date_generated` → buat relationship → buat visual (lihat WEEKLY_LOG_POWER_BI_IMPLEMENTATION.md).
