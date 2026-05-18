# Testing Guide - MMSR Daily Pipeline

## Estimasi Waktu Eksekusi

Berdasarkan log historis dari harvester dan dbt:

### FusionSolar Ingestion
- **1 hari data**: ~98-100 detik (~1.6 menit)
- **25 hari data**: ~310 detik (~5.2 menit)

### iSolarCloud Ingestion  
- **1 hari data**: ~120-180 detik (~2-3 menit) dengan parallel processing ⚡
- **Sebelum optimasi**: ~980-995 detik (~16.3 menit)
- **Optimasi**: Parallel processing untuk 8 batch (3-hour intervals) secara bersamaan

### Total Pipeline (1 hari data)
- **Ingestion (parallel)**: ~2-3 menit (FusionSolar ~1.6 menit + iSolarCloud ~2-3 menit dengan parallel processing)
- **dbt Staging** (incremental, 2 models): ~1-3 menit
  - Staging models adalah incremental, jadi hanya insert data baru
  - Untuk 1 hari data baru (~30-35K rows per platform), biasanya cepat
- **dbt Marts (5min)**: ~2-4 menit (incremental, hanya data baru)
- **dbt Facts (5min)**: ~1-3 menit (calculated metrics dari marts)
- **dbt Marts (daily)**: ~1-2 menit (aggregasi dari 5min data)
- **dbt Tests**: ~30 detik - 1 menit
- **Total estimasi**: **~6-10 menit** untuk 1 hari data (setelah optimasi parallel processing) ⚡

## Status Testing

Flow **TIDAK ERROR**, hanya butuh waktu lama karena:

1. **iSolarCloud ingestion** memang butuh ~16-17 menit untuk 1 hari
2. **Banyak log CASCADE_DEBUG** dari FusionSolar harvester (normal, tapi verbose)
3. **Proses berjalan normal** - hanya perlu menunggu sampai selesai

## Cara Test yang Lebih Cepat

### Option 1: Test dengan Date Range Kecil
Test dengan beberapa jam saja untuk verifikasi cepat:

```python
# Test dengan 1 jam data saja
start_date = "2025-12-02"
end_date = "2025-12-02"
# Tapi ini tetap akan fetch full day karena harvester design
```

### Option 2: Test Individual Tasks
Test tasks satu per satu untuk isolasi masalah:

```python
from flows.mmsr_daily_pipeline import prepare_date_window, ingest_fusionsolar_task

# Test date window
date_window = prepare_date_window()
print(f"Date window: {date_window}")

# Test FusionSolar only (lebih cepat ~2 menit)
result = ingest_fusionsolar_task(date_window)
print(f"FusionSolar result: {result}")
```

### Option 3: Monitor Progress
Gunakan Prefect UI untuk monitor progress real-time:
1. Start Prefect server: `start_server.bat`
2. Access UI: http://localhost:4200
3. Run flow dan monitor di UI

## Troubleshooting

### Flow Terlalu Lama?
- **Normal**: iSolarCloud memang butuh ~16-17 menit
- **Check**: Apakah ada error di log? (Cek file log di `fusionsolar/logs/` dan `isolarcloud/logs/`)
- **Solution**: Biarkan berjalan sampai selesai, atau test individual tasks

### Banyak Log CASCADE_DEBUG?
- **Normal**: Ada print statements untuk debugging di harvester
- **Tidak error**: Ini hanya verbose logging
- **Solution**: Bisa di-remove nanti jika tidak diperlukan

### Flow Hang/Stuck?
- **Check**: Apakah ada network issue ke API?
- **Check**: Apakah database connection OK?
- **Solution**: Check logs di `fusionsolar/logs/` dan `isolarcloud/logs/`

## Next Steps

1. **Biarkan flow berjalan sampai selesai** untuk melihat hasil akhir
2. **Monitor di Prefect UI** untuk progress real-time
3. **Check logs** setelah selesai untuk verifikasi
4. **Test dengan production schedule** setelah verifikasi berhasil

