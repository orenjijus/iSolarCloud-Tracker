# iSolarCloud Ingestion Monitoring Guide

## Cara Monitor Progress

### 1. **Check Status Cepat** (Tanpa Menunggu)
```bash
python check_status.py
```
Menampilkan:
- Status run terakhir (SUCCESS/FAILED)
- Execution time
- Log file terbaru
- 15 baris terakhir dari log

### 2. **Monitor Real-time** (Saat Proses Berjalan)
```bash
python monitor_progress.py
```
- Menampilkan log secara real-time
- Update setiap 3 detik
- Tekan Ctrl+C untuk stop

### 3. **Test dengan Progress Monitoring**
```bash
python test_with_progress.py
```
- Menjalankan test ingestion
- Menampilkan progress real-time
- Menampilkan hasil akhir dengan analisis performance

## File-file Monitoring

### Status File
- **Lokasi**: `logs/isolarcloud_last_etl_status.txt`
- **Format**: Key-value pairs
- **Update**: Setelah ingestion selesai

### Log Files
- **Lokasi**: `logs/isolarcloud_etl_YYYYMMDD_HHMMSS.log`
- **Format**: Standard logging format
- **Update**: Real-time selama proses berjalan

## Cara Tahu Apakah Berhasil

### ✅ Indikator Sukses:
1. **Status file** menunjukkan `status: SUCCESS`
2. **Execution time** < 5 menit (dengan parallel processing)
3. **Log file** tidak ada error messages
4. **Rows inserted** > 0

### ❌ Indikator Gagal:
1. **Status file** menunjukkan `status: FAILED`
2. **Execution time** sangat lama (> 15 menit)
3. **Log file** ada error messages
4. **Rows inserted** = 0

## Contoh Output

### Status Check (Sukses):
```
📊 Last Run Status:
  status              : SUCCESS
  execution_time      : 70.05
  rows_inserted       : 12345
```

### Status Check (Gagal):
```
📊 Last Run Status:
  status              : FAILED
  execution_time      : 5.23
  error               : Database connection failed
```

## Tips

1. **Jika proses berjalan lama**: 
   - Buka terminal baru
   - Jalankan `python check_status.py` untuk cek status
   - Atau `python monitor_progress.py` untuk lihat progress

2. **Jika ingin lihat log lengkap**:
   - Buka file di `logs/isolarcloud_etl_*.log`
   - Atau gunakan `tail -f logs/isolarcloud_etl_*.log` (Linux/Mac)

3. **Performance Check**:
   - **Baik**: < 5 menit
   - **Cukup**: 5-10 menit
   - **Buruk**: > 10 menit (mungkin parallel processing tidak aktif)

## Troubleshooting

### Proses Terlalu Lama
1. Check apakah parallel processing aktif:
   ```python
   # Di isolar_config.py
   PARALLEL_PROCESSING_ENABLED = True
   PARALLEL_MAX_WORKERS = 8
   ```

2. Check log untuk error messages
3. Check database connection

### Tidak Ada Progress
1. Check apakah log file terbuat
2. Check apakah database connection OK
3. Check apakah API login berhasil

### Error Messages
- Baca log file untuk detail error
- Check status file untuk error summary
- Pastikan semua environment variables ter-set

