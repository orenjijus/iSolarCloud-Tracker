# Prefect Quick Start Guide

## Status Setup

Berdasarkan verifikasi, Prefect sudah siap untuk dijalankan! Berikut langkah-langkahnya:

## ✅ Yang Sudah Selesai

1. ✅ **Dependencies terinstall** - Prefect dan semua package sudah terinstall
2. ✅ **Flow file siap** - `mmsr_daily_pipeline.py` sudah lengkap
3. ✅ **Deploy script siap** - `deploy.py` sudah diperbaiki untuk Prefect 3.x
4. ✅ **Environment variables** - File `.env` sudah ada di `fusionsolar/` dan `isolarcloud/`

## 🚀 Langkah Menjalankan

### Option 1: Test Manual (Recommended untuk pertama kali)

1. **Test flow manual di PowerShell:**
   ```powershell
   cd prefect
   .\run_flow_manual.ps1
   ```
   
   Atau langsung dengan Python:
   ```powershell
   cd prefect
   python flows\mmsr_daily_pipeline.py
   ```
   
   **Note:** Di PowerShell, untuk menjalankan batch file gunakan `.\` prefix:
   ```powershell
   .\run_flow_manual.bat
   ```

### Option 2: Setup Production dengan Prefect Server

1. **Start Prefect Server** (buka Command Prompt baru):
   ```batch
   cd prefect
   start_server.bat
   ```
   
   Server akan berjalan di: http://localhost:4200

2. **Create Work Queue** (jika belum ada):
   ```batch
   python -m prefect work-queue create mmsr-queue --limit 1
   ```

3. **Start Prefect Worker** (buka Command Prompt baru):
   ```batch
   cd prefect
   start_worker.bat
   ```

4. **Deploy Flow:**
   ```batch
   cd prefect
   python deploy.py
   ```

   Flow akan terdeploy dengan schedule: Daily at 01:00 WIB

### Option 3: Setup sebagai Windows Service (Production)

Untuk production yang lebih stabil, gunakan NSSM untuk membuat Windows Service:

1. **Install NSSM** dari https://nssm.cc/download

2. **Create Prefect Server Service:**
   ```batch
   nssm install PrefectServer "C:\Python\python.exe" "-m prefect server start --host 0.0.0.0 --port 4200"
   nssm set PrefectServer AppDirectory "C:\Users\Administrator\Documents\Code\MMSR API - Server MA\prefect"
   nssm set PrefectServer DisplayName "Prefect Server"
   nssm set PrefectServer Start SERVICE_AUTO_START
   nssm start PrefectServer
   ```

3. **Create Prefect Worker Service:**
   ```batch
   nssm install PrefectWorker "C:\Python\python.exe" "-m prefect worker start --pool mmsr-queue"
   nssm set PrefectWorker AppDirectory "C:\Users\Administrator\Documents\Code\MMSR API - Server MA\prefect"
   nssm set PrefectWorker DisplayName "Prefect Worker"
   nssm set PrefectWorker Start SERVICE_AUTO_START
   nssm start PrefectWorker
   ```

4. **Deploy Flow:**
   ```batch
   cd prefect
   python deploy.py
   ```

## 📋 Checklist Sebelum Menjalankan

- [x] Prefect terinstall
- [x] Dependencies terinstall (psycopg2, pendulum, dll)
- [x] Flow file siap
- [x] Environment variables sudah diset (di .env files)
- [ ] dbt terinstall (cek dengan: `dbt --version`)
- [ ] Database connection bisa diakses
- [ ] API credentials valid

## 🔍 Verifikasi

Jalankan script verifikasi:
```batch
cd prefect
python verify_setup.py
```

## 📊 Monitoring

Setelah server running, akses Prefect UI di:
- **Local:** http://localhost:4200
- **Network:** http://your-server-ip:4200

Di UI Anda bisa:
- Melihat flow runs
- Check task status
- View logs
- Monitor execution times

## 🐛 Troubleshooting

### Port 4200 sudah digunakan
```batch
# Gunakan port lain
python -m prefect server start --port 4201
```

### Worker tidak mengambil tasks
```batch
# Cek work queue
python -m prefect work-queue ls

# Pastikan worker connect ke queue yang benar
python -m prefect worker start --pool mmsr-queue
```

### Import errors
Pastikan project root ada di Python path. Flow file sudah handle ini dengan:
```python
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
```

### Database connection errors
- Verify environment variables di `.env` files
- Test connection: `python -c "from fusionsolar_harvester_src.fusionsolar_config import DATABASE_URL; print('OK')"`

## 📝 Catatan Penting

1. **Schedule:** Flow dijadwalkan untuk run setiap hari jam 01:00 WIB (Asia/Jakarta)
2. **Date Window:** Flow akan memproses data untuk hari kemarin (yesterday)
3. **Parallel Execution:** FusionSolar dan iSolarCloud ingestion berjalan parallel
4. **Retry Logic:** Ingestion tasks punya 2 retries dengan 15 menit delay
5. **Logs:** Status completion di-log ke `logs/mmsr_pipeline_status_*.txt`

## 🎯 Next Steps

1. ✅ Test flow manual untuk memastikan semua berfungsi
2. ✅ Setup Prefect server dan worker
3. ✅ Deploy flow dengan schedule
4. ✅ Monitor first few runs di Prefect UI
5. ✅ Setup alerts (opsional)

## 📚 Dokumentasi

- Setup detail: `SETUP_WINDOWS.md`
- Implementation summary: `IMPLEMENTATION_SUMMARY.md`
- Testing guide: `TESTING_GUIDE.md`
- Prefect docs: https://docs.prefect.io/

