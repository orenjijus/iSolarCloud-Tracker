# Panduan Scheduling MMSR Daily Pipeline

## Ringkasan

Script sederhana untuk menjalankan pipeline harian tanpa Airflow atau Prefect:
1. **Mengambil tanggal kemarin** (dari Windows system date)
2. **Ingestion paralel** untuk FusionSolar dan iSolarCloud
3. **dbt run** untuk transformasi data

## File yang Dibuat

- `run_daily_pipeline.py` - Script Python utama
- `run_daily_pipeline.bat` - Batch script untuk Windows Task Scheduler
- `SCHEDULING_GUIDE.md` - Panduan ini

## Cara Menggunakan

### 1. Test Manual

Jalankan script secara manual untuk memastikan semuanya bekerja:

```powershell
# Test dengan Python langsung
python run_daily_pipeline.py

# Atau dengan batch file
.\run_daily_pipeline.bat
```

### 2. Setup Windows Task Scheduler

#### A. Buka Task Scheduler
- Tekan `Win + R`, ketik `taskschd.msc`, tekan Enter
- Atau cari "Task Scheduler" di Start Menu

#### B. Create Basic Task
1. Klik **"Create Basic Task"** di panel kanan
2. **Name**: `MMSR Daily Pipeline`
3. **Description**: `Daily data ingestion and transformation pipeline`
4. Klik **Next**

#### C. Set Trigger
1. Pilih **"Daily"**
2. Klik **Next**
3. Set waktu: **01:00:00** (atau waktu yang diinginkan)
4. Set recurrence: **Every 1 days**
5. Klik **Next**

#### D. Set Action
1. Pilih **"Start a program"**
2. Klik **Next**
3. **Program/script**: Browse ke `run_daily_pipeline.bat`
   - Atau ketik: `C:\Users\Administrator\Documents\Code\MMSR API - Server MA\run_daily_pipeline.bat`
4. **Start in**: `C:\Users\Administrator\Documents\Code\MMSR API - Server MA`
5. Klik **Next**

#### E. Finish
1. Centang **"Open the Properties dialog for this task when I click Finish"**
2. Klik **Finish**

#### F. Configure Advanced Settings
Di Properties dialog:

1. **General Tab**:
   - Centang **"Run whether user is logged on or not"**
   - Centang **"Run with highest privileges"**
   - Pilih **"Configure for: Windows 10"**

2. **Actions Tab**:
   - Pastikan action sudah benar
   - Bisa tambahkan action untuk mengirim email jika gagal (opsional)

3. **Conditions Tab**:
   - Uncheck **"Start the task only if the computer is on AC power"** (jika tidak perlu)
   - Uncheck **"Stop if the computer switches to battery power"** (jika tidak perlu)

4. **Settings Tab**:
   - Centang **"Allow task to be run on demand"**
   - Centang **"Run task as soon as possible after a scheduled start is missed"**
   - Centang **"If the task fails, restart every: 10 minutes"**
   - Set **"Attempt to restart up to: 3 times"**
   - Set **"If the running task does not end when requested, force it to stop"**

5. Klik **OK** dan masukkan password jika diminta

### 3. Test Scheduled Task

1. Klik kanan task di Task Scheduler
2. Pilih **"Run"**
3. Cek log di folder `logs\daily_pipeline_*.log`

## Struktur Pipeline

```
┌─────────────────┐
│  Get Yesterday  │
│     Date        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Parallel Run   │
│  ┌───────────┐  │
│  │FusionSolar│  │
│  └───────────┘  │
│  ┌───────────┐  │
│  │iSolarCloud│  │
│  └───────────┘  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    dbt run      │
└─────────────────┘
```

## Logging

Logs disimpan di folder `logs\` dengan format:
- `daily_pipeline_YYYYMMDD_HHMMSS.log`

Setiap log berisi:
- Timestamp untuk setiap step
- Status ingestion (success/failed)
- Jumlah rows yang di-insert
- Execution time
- Error messages jika ada

## Troubleshooting

### Script tidak jalan
1. Pastikan Python ada di PATH
2. Test dengan: `python --version`
3. Pastikan semua dependencies terinstall

### Import error
1. Pastikan virtual environment aktif (jika menggunakan venv)
2. Install dependencies: `pip install -r requirements.txt` (jika ada)

### dbt tidak ditemukan
1. Pastikan dbt terinstall: `dbt --version`
2. Pastikan dbt ada di PATH

### Task Scheduler tidak jalan
1. Cek Task Scheduler service running
2. Cek log di Event Viewer (Windows Logs > Application)
3. Pastikan user account punya permission untuk run task

### Ingestion gagal
1. Cek koneksi database
2. Cek API credentials di config files
3. Cek log file untuk detail error

## Monitoring

### Manual Check
```powershell
# Lihat log terbaru
Get-ChildItem logs\daily_pipeline_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content -Tail 50
```

### Task Scheduler History
1. Buka Task Scheduler
2. Pilih task "MMSR Daily Pipeline"
3. Klik tab "History" di bawah
4. Lihat execution history dan status

## Alternatif: PowerShell Script

Jika ingin lebih kontrol, bisa buat PowerShell script:

```powershell
# run_daily_pipeline.ps1
Set-Location $PSScriptRoot
python run_daily_pipeline.py
```

Lalu di Task Scheduler, set:
- **Program/script**: `powershell.exe`
- **Add arguments**: `-ExecutionPolicy Bypass -File "C:\path\to\run_daily_pipeline.ps1"`

## Catatan

- Script ini **sederhana** dan tidak punya fitur retry/error handling canggih seperti Airflow/Prefect
- Jika butuh fitur lebih (retry, monitoring, alerting), pertimbangkan untuk fix Airflow/Prefect setup
- Untuk production, disarankan menggunakan proper orchestration tool

