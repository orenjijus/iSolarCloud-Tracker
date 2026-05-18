# Prefect Setup Guide untuk Windows Server

Panduan lengkap untuk setup Prefect di Windows Server untuk menjalankan MMSR daily pipeline.

## Prerequisites

1. **Python 3.8+** - Download dari https://www.python.org/downloads/
   - Pastikan "Add Python to PATH" dicentang saat install
   - Verifikasi: `python --version`

2. **PostgreSQL Client** (opsional, jika menggunakan database terpisah untuk Prefect metadata)
   - Atau gunakan SQLite untuk development (default)

3. **dbt** - Install dbt untuk transformasi data
   ```bash
   pip install dbt-postgres
   ```

4. **Akses ke Database MMSR** - Pastikan server bisa connect ke database MMSR

## Quick Start

### 1. Install Dependencies

Jalankan script setup:

```batch
cd prefect
setup_windows.bat
```

Atau manual:

```batch
cd prefect
pip install -r requirements.txt
```

### 2. Set Environment Variables

Buat file `.env` di root project atau set environment variables di Windows:

**Via System Properties:**
1. Right-click "This PC" → Properties
2. Advanced System Settings → Environment Variables
3. Add variables di System Variables atau User Variables

**Via Command Prompt (temporary):**
```batch
set POSTGRES_HOST=10.101.4.88
set POSTGRES_PORT=5432
set POSTGRES_DB=MMSR
set POSTGRES_USER=juice
set POSTGRES_PASSWORD=your_password
set FUSIONSOLAR_USERNAME=your_username
set FUSIONSOLAR_PASSWORD=your_password
set ISOLARCLOUD_APP_KEY=your_app_key
set ISOLARCLOUD_SECRET_KEY=your_secret_key
set ISOLARCLOUD_USERNAME=your_username
set ISOLARCLOUD_PASSWORD=your_password
```

**Via PowerShell (persistent untuk session):**
```powershell
[System.Environment]::SetEnvironmentVariable('POSTGRES_HOST', '10.101.4.88', 'Machine')
# ... set lainnya
```

### 3. Test Installation

```batch
cd prefect
python -c "import prefect; print(prefect.__version__)"
```

### 4. Run Flow Manual (Testing)

```batch
cd prefect
run_flow_manual.bat
```

## Production Setup

### Option 1: Prefect Server + Worker (Recommended)

#### Step 1: Start Prefect Server

Jalankan di Command Prompt atau PowerShell:

```batch
cd prefect
start_server.bat
```

Atau manual:
```batch
python -m prefect server start --host 0.0.0.0 --port 4200
```

Server akan berjalan di: http://localhost:4200

#### Step 2: Create Work Queue

```batch
python -m prefect work-queue create mmsr-queue --limit 1
```

#### Step 3: Start Worker

Buka Command Prompt baru:

```batch
cd prefect
start_worker.bat
```

Atau manual:
```batch
python -m prefect worker start --pool mmsr-queue
```

#### Step 4: Deploy Flow

```batch
cd prefect
python deploy.py
```

Flow akan terdeploy dan siap dijalankan sesuai schedule (01:00 WIB daily).

### Option 2: Windows Service (Most Reliable)

Untuk production yang lebih stabil, setup sebagai Windows Service menggunakan NSSM.

#### Install NSSM

1. Download NSSM dari: https://nssm.cc/download
2. Extract ke folder (misalnya `C:\nssm`)
3. Add ke PATH atau gunakan full path

#### Create Prefect Server Service

```batch
nssm install PrefectServer "C:\Python\python.exe" "-m prefect server start --host 0.0.0.0 --port 4200"
nssm set PrefectServer AppDirectory "C:\path\to\MMSR-API-Server-MA\prefect"
nssm set PrefectServer DisplayName "Prefect Server"
nssm set PrefectServer Description "Prefect orchestration server for MMSR pipeline"
nssm set PrefectServer Start SERVICE_AUTO_START
nssm set PrefectServer AppStdout "C:\path\to\MMSR-API-Server-MA\prefect\logs\server.log"
nssm set PrefectServer AppStderr "C:\path\to\MMSR-API-Server-MA\prefect\logs\server_error.log"
nssm start PrefectServer
```

#### Create Prefect Worker Service

```batch
nssm install PrefectWorker "C:\Python\python.exe" "-m prefect worker start --pool mmsr-queue"
nssm set PrefectWorker AppDirectory "C:\path\to\MMSR-API-Server-MA\prefect"
nssm set PrefectWorker DisplayName "Prefect Worker"
nssm set PrefectWorker Description "Prefect worker for MMSR pipeline"
nssm set PrefectWorker Start SERVICE_AUTO_START
nssm set PrefectWorker AppStdout "C:\path\to\MMSR-API-Server-MA\prefect\logs\worker.log"
nssm set PrefectWorker AppStderr "C:\path\to\MMSR-API-Server-MA\prefect\logs\worker_error.log"
nssm start PrefectWorker
```

**Note:** Ganti `C:\Python\python.exe` dengan path Python Anda.

#### Manage Services

```batch
# Start services
nssm start PrefectServer
nssm start PrefectWorker

# Stop services
nssm stop PrefectServer
nssm stop PrefectWorker

# Restart services
nssm restart PrefectServer
nssm restart PrefectWorker

# Remove services
nssm remove PrefectServer confirm
nssm remove PrefectWorker confirm
```

### Option 3: Windows Task Scheduler

Alternatif tanpa NSSM, menggunakan Windows Task Scheduler.

#### Create Task for Prefect Server

1. Open Task Scheduler
2. Create Basic Task
3. Name: "Prefect Server"
4. Trigger: "When the computer starts"
5. Action: "Start a program"
   - Program: `C:\Python\python.exe`
   - Arguments: `-m prefect server start --host 0.0.0.0 --port 4200`
   - Start in: `C:\path\to\MMSR-API-Server-MA\prefect`
6. Check "Run whether user is logged on or not"
7. Check "Run with highest privileges"

#### Create Task for Prefect Worker

1. Create Basic Task
2. Name: "Prefect Worker"
3. Trigger: "When the computer starts"
4. Action: "Start a program"
   - Program: `C:\Python\python.exe`
   - Arguments: `-m prefect worker start --pool mmsr-queue`
   - Start in: `C:\path\to\MMSR-API-Server-MA\prefect`
5. Check "Run whether user is logged on or not"
6. Check "Run with highest privileges"

## Monitoring

### Prefect UI

Akses Prefect UI di:
- Local: http://localhost:4200
- Network: http://your-server-ip:4200

Features:
- View flow runs
- Check task status
- View logs
- Monitor execution times

### Status Logs

Pipeline completion status logged ke:
- `logs/mmsr_pipeline_status_YYYYMMDD_HHMMSS.txt`

### Windows Event Viewer

Jika menggunakan Windows Service, check logs di:
- Event Viewer → Windows Logs → Application
- Filter by source: PrefectServer atau PrefectWorker

## Troubleshooting

### Common Issues

1. **Python not found**
   - Pastikan Python di PATH
   - Restart Command Prompt setelah install Python

2. **Import errors**
   - Pastikan project root di Python path
   - Check bahwa semua dependencies terinstall

3. **Database connection errors**
   - Verify environment variables
   - Test connection: `python -c "from fusionsolar_harvester_src.fusionsolar_config import DATABASE_URL; print(DATABASE_URL)"`

4. **dbt command not found**
   - Install dbt: `pip install dbt-postgres`
   - Verify: `dbt --version`

5. **Port 4200 already in use**
   - Change port: `prefect server start --port 4201`
   - Or stop existing service

6. **Worker not picking up tasks**
   - Verify work queue exists: `prefect work-queue ls`
   - Check worker is connected: Check Prefect UI → Work Queues

### Debugging

1. **Test flow manually:**
   ```batch
   cd prefect
   python flows\mmsr_daily_pipeline.py
   ```

2. **Check Prefect logs:**
   - Server logs: Check console output atau log files
   - Worker logs: Check console output atau log files

3. **Verify environment:**
   ```batch
   python -c "import os; print(os.environ.get('POSTGRES_HOST'))"
   ```

## Maintenance

### Update Prefect

```batch
pip install --upgrade prefect
```

### Update Dependencies

```batch
cd prefect
pip install -r requirements.txt --upgrade
```

### Backup Configuration

Backup file penting:
- `prefect/flows/mmsr_daily_pipeline.py`
- Environment variables
- Service configurations (NSSM)

## Next Steps

Setelah setup berhasil:

1. ✅ Test flow manual
2. ✅ Deploy flow dengan schedule
3. ✅ Monitor first few runs
4. ✅ Setup alerts (opsional)
5. ✅ Document any customizations

## Support

Untuk bantuan:
- Prefect docs: https://docs.prefect.io/
- Check logs di `logs/` directory
- Check Prefect UI untuk flow run details

