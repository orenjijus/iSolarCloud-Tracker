# Prefect Troubleshooting Guide

## Error: ModuleNotFoundError: No module named 'prefect.task_runners'

### Penyebab
Error ini terjadi karena:
1. Prefect belum terinstall dengan benar
2. Versi Prefect yang terinstall tidak lengkap
3. Virtual environment tidak aktif
4. Konflik antara Prefect 2.x dan 3.x

### Solusi

#### 1. Install/Upgrade Prefect
```powershell
# Di virtual environment
pip install --upgrade "prefect>=3.0.0"
```

Atau install ulang:
```powershell
pip uninstall prefect
pip install "prefect>=3.0.0"
```

#### 2. Verifikasi Instalasi
```powershell
python -c "import prefect; print(prefect.__version__)"
```

#### 3. Cek Virtual Environment
Pastikan virtual environment aktif:
```powershell
# Windows
.venv\Scripts\Activate.ps1

# Verifikasi
python -c "import sys; print(sys.executable)"
```

#### 4. Install Semua Dependencies
```powershell
cd prefect
pip install -r requirements.txt --upgrade
```

#### 5. Test Import
```powershell
python test_import.py
```

### Alternatif: Gunakan Prefect 2.x

Jika Prefect 3.x bermasalah, Anda bisa menggunakan Prefect 2.x:

```powershell
pip install "prefect>=2.0.0,<3.0.0"
```

Tapi perlu update flow code untuk Prefect 2.x syntax.

### Verifikasi Setup Lengkap

Jalankan script verifikasi:
```powershell
python verify_setup.py
```

## Error Lainnya

### Import Error untuk fusionsolar.tasks atau isolarcloud.tasks
- Pastikan project root ada di Python path
- Cek bahwa file `fusionsolar/tasks.py` dan `isolarcloud/tasks.py` ada

### Database Connection Error
- Verifikasi environment variables di `.env` files
- Test connection: `python -c "from fusionsolar_harvester_src.fusionsolar_config import DATABASE_URL; print('OK')"`

### dbt Command Not Found
```powershell
pip install dbt-postgres
```

### Port 4200 Already in Use
```powershell
# Gunakan port lain
python -m prefect server start --port 4201
```

## Getting Help

1. Check Prefect logs
2. Run `python test_import.py` untuk diagnose import issues
3. Check Prefect documentation: https://docs.prefect.io/
4. Verify all environment variables are set correctly
