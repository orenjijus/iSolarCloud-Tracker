# Tools — Form & utilitas internal

## Satu Form Streamlit: Cleaning Log + Weekly Log

Satu aplikasi Streamlit dengan **dua tab**:
- **Tab Cleaning Log** — input log pembersihan sensor/module → **staging.cleaning_log**
- **Tab Weekly Log** — input log masalah & tindakan korektif → **staging.seed_weekly_log** + evaluasi (Seed/Mart: days_open, is_open)

**Jalankan:** `streamlit run tools/cleaning_log_form.py` (atau `tools\run_cleaning_log_form.bat`).  
`tools\run_weekly_log_form.bat` juga membuka form yang sama (port 8503).

---

## Cleaning Log (tab pertama)

**Site pakai dropdown** (pilih dari daftar, jangan ketik) supaya tidak salah input. Data langsung INSERT ke **`staging.cleaning_log`**. Tabel dibuat dengan `scripts/create_staging_cleaning_log_table.sql` (atau via MCP Postgres). Cocok untuk tim engineering.

---

## Agar tim engineering lain bisa pakai

Ada **dua cara**:

### Opsi A: Setiap orang menjalankan di komputernya sendiri

1. **Clone/copy project** (atau akses folder project yang sama di network share).
2. **Persiapan** (sekali saja):
   - Python 3.8+ terpasang.
   - Install dependency: `pip install streamlit sqlalchemy psycopg2-binary python-dotenv pandas`
   - File **`.env`** di **root project** atau di folder **`tools/`** berisi koneksi DB:
     ```
     POSTGRES_HOST=...
     POSTGRES_PORT=5432
     POSTGRES_DB=MMSR
     POSTGRES_USER=...
     POSTGRES_PASSWORD=...
     ```
   - Pastikan komputer bisa **akses database** (jaringan/VPN ke server DB).
3. **Jalankan form**:
   - **Windows**: Double-click `tools\run_cleaning_log_form.bat` atau dari CMD: `tools\run_cleaning_log_form.bat`
   - **Linux/Mac**: `./tools/run_cleaning_log_form.sh` atau `bash tools/run_cleaning_log_form.sh`
   - Atau manual: dari root project jalankan `streamlit run tools/cleaning_log_form.py`
4. Buka browser di **http://localhost:8501** → isi form → Simpan.

### Opsi B: Satu komputer/server dipakai bersama (tim akses lewat browser)

Satu orang menjalankan form di **satu komputer** (atau server internal), yang lain cukup **buka browser** ke alamat komputer itu.

1. Di **komputer yang dipakai sebagai “server”**:
   - Selesaikan **Persiapan** seperti Opsi A (Python, dependency, `.env`, akses DB).
   - Jalankan form **shared** (bisa diakses dari LAN):
     - **Windows**: Double-click `tools\run_cleaning_log_form_shared.bat`
     - **Linux/Mac**: `./tools/run_cleaning_log_form_shared.sh`
     - Atau manual: `streamlit run tools/cleaning_log_form.py --server.port 8501 --server.address 0.0.0.0`
2. Cek **IP komputer server** (Windows: `ipconfig`, Linux/Mac: `ip addr` atau `ifconfig`).
3. **Tim lain**: Buka browser ke **http://IP_SERVER:8502** (shared pakai port 8502; contoh: `http://10.101.4.100:8502`).
4. **Firewall**: Pastikan port **8502** tidak diblok (Windows: izinkan di Firewall; Linux: `ufw allow 8502` jika pakai ufw).

**Ringkas untuk tim:**  
- **Lokal**: Jalankan `run_cleaning_log_form.bat` (Windows) atau `run_cleaning_log_form.sh` (Linux/Mac), lalu buka http://localhost:8501.  
- **Bersama**: Satu orang jalankan `run_cleaning_log_form_shared.bat` / `run_cleaning_log_form_shared.sh`, yang lain buka http://IP_SERVER:**8502** (port 8502 agar tidak bentrok dengan 8501).

---

## Deploy di server (jalan 24 jam, device lain akses)

Jika form dijalankan di **satu device server** dan ingin **jalan 24 jam** agar device lain (PC/laptop/HP) bisa akses kapan saja:

### 1. Di server (Windows)

1. **Persiapan** (sekali): Python, dependency, `.env` di root atau `tools/` (koneksi DB).
2. **Jalankan form shared**  
   Double-click **`tools\run_cleaning_log_form_shared.bat`** atau dari CMD:
   ```bat
   cd \path\ke\project
   tools\run_cleaning_log_form_shared.bat
   ```
3. **Jangan tutup jendela CMD** — biarkan terbuka (boleh di-minimize). Selama jendela ini tetap jalan, form bisa diakses 24 jam.
4. **Cek IP server**: di CMD jalankan `ipconfig`, catat **IPv4 Address** (mis. `10.101.4.88`).

### 2. Firewall (di server)

Agar device lain bisa konek ke port **8502** (shared pakai 8502 agar tidak bentrok dengan 8501):

- **Windows**:  
  - Windows Defender Firewall → Advanced settings → Inbound Rules → New Rule → Port → TCP **8502** → Allow.  
  - Atau (PowerShell as Admin):  
    `New-NetFirewallRule -DisplayName "Streamlit Cleaning Log" -Direction Inbound -Protocol TCP -LocalPort 8502 -Action Allow`

### 3. Dari device lain (tim)

1. Pastikan device **satu jaringan** dengan server (LAN/VPN yang sama).
2. Buka browser (Chrome, Edge, dll.).
3. Masuk ke: **`http://IP_SERVER:8502`**  
   Contoh: `http://10.101.4.88:8502`  
   Jika server punya hostname: `http://nama-server:8502`
4. Isi form → Simpan.

### 4. Opsional: jalan otomatis setelah server restart

- **Windows**:  
  - Taruh shortcut `run_cleaning_log_form_shared.bat` di folder **Startup** (Win+R → `shell:startup`), atau  
  - Buat **Scheduled Task** yang trigger "At startup" / "At log on", action: jalankan `run_cleaning_log_form_shared.bat`.

### Ringkas

| Di server | Di device lain |
|-----------|-----------------|
| Jalankan `tools\run_cleaning_log_form_shared.bat` (port **8502**) | Buka browser **http://IP_SERVER:8502** |
| Biarkan jendela CMD terbuka (24 jam) | Satu jaringan (LAN/VPN) dengan server |
| Buka port **8502** di firewall | Isi form → Simpan |

**Catatan:** Shared pakai port **8502** agar tidak bentrok dengan form yang jalan lokal di 8501. Jika muncul "Port 8501 is not available", tutup aplikasi yang pakai 8501 atau pakai script shared (8502). **Jangan buka http://0.0.0.0:8502** — itu invalid; gunakan **IP asli server** (cek dengan `ipconfig`) atau **http://localhost:8502** jika akses dari server itu sendiri.

---

### Persiapan (dependency & .env)

1. Python 3.8+
2. Install dependency:
   ```bash
   pip install streamlit sqlalchemy psycopg2-binary python-dotenv
   ```
3. File `.env` di **root project** atau di **tools/** berisi:
   ```
   POSTGRES_HOST=...
   POSTGRES_PORT=5432
   POSTGRES_DB=MMSR
   POSTGRES_USER=...
   POSTGRES_PASSWORD=...
   ```
   (Bisa pakai .env yang sama dengan harvesters. Untuk Opsi B, cukup satu .env di komputer server.)

### Jalankan (manual)

Dari **root project**:

```bash
streamlit run tools/cleaning_log_form.py
```

Browser akan terbuka di `http://localhost:8501`. Isi form → Simpan.

### Jika "Tidak ada daftar site"

- Pastikan `.env` berisi `POSTGRES_*` dan komputer bisa konek ke DB.
- Daftar site diambil dari **dim_site** (prioritas), lalu fallback: `dim_assets` → `cleaning_log` → `seed_site_config`. Pastikan minimal salah satu tabel ada.
- Jika pakai **dim_site**: pastikan tabel `dimensions.dim_site` ada (kolom `site_code` & `site_name`, atau `site_id` & `site_name`).
- Klik "🔄 Refresh daftar site" setelah DB siap.

### Opsi lain

- **Tanpa form**: Input langsung lewat SQL — lihat `scripts/insert_cleaning_log.sql` dan `docs/CLEANING_LOG.md` Section 6.

---

## Weekly Log (tab kedua di form yang sama)

Di aplikasi yang sama, pilih tab **📋 Weekly Log**. Input log masalah & tindakan korektif → **staging.seed_weekly_log**. Site dari dropdown **dim_site**. Mart (**days_open**, **is_open**) di-build dengan `dbt run --select mart_weekly_log`.

**Alur & evaluasi:** docs/weekly-log/WEEKLY_LOG_INPUT_OUTPUT_FLOW.md. Di dalam tab Weekly Log ada sub-tab **Evaluasi**: "Data terakhir di Seed (input)" dan "Data terakhir di Mart (output)" untuk cek data dan prioritas.
