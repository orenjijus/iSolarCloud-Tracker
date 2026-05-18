# Alur Input → Output Weekly Log & Evaluasi

Dokumen ini menjelaskan alur lengkap dari **input** (form atau CSV) sampai **output** (mart + Power BI) dan cara **memverifikasi** bahwa data sudah benar serta input/output sesuai tujuan.

---

## 1. Ringkas Alur

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  INPUT                                                                       │
│  • Form (Streamlit): tools/weekly_log_form.py  →  INSERT                      │
│  • Atau CSV: dbt/seeds/seed_weekly_log.csv   →  dbt seed                     │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGING: staging.seed_weekly_log                                            │
│  Kolom: log_date, site_id, site_name, problem_identification,                 │
│         corrective_action, status, created_at, updated_at                     │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  BUILD MART: dbt run --select mart_weekly_log                                 │
│  Mart membaca seed_weekly_log + join dim_date_generated                      │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  OUTPUT: mart.mart_weekly_log                                                 │
│  Kolom input + date_key, year, month, week_of_month, day_name, dll.          │
│  + days_open (berapa hari sejak issue di-log)                                 │
│  + is_open (1 = Open/In Progress, 0 = Resolved/Closed)                        │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  KONSUMSI                                                                     │
│  • Power BI: import mart.mart_weekly_log → filter is_open, sort days_open     │
│  • SQL: query mart untuk prioritas & report                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Input

### 2.1 Via Form (disarankan untuk tim)

1. Jalankan: `streamlit run tools/cleaning_log_form.py` (satu form dengan dua tab: Cleaning Log + Weekly Log). Atau `tools\run_cleaning_log_form.bat` (8501) / `tools\run_weekly_log_form.bat` (8503).
2. Pilih tab **📋 Weekly Log**, lalu pilih **Site** dari dropdown (dari `dim_site` — site_id & site_name ikut otomatis).
3. Isi **Tanggal log**, **Problem identification**, **Corrective action**, **Status** (Open / In Progress / Resolved / Closed).
4. Klik **Simpan** → data masuk ke **staging.seed_weekly_log**.

### 2.2 Via CSV (dbt seed)

1. Edit **dbt/seeds/seed_weekly_log.csv** (format: log_date, site_id, site_name, problem_identification, corrective_action, status).
2. Jalankan: `dbt seed --select seed_weekly_log` → data masuk ke tabel seed (staging).

---

## 3. Staging: staging.seed_weekly_log

- **Sumber**: Form INSERT atau dbt seed dari CSV.
- **Kolom**: log_date, site_id, site_name, problem_identification, corrective_action, status, created_at, updated_at.
- **Tujuan**: Menyimpan **input mentah**; satu baris per issue per hari (boleh lebih dari satu issue per site per hari).

---

## 4. Build Mart

Setelah ada data di seed, mart harus di-build agar output punya **days_open** dan **is_open**:

```bash
cd dbt
dbt run --select mart_weekly_log
```

- Mart membaca **seed_weekly_log** dan join ke **dim_date_generated**.
- Menambah kolom: **days_open** = (CURRENT_DATE - log_date), **is_open** = 1 jika status Open/In Progress.

---

## 5. Output: mart.mart_weekly_log

- **Kolom dari input**: log_date, site_id, site_name, problem_identification, corrective_action, status, created_at, updated_at.
- **Kolom tambahan (evaluasi/prioritas)**:
  - **days_open**: Berapa hari sejak issue di-log → untuk **prioritas** (issue lama = prioritas tinggi).
  - **is_open**: 1 = masih Open/In Progress, 0 = Resolved/Closed → untuk **filter** prioritas.
- **Kolom dari dim_date_generated**: date_key, year, month, month_name, week_of_month, day_of_week, day_name, day_type.

---

## 6. Evaluasi: Apakah Data Sudah Benar?

### 6.1 Cek di Form (tab Evaluasi / Verifikasi)

Form menyediakan:

- **Data terakhir di Seed (input)**  
  Menampilkan baris terakhir di **staging.seed_weekly_log**. Pastikan: site, tanggal, problem, corrective action, status sesuai yang di-input.

- **Data terakhir di Mart (output)**  
  Menampilkan baris terakhir di **mart.mart_weekly_log** termasuk **days_open** dan **is_open**. Pastikan:
  - Nilai **days_open** = hari dari log_date sampai hari ini.
  - **is_open** = 1 untuk status Open/In Progress, 0 untuk Resolved/Closed.

Jika Mart kosong atau error: jalankan dulu `dbt run --select mart_weekly_log`, lalu refresh/klik lagi verifikasi di form.

### 6.2 Cek via SQL

**Seed (input):**

```sql
SELECT log_date, site_id, site_name, problem_identification, corrective_action, status
FROM staging.seed_weekly_log
ORDER BY log_date DESC, created_at DESC
LIMIT 20;
```

**Mart (output + prioritas):**

```sql
SELECT log_date, site_name, problem_identification, status, days_open, is_open
FROM mart.mart_weekly_log
ORDER BY log_date DESC, site_name
LIMIT 20;
```

Pastikan: **days_open** wajar (≥ 0), **is_open** sesuai status.

### 6.3 Cek di Power BI

- Import **mart.mart_weekly_log**.
- Filter **is_open = 1** → hanya issue yang masih open.
- Sort by **days_open** (Descending) → issue paling lama di atas (prioritas).
- Bandingkan dengan data yang di-input di form/CSV.

---

## 7. Ringkas Checklist

| Langkah | Yang dicek |
|--------|-------------|
| Input | Form/CSV mengisi log_date, site, problem, corrective_action, status dengan benar. |
| Seed | Data muncul di staging.seed_weekly_log sesuai input. |
| Mart | Setelah `dbt run --select mart_weekly_log`, data muncul di mart.mart_weekly_log. |
| Output | Kolom days_open dan is_open ada dan nilainya benar (prioritas & filter). |
| Tujuan | Di Power BI/report: filter is_open, sort days_open → prioritas decision sesuai kebutuhan. |

Dengan alur ini Anda bisa **mencoba input** (form atau CSV), **memastikan data benar** di seed dan mart, dan **memastikan output** (days_open, is_open) sesuai tujuan evaluasi dan prioritas.
