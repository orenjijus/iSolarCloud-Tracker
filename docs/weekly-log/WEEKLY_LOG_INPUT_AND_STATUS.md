# Weekly Log: Input & Status

## Input yang Diisi (Seed)

Yang **Anda input** di seed (CSV atau form) hanya:

| Kolom | Wajib | Contoh | Keterangan |
|-------|--------|--------|------------|
| **log_date** | Ya | 2025-12-09 | Tanggal kejadian/log (tanggal saja, format YYYY-MM-DD) |
| **site_id** | Ya | ISO_SITE_1637095 | ID site (sama dengan di dim_assets) |
| **site_name** | Ya | CP Majalengka | Nama site untuk tampilan |
| **problem_identification** | Ya | MV ACB Trip / 1. Overcurrent... | Deskripsi masalah (bisa multi-line) |
| **corrective_action** | Tidak | Adjust ACB Settings | Tindakan korektif (boleh kosong) |
| **status** | Ya | Open / Resolved | Status masalah (lihat bawah) |
| created_at, updated_at | Tidak | 2025-12-14 10:00:00 | Opsional; bisa dikosongkan |

**Week tidak di-input.** Week (week_of_month), month, year digenerate di mart dari `log_date` dengan algoritma yang sama seperti `mart_site_performance_daily` (pakai `dim_date_generated`).

---

## Status: Input Manual

Status **Anda yang input** (manual). Sistem tidak mengubah status otomatis.

### Nilai yang Dipakai

| Nilai | Arti | Kapan dipakai |
|-------|------|----------------|
| **Open** | Masalah belum selesai | Saat pertama kali input log (default untuk masalah baru) |
| **In Progress** | Sedang ditangani | Opsional; saat tim sedang mengerjakan |
| **Resolved** | Sudah selesai ditangani | Saat masalah sudah diperbaiki / tindakan sudah dilakukan |
| **Closed** | Ditutup (tanpa tindakan / cancel) | Opsional; untuk kasus yang tidak jadi ditindak |

### Cara Input Status

1. **Saat buat log baru**  
   Isi status **Open** (atau **In Progress** jika memang sudah mulai ditangani).

2. **Saat masalah sudah selesai**  
   **Update** baris yang sama: ubah status jadi **Resolved** (atau **Closed** jika tidak jadi ditindak).

3. **Di mana mengubah**  
   - **Jika pakai CSV**: edit `dbt/seeds/seed_weekly_log.csv`, ubah kolom `status` di baris yang bersangkutan, lalu jalankan `dbt seed --select seed_weekly_log` dan `dbt run --select mart_weekly_log`.  
   - **Jika pakai form/aplikasi**: ubah status di form lalu simpan (sistem yang nanti update seed/DB).

Jadi: **kita yang input** status; **Open** = belum selesai, **Resolved** = sudah selesai. Tidak ada otomatisasi; setiap kali update status, data seed/DB harus di-refresh agar Power BI ikut update.

---

## Contoh CSV (Seed)

```csv
log_date,site_id,site_name,problem_identification,corrective_action,status,created_at,updated_at
2025-12-09,ISO_SITE_1637095,CP Majalengka,MV ACB Trip,Adjust ACB Settings,Open,2025-12-14 10:00:00,2025-12-14 10:00:00
2025-12-09,ISO_SITE_1637095,Charoen Pokphand,Cleaning not scheduled yet,,Open,2025-12-14 10:00:00,2025-12-14 10:00:00
2025-12-09,ISO_SITE_1637816,SPF,Grid Overvoltage and Undervoltage,Evaluate action done in october,Resolved,2025-12-14 10:00:00,2025-12-14 10:00:00
```

Setelah masalah baris pertama selesai, edit jadi:

```csv
...,Adjust ACB Settings,Resolved,...
```

Lalu jalankan lagi `dbt seed` dan `dbt run` agar mart dan Power BI ter-update.

---

## Ringkasan

- **Input penting**: `log_date` (tanggal saja), **site**, **problem_identification**, **corrective_action**, **status**.
- **Week** tidak di-input; week/month/year **digenerate di mart** dari `log_date` (algoritma sama dengan mart lain).
- **Status** = **input manual**: kita yang pilih Open / In Progress / Resolved / Closed; saat buat log pakai Open, saat selesai update jadi Resolved (atau Closed).
