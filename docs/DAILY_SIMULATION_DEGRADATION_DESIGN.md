# Desain Daily Simulation dengan Degradasi (25 Tahun)

Dokumen ini mendefinisikan **algoritma** dan **skema tabel** untuk menghasilkan nilai daily simulation untuk semua site dengan penerapan faktor degradasi per tahun (1–25). Hanya dokumentasi; tidak ada implementasi kode atau perubahan database di sini.

**Laporan cakupan site (sudah/belum ter-generate dan data yang dibutuhkan):** [SIMULATION_25Y_SITE_COVERAGE_REPORT.md](SIMULATION_25Y_SITE_COVERAGE_REPORT.md)

---

## 1. Konteks dan Sumber Data yang Ada

### 1.1 Data yang Sudah Ada

| Sumber | Isi | Format |
|--------|-----|--------|
| **seed_daily_simulation_target** | Daily simulation **tahun pertama saja** (1 tahun × N site) | Site_Code, Date (DD/MM/YYYY), Energy Simulation (MW), Energy Target (MW), GHI, POA, PR, dll. Satuan energy di CSV: MW (nilai harian, mis. 0,312 ≈ 0.312 MWh). |
| **seed_yearly_simulation_target** | Energy tahunan **25 tahun** per site | Site_Code, Year (1–25), "Energy TS (MWh)", "Energy Sim Target (MWh)". Desimal pakai koma (e.g. 930,604 = 930.604 MWh). |

### 1.2 Contoh Numerik (Garuda Metalindo 1)

- **Tahun 1**: 930,60 MWh  
- **Tahun 2**: 927,54 MWh  
- **Tahun 3**: 923,98 MWh  
- …  
- **Tahun 25**: 802,58 MWh  

Daily tahun 1 contoh: **1 Januari = 0,312 MWh** (dari seed daily).

---

## 2. Rumus Faktor Degradasi

Faktor degradasi untuk **simulation year** \(n\) (1–25) didefinisikan sebagai rasio energy tahun \(n\) terhadap energy tahun 1:

\[
\text{degradation\_factor}(n) = \frac{\text{Energy\_Year}_n}{\text{Energy\_Year}_1}
\]

Contoh (Garuda Metalindo 1):

- Tahun 2: \(927{,}54 / 930{,}60 = 0{,}99670644\)
- Tahun 3: \(923{,}98 / 930{,}60 = 0{,}992883117\)

Faktor ini **per site** dan **per simulation year** (1–25), karena setiap site punya kurva degradasi sendiri (energy tahunan berbeda).

---

## 3. Algoritma: Daily Simulation 25 Tahun

### 3.1 Prinsip

- **Profil harian tahun 1** sudah ada di `seed_daily_simulation_target` (satu tahun kalender, 365 hari, per site).
- Untuk **tahun simulasi** \(n = 2 \ldots 25\), nilai energy harian = nilai tahun 1 untuk **hari yang sama dalam setahun** dikalikan faktor degradasi tahun \(n\).

### 3.2 Rumus

Untuk site \(s\), simulation year \(n\), dan **day-of-year** \(d\) (1–365, atau 1–366 jika ada kabisat):

\[
\text{energy\_daily}(s,\, n,\, d) = \text{energy\_daily\_year1}(s,\, d) \times \text{degradation\_factor}(s,\, n)
\]

Contoh:

- 1 Januari tahun 1: 0,312 MWh  
- 1 Januari tahun 2: \(0{,}312 \times 0{,}99670644 \approx 0{,}310\) MWh  
- 1 Januari tahun 3: \(0{,}312 \times 0{,}992883117 \approx 0{,}310\) MWh (nilai turun lagi)

### 3.3 Langkah Algoritma (Pseudocode)

1. **Hitung faktor degradasi per site per tahun**
   - Baca `seed_yearly_simulation_target`.
   - Untuk setiap (site_code, year 1..25):  
     `degradation_factor(site_code, year) = energy_year_n / energy_year_1`  
   - Simpan ke struktur lookup: (site_code, simulation_year) → faktor.

2. **Buat profil harian tahun 1 per site**
   - Baca `seed_daily_simulation_target`.
   - Untuk setiap site: parse Date → **day_of_year** (1–365 atau 1–366).
   - Simpan: (site_code, day_of_year) → energy_simulation_mwh (dan kolom lain yang akan didegradasi jika ada, mis. energy_target_mwh).

3. **Generate daily simulation 25 tahun**
   - Untuk setiap site_code:
     - Untuk simulation_year = 1 .. 25:
       - Untuk day_of_year = 1 .. 365 (atau 366):
         - energy_daily = energy_daily_year1(site_code, day_of_year) × degradation_factor(site_code, simulation_year)
         - (Opsi) energy_target_daily = energy_target_year1(...) × degradation_factor(site_code, simulation_year)
         - Tulis satu baris ke output (sesuai skema yang dipilih di bawah).

4. **GHI dan POA**  
   Dianggap **sama untuk seluruh 25 tahun** (tidak terdegradasi). Untuk tahun 2–25: **disalin dari tahun 1** (hari yang sama dalam setahun). Sumber: profil harian tahun 1 di seed daily.

5. **Performance Ratio (PR)**  
   **Tidak** disalin dari tahun 1 untuk tahun 2–25. Karena energy berkurang sementara GHI/POA tetap, PR harus **dihitung** agar konsisten:

   - Rumus PR (referensi):  
     `PR_GHI = daily_energy_mwh / (daily_ghi_kwh_m2 / 1000) / site_capacity_mw`  
     `PR_POA = daily_energy_mwh / (daily_poa_kwh_m2 / 1000) / site_capacity_mw`

   - Karena GHI dan POA tahun \(n\) = GHI dan POA tahun 1 (disalin), dan energy tahun \(n\) = energy tahun 1 × degradation_factor(\(n\)):

   \[
   \text{PR}_{n} = \frac{\text{energy}_n}{\text{irradiance} \times \text{capacity}} = \frac{\text{energy}_1 \times \text{degradation\_factor}(n)}{\text{irradiance}_1 \times \text{capacity}} = \text{PR}_1 \times \text{degradation\_factor}(n)
   \]

   Jadi untuk tahun simulasi \(n\) (2–25):

   \[
   \text{PR\_daily}(s,\, n,\, d) = \text{PR\_daily\_year1}(s,\, d) \times \text{degradation\_factor}(s,\, n)
   \]

   Berlaku untuk **PR GHI** dan **PR POA** (simulation dan target). Ringkasnya: **energy dan PR keduanya dikalikan faktor degradasi**; **GHI dan POA disalin dari tahun 1**.

---

## 4. Penanganan Tahun Kabisat

- **Profil tahun 1**: Jika seed daily hanya 365 hari, **day_of_year** 1–365. Jika ada 29 Februari, day_of_year = 60 (atau 61 tergantung konvensi).
- **Tahun 2–25**: Jika menggunakan **day_of_year** saja (1–365), maka setiap “tahun” punya 365 slot; tanggal 29 Februari tidak ada slot khusus. Alternatif:
  - **Opsi 1**: Selalu 365 hari; untuk tahun kabisat, “hari ke-60” bisa dipetakan ke 28 Feb atau 29 Feb (konsisten saja).
  - **Opsi 2**: 366 slot; untuk tahun non-kabisat, slot 60 (29 Feb) bisa diisi nilai sama dengan slot 59 (28 Feb) atau NULL.

Dokumen ini **tidak mengunci** pilihan; implementasi nanti bisa pilih salah satu dan dicatat di sini.

---

## 5. Skema Tabel Database (Usulan)

Tujuan: menyimpan **daily simulation untuk semua site untuk 25 tahun** dengan nilai energy yang sudah dikalikan faktor degradasi, sekaligus **tracking** kapan site dimulai (tahun kalender mana = simulation year 1) dan untuk tanggal tertentu site tersebut sedang di tahun ke berapa (simulation year 1–25).

### 5.1 Skema Gabungan: date_key + simulation_year

Tidak pakai tanggal kalender nyata; hanya “tahun simulasi” dan “Join & report per tanggal → date_key; tracking "site dimulai dari tahun berapa" → simulation_start_year (lihat 5.2); tracking "untuk tanggal ini, site sedang di tahun ke berapa" → simulation_year (1–25). Relasi: YEAR(date_key) = simulation_start_year + simulation_year - 1”.

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| site_code | TEXT | Identifier site |
| site_name | TEXT | Opsional (bisa dari dim) |
| date_key | DATE | Tanggal kalender (untuk join, filter, report) |
| simulation_year | SMALLINT | 1–25 — **"sedang di tahun ke berapa"** dalam siklus 25 tahun |
| simulation_start_year | SMALLINT | Tahun kalender ketika simulation year 1 dimulai — **"dimulai dari tahun berapa"** (redundant dari master site, untuk kemudahan query) |
| day_of_year | SMALLINT | 1–365/366 (dari date_key, untuk audit/validasi) |
| energy_simulation_mwh | NUMERIC | Energy harian simulasi (sudah × degradasi) |
| energy_target_mwh | NUMERIC | Energy harian target (sudah × degradasi, jika dipakai) |
| daily_pr_poa_simulation | NUMERIC | PR POA simulasi: PR tahun 1 × degradation_factor (dihitung, bukan disalin) |
| daily_pr_poa_target | NUMERIC | PR POA target: PR tahun 1 × degradation_factor (dihitung) |
| daily_pr_ghi_simulation | NUMERIC | PR GHI simulasi: PR tahun 1 × degradation_factor (dihitung) |
| daily_pr_ghi_target | NUMERIC | PR GHI target: PR tahun 1 × degradation_factor (dihitung) |
| ghi, poa | NUMERIC | Disalin dari tahun 1 (sama untuk seluruh 25 tahun) |
| degradation_factor | NUMERIC | Faktor degradasi yang dipakai (untuk audit) |

- **Primary key**: (site_code, date_key).  
- **Unik per site + tahun simulasi + hari**: (site_code, simulation_year, day_of_year) secara logika satu baris; `date_key` mengunci tanggal kalender.

**Contoh:** Site A mulai 2025 → `simulation_start_year = 2025`. Tanggal 2025-01-01 → `simulation_year = 1`; 2026-01-01 → `simulation_year = 2`; 2049-12-31 → `simulation_year = 25`. Site B mulai 2024 → untuk 2025-06-15 → `simulation_year = 2`. Dengan skema ini kita langsung tahu **tanggal berapa** (`date_key`), **tahun ke berapa** (`simulation_year`), dan **mulai tahun berapa** (`simulation_start_year`). “tanggal kalender”  
---

### 5.2 Master "Tahun Mulai" per Site (Referensi)

Agar tracking "site dimulai dari tahun berapa" konsisten dan satu sumber kebenaran, perlu master per site:

- **Opsi A**: Kolom di **dim_assets** (atau tabel site lain): `simulation_start_year` (SMALLINT). Satu nilai per site = tahun kalender ketika simulation year 1 dimulai.
- **Opsi B**: Tabel/seed terpisah, mis. **seed_site_simulation_config**: (site_code, simulation_start_year).

Tabel daily (5.1) bisa menyimpan simulation_start_year di setiap baris (redundant) agar query tanpa join tetap bisa filter/group menurut mulai tahun berapa, atau cukup derive dari master saat generate (tetap perlu master untuk validasi).

_ “tahun dasar” 
---

### 5.3 Tabel Pendukung: Faktor Degradasi (Opsional)

Bisa menyimpan faktor degradasi saja agar tidak hitung ulang dan agar validasi mudah.

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| site_code | TEXT | |
| simulation_year | SMALLINT | 1–25 |
| energy_yearly_mwh | NUMERIC | Dari seed yearly (Energy TS atau Sim Target) |
| degradation_factor | NUMERIC | energy_yearly_mwh / energy_year1 |

- **Primary key**: (site_code, simulation_year).

Sumber: `seed_yearly_simulation_target` (kolom energy pakai desimal koma → konversi ke numeric).

---

## 6. Konsistensi dengan Mart yang Ada

- **mart_simulation_targets_daily** saat ini hanya memuat **satu tahun** (dari seed daily). Setelah ada tabel daily 25 tahun:
  - Mart tersebut bisa tetap untuk “tahun berjalan” atau “tahun referensi”;
  - Atau bisa diganti/diperluas dari tabel baru (filter `simulation_year = 1` atau `date_key` dalam range tahun referensi).
- **mart_simulation_targets_monthly**: Jika nanti ada monthly 25 tahun, bisa di-aggregate dari tabel daily 25 tahun (SUM energy per site, simulation_year, month).

---

## 7. Ringkasan Keputusan

| Aspek | Keputusan |
|-------|-----------|
| Rumus degradasi | energy_year_n / energy_year_1, per site, per simulation year |
| Daily 25 tahun | energy_daily(s, n, d) = energy_daily_year1(s, d) × degradation_factor(s, n) |
| Kolom yang didegradasi / dihitung | **Energy** dan **PR** (simulation & target): dikalikan faktor degradasi. **GHI** dan **POA**: disalin dari tahun 1 (tidak berubah). PR tahun n = PR tahun 1 × degradation_factor(n). |
| Tahun kabisat | 365 vs 366 slot—ditetapkan saat implementasi |
| Skema tabel | Gabungan: date_key + simulation_year + simulation_start_year; satu tabel untuk join per tanggal sekaligus tracking tahun mulai dan tahun ke berapa (1–25) |

---

## 8. Langkah Implementasi (Referensi Nanti)

1. Parse dan normalisasi **seed_yearly_simulation_target** (desimal koma → numeric).  
2. Hitung dan simpan **degradation_factor** per (site_code, simulation_year).  
3. Dari **seed_daily_simulation_target** bangun lookup (site_code, day_of_year) → energy, target, GHI, POA, PR_poa, PR_ghi (tahun 1).  
4. Generate baris untuk setiap (site_code, date_key, simulation_year, simulation_start_year, day_of_year): energy dan PR = nilai tahun 1 × degradation_factor; GHI dan POA = disalin dari tahun 1. Tulis ke tabel daily (skema gabungan 5.1).  
5. (Opsional) Tabel faktor degradasi sebagai materialisasi seed yearly.  
6. Update mart/downstream jika perlu (mart_simulation_targets_daily / monthly) agar bisa pakai data 25 tahun atau tetap pakai subset tahun 1.

Dokumen ini sengaja hanya mendefinisikan **algoritma dan skema**; tidak ada perubahan kode atau database di dalamnya.
