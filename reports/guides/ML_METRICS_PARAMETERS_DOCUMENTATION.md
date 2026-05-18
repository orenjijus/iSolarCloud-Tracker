# Dokumentasi Parameter Metrik untuk Machine Learning

## Pendahuluan

Dokumen ini menjelaskan semua parameter metrik yang digunakan dalam sistem analitik PLTS (Pembangkit Listrik Tenaga Surya) untuk keperluan machine learning. Setiap parameter dilengkapi dengan penjelasan detail, rumus kalkulasi (jika ada), unit pengukuran, dan konteks penggunaan.

Dokumen ini dirancang untuk membantu tim machine learning memahami setiap metrik yang akan digunakan sebagai feature dalam model prediksi, klasifikasi, atau analisis lainnya.

---

## 1. Metrik Energi (Energy Metrics)

### 1.1 Daily Energy (Energi Harian)

**Penjelasan:**
Daily Energy adalah total energi yang dihasilkan oleh PLTS dalam satu hari, diukur dari revenue meter (meter pendapatan). Metrik ini merupakan indikator utama performa sistem karena menunjukkan berapa banyak energi listrik yang berhasil dihasilkan dan dapat dijual.

**Sumber Data:**
- Tabel: `mart_meter_performance_5min`
- Filter: `meter_type = 'Revenue'` dari `seed_meter_config`
- Metric: `positive_active_energy` atau `negative_active_energy`

**Rumus Kalkulasi:**

Untuk meter kumulatif (nilai terus bertambah):
```
Daily Energy (kWh) = MAX(metric_value) - MAX(metric_value_hari_sebelumnya)
```

Untuk meter reset (nilai di-reset setiap hari):
```
Daily Energy (kWh) = MAX(metric_value) - MIN(metric_value)
```

**Konversi Unit:**
- Input: kWh atau Wh (tergantung sistem)
- Output: MWh (untuk konsistensi dengan target)
- Konversi: `Daily Energy (MWh) = Daily Energy (kWh) / 1000`

**Polarity Correction:**
Jika meter memiliki flag `polarity_swapped = TRUE`:
```
Daily Energy = ABS(negative_energy - positive_energy)
```
Jika tidak:
```
Daily Energy = ABS(positive_energy - negative_energy)
```

**Multiple Meters:**
Jika site memiliki lebih dari satu revenue meter, total energi dihitung dengan:
```
Total Daily Energy = SUM(Daily Energy dari semua revenue meters)
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `daily_energy_mwh` | Total energi harian yang dihasilkan | MWh | ≥ 0 | Nilai NULL jika tidak ada data meter |
| `daily_energy_kwh` | Total energi harian (dalam kWh) | kWh | ≥ 0 | Dihitung sebelum konversi ke MWh |
| `is_polarity_swapped` | Flag apakah meter perlu polarity correction | Boolean | TRUE/FALSE | Dari `seed_meter_config` |
| `is_cumulative` | Flag apakah meter bersifat kumulatif | Boolean | TRUE/FALSE | Deteksi otomatis dari data |

**Threshold Minimum:**
- Minimum energy untuk validasi: `0.01 MWh` (10 kWh)
- Nilai di bawah threshold dianggap tidak valid dan akan menghasilkan NULL

---

## 2. Metrik Irradiance (Irradiance Metrics)

### 2.1 GHI (Global Horizontal Irradiance)

**Penjelasan:**
GHI adalah total radiasi matahari yang diterima oleh permukaan horizontal di permukaan bumi. GHI mengukur intensitas radiasi matahari yang tersedia secara global, tidak terpengaruh oleh orientasi panel surya. Metrik ini penting untuk memahami potensi energi matahari yang tersedia di lokasi site.

**Sumber Data:**
- Tabel: `mart_sensor_measurements_5min`
- Filter: `sensor_type = 'GHI'` dari `seed_sensor_config`
- Metric: `daily_irradiance`

**Rumus Kalkulasi:**

```
Daily GHI (kWh/m²) = MAX(daily_irradiance per sensor per hari)
```

Jika site memiliki multiple GHI sensors:
```
Site GHI = MAX(GHI dari semua sensors)
```

**Konversi Unit:**

| Sistem | Unit Input | Konversi ke kWh/m² |
|--------|------------|-------------------|
| FusionSolar | MJ/m² | `value / 3.6` |
| iSolarCloud | Wh/㎡ | `value / 1000` |
| Lainnya (MJ/m²) | MJ/m² | `value / 3.6` |
| Lainnya (W/m²) | W/m² | `value / 1000` |

**GHI Fallback Logic:**
Beberapa site tidak memiliki sensor GHI sendiri, sehingga menggunakan GHI dari site lain (source site). Contoh: MMKI Phase 2 dan Phase 3 menggunakan GHI dari MMKI Phase 1.

```
IF site tidak memiliki GHI sensor:
    GHI = GHI dari source site (berdasarkan seed_sensor_site_mapping)
ELSE:
    GHI = GHI dari sensor site sendiri
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `daily_ghi_kwh_m2` | Total GHI harian | kWh/m² | ≥ 0 | Nilai NULL jika tidak ada data sensor |
| `ghi_source` | Sumber GHI (OWN/GHI_FALLBACK) | String | - | Untuk tracking sumber data |
| `ghi_sensor_count` | Jumlah sensor GHI aktif | Integer | ≥ 0 | Untuk validasi data |

**Threshold Minimum:**
- Minimum GHI untuk validasi: `0.1 kWh/m²`
- Nilai di bawah threshold dianggap tidak valid untuk perhitungan PR

---

### 2.2 POA (Plane of Array Irradiance)

**Penjelasan:**
POA adalah radiasi matahari yang diterima oleh permukaan panel surya dengan orientasi dan kemiringan tertentu. POA lebih akurat daripada GHI untuk menghitung performa sistem karena mengukur radiasi yang benar-benar diterima oleh panel. POA dihitung sebagai weighted average berdasarkan kapasitas setiap sensor POA.

**Sumber Data:**
- Tabel: `mart_sensor_measurements_5min`
- Filter: `sensor_type = 'POA'` dari `seed_sensor_config`
- Metric: `daily_irradiance`

**Rumus Kalkulasi:**

Step 1: Hitung MAX daily irradiance per sensor:
```
POA_per_sensor (kWh/m²) = MAX(daily_irradiance per sensor per hari)
```

Step 2: Weighted average berdasarkan kapasitas:
```
Daily POA (kWh/m²) = SUM(POA_per_sensor × poa_capacity_kwp) / SUM(poa_capacity_kwp)
```

**Konversi Unit:**
Sama seperti GHI (lihat tabel konversi di atas).

**POA Override Logic:**
Beberapa sensor POA secara fisik berada di site A, tetapi secara logis digunakan untuk site B. Mapping ini dikonfigurasi di `seed_sensor_site_mapping` dengan `mapping_type = 'POA_OVERRIDE'`.

```
IF sensor memiliki POA_OVERRIDE mapping:
    POA di-assign ke logical_site_id (bukan physical_site_name)
ELSE:
    POA di-assign ke physical_site_name
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `daily_poa_weighted_kwh_m2` | POA harian (weighted average) | kWh/m² | ≥ 0 | Nilai NULL jika tidak ada data sensor |
| `poa_sensor_count` | Jumlah sensor POA aktif | Integer | ≥ 0 | Untuk validasi data |
| `poa_total_capacity_kwp` | Total kapasitas semua sensor POA | kWp | ≥ 0 | Untuk validasi weighted average |

**Threshold Minimum:**
- Minimum POA untuk validasi: `0.1 kWh/m²`
- Nilai di bawah threshold dianggap tidak valid untuk perhitungan PR

---

## 3. Metrik Performance Ratio (PR Metrics)

### 3.1 PR GHI (Performance Ratio berdasarkan GHI)

**Penjelasan:**
PR GHI adalah rasio antara energi aktual yang dihasilkan dengan energi yang diharapkan berdasarkan GHI. PR GHI mengukur efisiensi sistem dalam mengkonversi radiasi matahari global menjadi energi listrik. Nilai PR yang tinggi menunjukkan sistem beroperasi dengan efisien.

**Rumus Kalkulasi:**

```
PR GHI (%) = (Daily Energy (kWh) / Daily GHI (kWh/m²) / Site Capacity (kW)) × 100
```

**Dengan konversi unit:**
```
PR GHI (%) = ((Daily Energy (MWh) × 1000) / Daily GHI (kWh/m²) / Site Capacity (kW)) × 100
```

**Penjelasan Rumus:**
- **Numerator**: Energi aktual yang dihasilkan (dalam kWh)
- **Denominator**: Energi yang diharapkan = GHI × Capacity
- **Hasil**: Persentase efisiensi (0-100%, atau bisa > 100% jika performa sangat baik)

**Kondisi Validasi:**
PR GHI hanya dihitung jika:
- `daily_ghi_kwh_m2 >= 0.1 kWh/m²`
- `daily_energy_mwh >= 0.01 MWh`
- `site_capacity_kw > 0`
- Semua nilai tidak NULL

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `pr_ghi_actual` | Performance Ratio berdasarkan GHI | % | 0-10000 | Cap maksimum 10000% untuk prevent overflow |
| `pr_ghi_target` | Target PR GHI dari simulasi | % | 0-100 | Dari `mart_simulation_targets_daily` |
| `pr_ghi_variance_pct` | Selisih PR aktual vs target | % | -∞ to +∞ | `pr_ghi_actual - pr_ghi_target` |

**Interpretasi Nilai:**
- **PR < 70%**: Performa buruk, perlu investigasi
- **PR 70-80%**: Performa normal untuk sistem dengan losses
- **PR 80-90%**: Performa baik
- **PR > 90%**: Performa sangat baik
- **PR > 100%**: Mungkin ada error data atau kondisi khusus

---

### 3.2 PR POA (Performance Ratio berdasarkan POA)

**Penjelasan:**
PR POA adalah rasio antara energi aktual yang dihasilkan dengan energi yang diharapkan berdasarkan POA. PR POA lebih akurat daripada PR GHI karena menggunakan radiasi yang benar-benar diterima oleh panel. PR POA biasanya lebih tinggi daripada PR GHI karena POA lebih besar daripada GHI (panel menghadap matahari).

**Rumus Kalkulasi:**

```
PR POA (%) = (Daily Energy (kWh) / Daily POA (kWh/m²) / Site Capacity (kW)) × 100
```

**Dengan konversi unit:**
```
PR POA (%) = ((Daily Energy (MWh) × 1000) / Daily POA (kWh/m²) / Site Capacity (kW)) × 100
```

**Penjelasan Rumus:**
- **Numerator**: Energi aktual yang dihasilkan (dalam kWh)
- **Denominator**: Energi yang diharapkan = POA × Capacity
- **Hasil**: Persentase efisiensi (0-100%, atau bisa > 100% jika performa sangat baik)

**Kondisi Validasi:**
PR POA hanya dihitung jika:
- `daily_poa_weighted_kwh_m2 >= 0.1 kWh/m²`
- `daily_energy_mwh >= 0.01 MWh`
- `site_capacity_kw > 0`
- Semua nilai tidak NULL

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `pr_poa_actual` | Performance Ratio berdasarkan POA | % | 0-10000 | Cap maksimum 10000% untuk prevent overflow |
| `pr_poa_target` | Target PR POA dari simulasi | % | 0-100 | Dari `mart_simulation_targets_daily` |
| `pr_poa_variance_pct` | Selisih PR aktual vs target | % | -∞ to +∞ | `pr_poa_actual - pr_poa_target` |

**Interpretasi Nilai:**
- **PR POA < 75%**: Performa buruk, perlu investigasi
- **PR POA 75-85%**: Performa normal untuk sistem dengan losses
- **PR POA 85-95%**: Performa baik
- **PR POA > 95%**: Performa sangat baik
- **PR POA > 100%**: Mungkin ada error data atau kondisi khusus

**Perbandingan PR GHI vs PR POA:**
- PR POA biasanya lebih tinggi daripada PR GHI (karena POA > GHI)
- Selisih antara PR POA dan PR GHI menunjukkan efek orientasi panel
- Jika PR POA jauh lebih tinggi daripada PR GHI, panel memiliki orientasi yang baik

---

## 4. Metrik Availability (Availability Metrics)

### 4.1 MIT (Minimum Irradiance Threshold)

**Penjelasan:**
MIT adalah threshold minimum irradiance yang diperlukan untuk sistem dianggap "beroperasi". MIT digunakan untuk membedakan antara "tidak tersedia karena masalah teknis" dengan "tidak beroperasi karena tidak ada sinar matahari". Sistem hanya dihitung sebagai "unavailable" jika irradiance sudah melebihi MIT tetapi tidak menghasilkan energi.

**Rumus Kalkulasi:**

```
MIT = 1 IF (GHI > 40 W/m² OR POA > 40 W/m²) ELSE 0
```

**Fallback Priority untuk MIT:**
1. **Priority 1**: GHI dari site yang sama
2. **Priority 2**: POA dari site yang sama (fallback)
3. **Priority 3**: GHI dari site lain (configured fallback)

**Konversi Unit:**
- Threshold: `40 W/m²` = `0.04 kW/m²`
- Untuk 5-minute interval: `40 W/m² × (5/60) hours = 3.33 Wh/m²`

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `mit` | Minimum Irradiance Threshold flag | Integer | 0 atau 1 | 1 = ada sinar matahari, 0 = tidak ada |
| `mit_source` | Sumber data untuk MIT (GHI/POA/GHI_FALLBACK) | String | - | Untuk tracking sumber data |

---

### 4.2 Power Available Ratio (Rasio Daya Tersedia)

**Penjelasan:**
Power Available Ratio adalah rasio kapasitas inverter yang tersedia pada setiap interval 5 menit. Rasio ini dihitung berdasarkan jumlah inverter yang aktif (active_power > 0) dibandingkan dengan total inverter di site.

**Rumus Kalkulasi (5-minute interval):**

```
Power Available Ratio = SUM(available_inverter_capacity) / SUM(total_inverter_capacity)
```

Dimana:
- `available_inverter_capacity = inverter_capacity IF active_power > 0 ELSE 0`
- `total_inverter_capacity = SUM(inverter_capacity)`

**Jika tidak ada data kapasitas (temporary):**
```
Power Available Ratio = COUNT(available_inverters) / COUNT(total_inverters)
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `power_available_ratio` | Rasio daya tersedia per interval | Decimal | 0-1 | 1 = semua inverter aktif, 0 = semua inverter mati |
| `inverter_available_count` | Jumlah inverter yang aktif | Integer | ≥ 0 | Untuk detail tracking |
| `inverter_total_count` | Total jumlah inverter | Integer | ≥ 0 | Untuk detail tracking |

---

### 4.3 Unavailability Ratio (Rasio Ketidaktersediaan)

**Penjelasan:**
Unavailability Ratio adalah rasio ketidaktersediaan sistem yang hanya dihitung ketika MIT = 1 (ada sinar matahari). Ini membedakan antara "tidak beroperasi karena tidak ada matahari" dengan "tidak beroperasi karena masalah teknis".

**Rumus Kalkulasi:**

```
Unavailability Ratio = CASE 
    WHEN mit = 1 THEN (1 - power_available_ratio)
    ELSE 0
END
```

**Penjelasan:**
- Jika `mit = 0` (tidak ada sinar matahari): `unavailability_ratio = 0` (tidak dihitung sebagai unavailable)
- Jika `mit = 1` (ada sinar matahari): `unavailability_ratio = 1 - power_available_ratio` (dihitung sebagai unavailable)

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `unavailability_ratio` | Rasio ketidaktersediaan per interval | Decimal | 0-1 | Hanya > 0 ketika MIT = 1 |

---

### 4.4 Power Available Hours (Jam Daya Tersedia)

**Penjelasan:**
Power Available Hours adalah total jam dalam sehari dimana sistem tersedia dan beroperasi. Dihitung dengan menjumlahkan semua interval 5 menit dimana `power_available_ratio > 0` dan `mit = 1`.

**Rumus Kalkulasi:**

```
Power Available Hours = SUM(power_available_ratio) × (5/60) hours
```

Dimana:
- Setiap interval 5 menit = `5/60 = 0.0833 hours`
- Hanya interval dengan `mit = 1` yang dihitung

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `power_available_hours` | Total jam daya tersedia per hari | Hours | 0-24 | Maksimum 24 jam (jika matahari 24 jam) |

---

### 4.5 Unavailability Hours (Jam Ketidaktersediaan)

**Penjelasan:**
Unavailability Hours adalah total jam dalam sehari dimana sistem tidak tersedia meskipun ada sinar matahari (MIT = 1). Ini menunjukkan waktu dimana sistem seharusnya beroperasi tetapi tidak beroperasi karena masalah teknis.

**Rumus Kalkulasi:**

```
Unavailability Hours = SUM(unavailability_ratio) × (5/60) hours
```

Dimana:
- Setiap interval 5 menit = `5/60 = 0.0833 hours`
- Hanya interval dengan `mit = 1` yang dihitung

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `unavailability_hours` | Total jam ketidaktersediaan per hari | Hours | 0-24 | Hanya dihitung ketika MIT = 1 |

---

### 4.6 Availability Percent (Persentase Ketersediaan)

**Penjelasan:**
Availability Percent adalah persentase waktu dimana sistem tersedia dan beroperasi dibandingkan dengan total waktu dimana sistem seharusnya beroperasi (MIT = 1). Ini adalah metrik utama untuk mengukur reliability sistem.

**Rumus Kalkulasi:**

```
Availability (%) = (Power Available Hours / (Power Available Hours + Unavailability Hours)) × 100
```

**Atau dalam bentuk ratio:**
```
Availability (%) = (SUM(power_available_ratio) / (SUM(power_available_ratio) + SUM(unavailability_ratio))) × 100
```

**Kondisi Validasi:**
Availability hanya dihitung jika:
- `SUM(power_available_ratio) + SUM(unavailability_ratio) > 0`
- Ada minimal satu interval dengan `mit = 1`

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `availability_percent` | Persentase ketersediaan harian | % | 0-100 | 100% = sistem selalu tersedia ketika ada matahari |

**Interpretasi Nilai:**
- **Availability = 100%**: Sistem selalu tersedia ketika ada sinar matahari (ideal)
- **Availability 95-99%**: Performa sangat baik, downtime minimal
- **Availability 90-95%**: Performa baik, ada sedikit downtime
- **Availability < 90%**: Perlu investigasi, ada masalah availability yang signifikan

**Catatan Penting:**
- Availability hanya dihitung untuk waktu dimana `mit = 1` (ada sinar matahari)
- Waktu malam hari (mit = 0) tidak dihitung sebagai unavailable
- Availability berbeda dengan Capacity Factor (yang menghitung energi aktual vs energi maksimum)

---

## 5. Metrik Perbandingan dengan Target (Target Comparison Metrics)

### 5.1 Energy Actual vs Target

**Penjelasan:**
Metrik ini membandingkan energi aktual yang dihasilkan dengan energi target dari simulasi. Target energi biasanya dihitung berdasarkan kondisi cuaca historis dan karakteristik sistem. Perbandingan ini membantu mengidentifikasi apakah sistem beroperasi sesuai ekspektasi.

**Rumus Kalkulasi:**

```
Energy Actual vs Target (%) = (Daily Energy Actual (MWh) / Energy Target (MWh)) × 100
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `energy_target_mwh` | Target energi dari simulasi | MWh | ≥ 0 | Dari `mart_simulation_targets_daily` |
| `energy_actual_vs_target_pct` | Persentase aktual vs target | % | 0-∞ | 100% = sesuai target, >100% = melebihi target |

**Interpretasi Nilai:**
- **> 100%**: Energi aktual melebihi target (sangat baik)
- **90-100%**: Energi aktual mendekati target (baik)
- **80-90%**: Energi aktual di bawah target (perlu perhatian)
- **< 80%**: Energi aktual jauh di bawah target (perlu investigasi)

---

### 5.2 GHI Actual vs Target

**Penjelasan:**
Metrik ini membandingkan GHI aktual dengan GHI target dari simulasi. GHI target biasanya dihitung berdasarkan data cuaca historis atau model cuaca. Perbandingan ini membantu memahami apakah kondisi cuaca aktual sesuai dengan ekspektasi.

**Rumus Kalkulasi:**

```
GHI Actual vs Target (%) = (Daily GHI Actual (kWh/m²) / GHI Target (kWh/m²)) × 100
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `ghi_target` | Target GHI dari simulasi | kWh/m² | ≥ 0 | Dari `mart_simulation_targets_daily` |
| `ghi_actual_vs_target_pct` | Persentase aktual vs target | % | 0-∞ | 100% = sesuai target |

**Interpretasi Nilai:**
- **> 100%**: GHI aktual lebih tinggi daripada target (cuaca lebih cerah)
- **90-100%**: GHI aktual mendekati target (cuaca normal)
- **< 90%**: GHI aktual lebih rendah daripada target (cuaca lebih mendung)

---

### 5.3 POA Actual vs Target

**Penjelasan:**
Metrik ini membandingkan POA aktual dengan POA target dari simulasi. POA target dihitung berdasarkan orientasi dan kemiringan panel. Perbandingan ini membantu memahami apakah radiasi yang diterima panel sesuai dengan ekspektasi.

**Rumus Kalkulasi:**

```
POA Actual vs Target (%) = (Daily POA Actual (kWh/m²) / POA Target (kWh/m²)) × 100
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `poa_target` | Target POA dari simulasi | kWh/m² | ≥ 0 | Dari `mart_simulation_targets_daily` |
| `poa_actual_vs_target_pct` | Persentase aktual vs target | % | 0-∞ | 100% = sesuai target |

**Interpretasi Nilai:**
- **> 100%**: POA aktual lebih tinggi daripada target (radiasi lebih baik)
- **90-100%**: POA aktual mendekati target (radiasi normal)
- **< 90%**: POA aktual lebih rendah daripada target (radiasi lebih rendah)

---

### 5.4 Energy vs GHI Variance

**Penjelasan:**
Metrik ini menunjukkan perbedaan antara performa energi dengan performa GHI. Jika variance positif, berarti energi berperform lebih baik daripada yang diharapkan berdasarkan GHI. Jika variance negatif, berarti energi berperform lebih buruk daripada yang diharapkan.

**Rumus Kalkulasi:**

```
Energy vs GHI Variance (%) = Energy Actual vs Target (%) - GHI Actual vs Target (%)
```

**Atau:**
```
Energy vs GHI Variance (%) = ((Energy Actual / Energy Target) × 100) - ((GHI Actual / GHI Target) × 100)
```

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `energy_vs_ghi_variance_pct` | Selisih performa energi vs GHI | % | -∞ to +∞ | Positif = energi lebih baik, Negatif = energi lebih buruk |

**Interpretasi Nilai:**
- **> 0**: Energi berperform lebih baik daripada yang diharapkan berdasarkan GHI (sistem efisien)
- **= 0**: Energi berperform sesuai dengan yang diharapkan berdasarkan GHI (normal)
- **< 0**: Energi berperform lebih buruk daripada yang diharapkan berdasarkan GHI (ada masalah sistem)

**Contoh:**
- Jika `energy_actual_vs_target = 95%` dan `ghi_actual_vs_target = 90%`
- Maka `energy_vs_ghi_variance = 95% - 90% = +5%`
- Artinya: Meskipun GHI 10% di bawah target, energi hanya 5% di bawah target (sistem berperform lebih baik daripada yang diharapkan)

---

## 6. Metrik Kapasitas dan Konfigurasi (Capacity & Configuration Metrics)

### 6.1 Site Capacity

**Penjelasan:**
Site Capacity adalah kapasitas terpasang total dari PLTS, diukur dalam kilowatt (kW). Kapasitas ini digunakan dalam perhitungan PR dan perbandingan dengan target.

**Sumber Data:**
- Tabel: `dim_site`
- Kolom: `actual_capacity_kw`

**Tabel Parameter:**

| Parameter | Deskripsi | Unit | Range | Catatan |
|-----------|-----------|------|-------|---------|
| `actual_capacity_kw` | Kapasitas terpasang site | kW | > 0 | Dari `dim_site` |
| `site_capacity_mw` | Kapasitas dalam MW | MW | > 0 | `actual_capacity_kw / 1000` |

---

## 7. Metrik Temporal (Temporal Metrics)

### 7.1 Date Dimensions

**Penjelasan:**
Metrik temporal digunakan untuk analisis time-series dan agregasi berdasarkan periode waktu (harian, bulanan, tahunan).

**Tabel Parameter:**

| Parameter | Deskripsi | Format | Catatan |
|-----------|-----------|--------|---------|
| `date_key` | Tanggal data | DATE | Format: YYYY-MM-DD |
| `year` | Tahun | Integer | 2024, 2025, dst |
| `month` | Bulan | Integer | 1-12 |
| `month_name` | Nama bulan | String | January, February, dst |
| `quarter` | Kuartal | Integer | 1-4 |
| `day_of_year` | Hari ke-berapa dalam tahun | Integer | 1-366 |
| `is_weekend` | Flag akhir pekan | Boolean | TRUE/FALSE |

---

## 8. Metrik Flag dan Status (Flag & Status Metrics)

### 8.1 Issue Date Flag

**Penjelasan:**
Flag ini menandai tanggal-tanggal dimana terjadi issue atau maintenance yang mempengaruhi performa sistem. Tanggal-tanggal ini biasanya di-exclude dari perhitungan adjusted performance.

**Sumber Data:**
- Tabel: `seed_issue_dates`
- Kolom: `issue_date`, `site_id`, `is_active`

**Tabel Parameter:**

| Parameter | Deskripsi | Type | Catatan |
|-----------|-----------|------|---------|
| `is_issue_date` | Flag apakah tanggal adalah issue date | Boolean | TRUE/FALSE |
| `issue_date` | Tanggal issue | DATE | Dari `seed_issue_dates` |
| `is_active` | Flag apakah issue masih aktif | Boolean | TRUE/FALSE |

---

## 9. Ringkasan Rumus Utama

### 9.1 Tabel Rumus Kalkulasi

| Metrik | Rumus | Unit Output |
|--------|-------|-------------|
| **Daily Energy** | `MAX(metric_value) - MAX(prev_day_value)` atau `MAX - MIN` | MWh |
| **Daily GHI** | `MAX(daily_irradiance)` per sensor | kWh/m² |
| **Daily POA** | `SUM(POA_sensor × capacity) / SUM(capacity)` | kWh/m² |
| **PR GHI** | `((Energy_MWh × 1000) / GHI_kWh_m2 / Capacity_kW) × 100` | % |
| **PR POA** | `((Energy_MWh × 1000) / POA_kWh_m2 / Capacity_kW) × 100` | % |
| **MIT** | `1 IF (GHI > 40 W/m² OR POA > 40 W/m²) ELSE 0` | 0 atau 1 |
| **Power Available Ratio** | `SUM(available_capacity) / SUM(total_capacity)` | 0-1 |
| **Unavailability Ratio** | `CASE WHEN mit=1 THEN (1-power_available_ratio) ELSE 0 END` | 0-1 |
| **Power Available Hours** | `SUM(power_available_ratio) × (5/60)` | Hours |
| **Unavailability Hours** | `SUM(unavailability_ratio) × (5/60)` | Hours |
| **Availability** | `(power_available_hours / (power_available_hours + unavailability_hours)) × 100` | % |
| **Energy vs Target** | `(Energy_Actual / Energy_Target) × 100` | % |
| **GHI vs Target** | `(GHI_Actual / GHI_Target) × 100` | % |
| **POA vs Target** | `(POA_Actual / POA_Target) × 100` | % |
| **Energy vs GHI Variance** | `Energy_vs_Target (%) - GHI_vs_Target (%)` | % |

---

## 10. Panduan Penggunaan untuk Machine Learning

### 10.1 Feature Engineering Recommendations

**Time-based Features:**
- Gunakan `year`, `month`, `quarter`, `day_of_year` untuk capture seasonality
- Buat lag features (nilai hari sebelumnya) untuk capture autocorrelation
- Buat rolling window features (rata-rata 7 hari, 30 hari) untuk capture trend

**Interaction Features:**
- `PR_GHI × GHI_Actual` untuk capture efek interaksi
- `Energy_Actual / Capacity` untuk normalized energy
- `Availability × PR_GHI` untuk capture combined performance

**Target Variables untuk Prediksi:**
- `daily_energy_mwh`: Prediksi energi harian
- `pr_ghi_actual`: Prediksi performance ratio
- `availability_percent`: Prediksi availability
- `energy_actual_vs_target_pct`: Prediksi performa vs target

**Feature Selection:**
- Prioritaskan: `daily_ghi_kwh_m2`, `daily_poa_weighted_kwh_m2`, `pr_ghi_actual`, `availability_percent`
- Pertimbangkan: `energy_actual_vs_target_pct`, `ghi_actual_vs_target_pct`
- Hati-hati dengan: `is_issue_date` (bisa menyebabkan data leakage jika digunakan untuk prediksi)

### 10.2 Data Quality Considerations

**Missing Values:**
- GHI/POA NULL: Gunakan fallback logic atau imputation
- Energy NULL: Investigasi lebih lanjut, mungkin ada masalah meter
- PR NULL: Biasanya karena salah satu komponen (Energy/GHI/POA/Capacity) NULL

**Outliers:**
- PR > 100%: Validasi dengan data mentah, mungkin ada error
- Availability < 50%: Investigasi, mungkin ada masalah sistem
- Energy vs Target < 50%: Investigasi, mungkin ada masalah atau kondisi cuaca ekstrem

**Data Validation:**
- Pastikan `daily_energy_mwh >= 0`
- Pastikan `daily_ghi_kwh_m2 >= 0` dan `daily_poa_weighted_kwh_m2 >= 0`
- Pastikan `availability_percent` antara 0-100%
- Pastikan `pr_ghi_actual` dan `pr_poa_actual` reasonable (biasanya 50-100%)

---

## 11. Referensi Tabel Database

### 11.1 Tabel Utama

| Tabel | Deskripsi | Schema |
|-------|-----------|--------|
| `mart_site_performance_daily` | Tabel utama untuk semua metrik harian | `mart` |
| `mart_meter_performance_5min` | Data meter 5 menit | `mart` |
| `mart_sensor_measurements_5min` | Data sensor 5 menit | `mart` |
| `mart_inverter_performance_5min` | Data inverter 5 menit | `mart` |
| `fact_site_calculations_5min` | Kalkulasi availability 5 menit | `mart` |
| `mart_simulation_targets_daily` | Target dari simulasi | `mart` |
| `dim_site` | Dimensi site (kapasitas, dll) | `dimensions` |
| `dim_date_generated` | Dimensi tanggal | `dimensions` |
| `seed_meter_config` | Konfigurasi meter | `staging` |
| `seed_sensor_config` | Konfigurasi sensor | `staging` |
| `seed_sensor_site_mapping` | Mapping sensor ke site | `staging` |
| `seed_issue_dates` | Tanggal issue/maintenance | `staging` |

---

## 12. Catatan Penting

1. **Unit Konsistensi**: Pastikan semua unit sudah dikonversi dengan benar sebelum digunakan dalam model ML
2. **Threshold Minimum**: Beberapa metrik memiliki threshold minimum untuk validasi, pastikan data memenuhi threshold
3. **NULL Handling**: Beberapa metrik bisa NULL jika komponennya tidak tersedia, pastikan handle NULL dengan benar
4. **Temporal Dependencies**: Beberapa metrik memiliki dependencies temporal (misalnya meter kumulatif), pastikan urutan data benar
5. **Site-specific Logic**: Beberapa site memiliki logic khusus (GHI fallback, POA override), pastikan memahami logic ini
6. **Data Quality**: Selalu validasi data sebelum digunakan dalam model ML, terutama untuk outliers dan missing values

---

**Dokumen ini dibuat untuk membantu tim machine learning memahami semua parameter metrik yang tersedia dalam sistem. Untuk pertanyaan lebih lanjut, silakan hubungi tim data engineering.**

