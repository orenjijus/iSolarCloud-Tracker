# Panduan Pengisian: seed_inverter_string_poa_mapping

Seed ini memetakan setiap **string** inverter ke **sensor POA** (Plane of Array) yang dipakai untuk irradiance, serta menyimpan **kapasitas per string** dan **orientasi** (azimuth, tilt). Dipakai oleh `mart_string_performance_5min` dan perhitungan performance ratio.

---

## Format file

- **Lokasi:** `dbt/seeds/seed_inverter_string_poa_mapping.csv`
- **Delimiter:** titik koma (`;`)
- **Encoding:** UTF-8
- **Header:** baris pertama = nama kolom (jangan dihapus)

---

## Definisi kolom

| Kolom | Tipe | Wajib | Keterangan |
|-------|------|-------|------------|
| **source** | text | Ya | `FusionSolar` atau `iSolarCloud` (sesuai sumber data inverter). |
| **site_id** | text | Ya | ID site: FusionSolar = `plant_code` (contoh `NE=50488260`), iSolarCloud = `ps_id` (contoh `1458125`). |
| **inverter_id** | text | Ya | ID perangkat inverter **tanpa prefix**: FusionSolar = `dev_id`, iSolarCloud = `device_ps_key` (contoh `1458125_1_29_1`). Untuk iSolarCloud gunakan ID yang aktif di data (setelah ID consolidation di `seed_inverter_site_mapping` jika ada). |
| **inverter_name** | text | Tidak | Nama inverter (untuk dokumentasi). |
| **string_number** | integer | Ya | Nomor string di inverter (1, 2, 3, …). Harus konsisten dengan metrik `string_N_voltage` / `string_N_current` di `seed_metric_mapper`. |
| **string_id** | text | Tidak | ID unik string (contoh `FS_DEV001_STR01`, `ISO_1458125_1_29_1_STR01`). Bisa dikosongkan; di mart bisa diturunkan dari `asset_id + '_STR' + string_number`. |
| **poa_sensor_id** | text | Ya | ID perangkat sensor POA **tanpa prefix**: sama dengan `device_id` di `seed_sensor_config` (FusionSolar contoh `EM01102287046729`, iSolarCloud contoh `1458125_5_21_1`). |
| **poa_sensor_name** | text | Tidak | Nama sensor dari `seed_sensor_config` (untuk dokumentasi). |
| **azimuth_degrees** | integer | Disarankan | Orientasi string (0–360°). 0 = Utara, 90 = Timur, 180 = Selatan, 270 = Barat. Dipakai untuk mencocokkan string dengan sensor POA yang orientasinya sama/serupa. |
| **tilt_degrees** | integer | Disarankan | Kemiringan modul string (derajat). |
| **panel_count** | integer | Tidak | Jumlah panel per string. |
| **string_capacity_w** | numeric | Ya | Kapasitas string dalam Watt (W). Dipakai untuk performance ratio: `string_energy / (poa_irradiance * string_capacity)`. Sumber: dokumen design, nameplate, atau (kapasitas site / total string). |
| **notes** | text | Tidak | Catatan (mis. "North-facing", "String 1–12 pakai POA IRR-NE-B"). |

---

## Sumber data untuk pengisian

### 1. Daftar inverter

- **Dari DB (setelah dbt run):**  
  `SELECT asset_id, asset_name, site_name, system FROM dimensions.dim_assets WHERE asset_level = 'Device' AND device_type_id = 1;`  
  `inverter_id` di seed = bagian setelah prefix (FS_ / ISO_), contoh `FS_DEV001` → `inverter_id` = `DEV001`.
- **Dari seed mapping:**  
  Untuk iSolarCloud, jika ada `seed_inverter_site_mapping` (ID_CONSOLIDATION), pakai `logical_device_id` sebagai `inverter_id` yang aktif.

### 2. Daftar sensor POA per site

- Buka `seed_sensor_config`, filter `sensor_type = 'POA'` dan `site_id` = site yang sama dengan inverter.
- Kolom `device_id` → isi **poa_sensor_id** di seed.
- Kolom `dev_name` → isi **poa_sensor_name** (opsional).
- Kolom `sensor_capacity` = kapasitas POA (W) untuk referensi; orientasi sensor bisa dari nama (mis. NORTH, SOUTH, Azimuth 152.6).

### 3. Kapasitas per string (string_capacity_w)

- Dari **dokumen design / single line diagram**: kapasitas per string (W).
- Atau: **kapasitas total inverter** (kW × 1000) / **jumlah string** = perkiraan per string (W).
- Atau dari **nameplate** modul × jumlah panel per string.

### 4. Orientasi (azimuth_degrees, tilt_degrees)

- Dari **layout / as-built** atau dokumen site.
- **Matching ke POA:** pilih sensor POA yang azimuth/tilt-nya paling dekat dengan string (atau satu blok string yang diwakili sensor yang sama). Nama sensor sering menyebut orientasi (NORTH, SOUTH, Azimuth 152.6, dll.).

---

## Aturan praktis

1. **Satu baris = satu string** (satu pasang voltage/current per inverter per nomor string).
2. **inverter_id** harus sama dengan key yang dipakai di staging/dim_assets (tanpa prefix `FS_` / `ISO_`).
3. **poa_sensor_id** harus ada di `seed_sensor_config` dengan `sensor_type = 'POA'` dan `site_id` yang sesuai.
4. **string_number** harus 1..N sesuai jumlah string di inverter (FusionSolar sering 1–24/28, iSolarCloud bisa sampai 40).
5. **string_capacity_w** harus > 0 agar perhitungan performance ratio valid.

---

## Contoh isian

- **FusionSolar:**  
  `source = FusionSolar`, `site_id = plant_code`, `inverter_id = dev_id` dari raw/staging. Sensor POA dari `seed_sensor_config` (kolom `device_id`).

- **iSolarCloud (dengan ID consolidation):**  
  Pakai `logical_device_id` dari `seed_inverter_site_mapping` sebagai `inverter_id` (mis. `1458125_1_29_1`). Sensor POA pakai `device_id` iSolarCloud di `seed_sensor_config` (mis. `1458125_5_21_1`).

Setelah mengisi, jalankan `dbt seed` agar seed ter-load ke schema staging.
