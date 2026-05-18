# Laporan Koreksi Konfigurasi — Weiss Tech & Gelora Djaja


| Field         | Nilai                                                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **Tanggal**   | 17 Mei 2026                                                                                                                    |
| **Lingkup**   | `seed_meter_config`, `seed_sensor_config`, dampak `mart_sensor_daily` / `mart_site_performance_daily`                          |
| **Referensi** | [Implementation Plan](./2026-05-16_Implementation_Plan.md) · [8 Site Energy](./2026-05-16_8Site_Energy_Availability_Report.md) · [Gelora calc start 7 Apr 2026](./2026-05-18_Gelora_Calculation_Start_Date_Update.md) |


---

## 1. Ringkasan

Konfigurasi seed untuk **PLTS Rooftop Weiss Tech** dan **PT Gelora Djaja 1 MWp** disesuaikan berdasarkan konfirmasi lapangan. Setelah perubahan, seed di-load ulang dan `mart_sensor_daily` di-**full-refresh** agar tipe sensor lama (mis. Weiss sebagai GHI) tidak tertinggal di mart incremental.

**Meteo Station 2 (Weiss)** — **tidak dipakai**: device kosong di portal, tidak ada data irradiance; **tidak** ditambahkan ke `seed_sensor_config`.

---

## 2. PLTS Rooftop Weiss Tech

### 2.1 Sensor


| Device (portal)     | `device_id` / `asset_id` | Tipe di seed      | Keterangan                                                                                              |
| ------------------- | ------------------------ | ----------------- | ------------------------------------------------------------------------------------------------------- |
| **Meteo Station 1** | `1789614_5_8_1`          | **POA**           | Bukan GHI — sebelumnya salah sehingga nilai POA masuk kolom GHI di mart                                 |
| **Meteo Station 2** | `1789614_5_9_1`          | *(tidak di-seed)* | Tidak dipakai untuk performa: di raw hanya `ambient_temp`, **tanpa** irradiance/GHI/POA — **diabaikan** |


**Baris seed (`seed_sensor_config.csv`):**

```text
iSolarCloud;1789614;Meteo Station1;1789614_5_8_1;POA;
```

### 2.2 Meter revenue


| Item          | Nilai                                                |
| ------------- | ---------------------------------------------------- |
| Meter revenue | **WT-RM-01** → `1789614_7_7_1` → `ISO_1789614_7_7_1` |
| Capacity site | **238.08 kWp** (`seed_site_config`)                  |
| Tariff        | **683.61** Rp/kWh (`seed_site_config`, 18 Mei 2026)  |


### 2.3 Dampak di mart (Apr–16 Mei 2026, setelah refresh)


| Metrik                      | Sebelum koreksi                       | **Sesudah**                                   |
| --------------------------- | ------------------------------------- | --------------------------------------------- |
| `daily_ghi_kwh_m2`          | Terisi (~45 hari) dari POA yang salah | **= nilai POA** Meteo Station 1 via `GHI_ADJUSTED` |
| `daily_poa_weighted_kwh_m2` | NULL                                  | **Terisi** (`sensor_capacity` 238,08 kWp)          |
| `daily_energy_mwh`          | Terisi                                | Terisi (~45 hari)                                  |


**GHI dari POA:** `seed_sensor_site_mapping` → `GHI_ADJUSTED;PLTS Rooftop Weiss Tech;ISO_1789614_5_8_1;2025-01-01;;` — mart memakai `daily_irradiance` sensor POA untuk `daily_ghi_kwh_m2`, `ghi_actual`, dan `ghi_adjusted`.

---

## 3. PT Gelora Djaja 1 MWp

### 3.1 Meter revenue


| Versi                         | Konfigurasi                           | Evaluasi                                                         |
| ----------------------------- | ------------------------------------- | ---------------------------------------------------------------- |
| Awal (16 Mei)                 | Hanya **EM-MVSWG** `AM0110254C346758` | Diganti — bukan titik revenue resmi                              |
| **Final**                     | Hanya **EM-POI** `AM0610254C346758`   | ✅ Dipakai setelah uji dual-revenue dibatalkan                    |
| Uji 17 Mei malam (dibatalkan) | POI + MVSWG keduanya Revenue          | **Energi kebesaran** (~2× stream); mart menjumlahkan kedua meter |


**Baris seed (`seed_meter_config.csv`):**

```text
FusionSolar;NE=61847068;EM-POI;AM0610254C346758;MV;Revenue;
FusionSolar;NE=61847068;EM-MVSWG;AM0110254C346758;MV;;
```

EM-MVSWG dan meter AM lain tetap di seed **tanpa** `meter_type = Revenue`.

**Catatan:** Saat **EM-POI** tidak ada interval di mart (Mei 7+), `energy_actual` bisa **0** walau MVSWG masih hidup — itu gap ingest/POI, bukan indikasi harus menjumlahkan MVSWG. Tindak lanjut: cek ingest **EM-POI** di FusionSolar, bukan menambah meter Revenue kedua.

### 3.2 Sensor


| Sensor (portal)      | `device_id`        | Tipe    | Kapasitas POA       |
| -------------------- | ------------------ | ------- | ------------------- |
| EMI-EM0010254C346758 | `EM0010254C346758` | **GHI** | —                   |
| EMI-EM0110254C346758 | `EM0110254C346758` | **POA** | **belum** — ditunda |
| EMI-EM0210254C346758 | `EM0210254C346758` | **POA** | **belum** — ditunda |
| EMI-EM0310254C346758 | `EM0310254C346758` | **POA** | **belum** — ditunda |


**Baris seed (`seed_sensor_config.csv`):**

```text
FusionSolar;NE=61847068;EMI-EM0010254C346758;EM0010254C346758;GHI;
FusionSolar;NE=61847068;EMI-EM0110254C346758;EM0110254C346758;POA;
FusionSolar;NE=61847068;EMI-EM0210254C346758;EM0210254C346758;POA;
FusionSolar;NE=61847068;EMI-EM0310254C346758;EM0310254C346758;POA;
```

### 3.3 Dampak di mart (Apr–16 Mei 2026)


| Metrik                      | Keterangan                                                                                             |
| --------------------------- | ------------------------------------------------------------------------------------------------------ |
| `daily_ghi_kwh_m2`          | Dari **EM001** (GHI) — ~38 hari terisi di rentang Apr–Mei                                              |
| `daily_poa_weighted_kwh_m2` | **NULL** sampai kapasitas per sensor POA diisi                                                         |
| `daily_energy_mwh`          | Dari **EM-POI** saja; Mei 7+ sering **0** jika interval EM-POI hilang di `mart_meter_performance_5min` |


---

## 4. Tindakan teknis yang sudah dijalankan

```bash
cd dbt
dbt seed --select seed_meter_config seed_sensor_config
dbt run --select mart_sensor_daily --full-refresh
dbt run --select mart_site_performance_daily \
  --vars '{"reingest_start_date": "2026-04-01", "reingest_end_date": "2026-05-16"}'
```

### 4.1 Uji dual revenue (17 Mei malam) — **dibatalkan**

Percobaan menandai **EM-POI + EM-MVSWG** sebagai Revenue menaikkan `daily_energy_mwh` ke ~4–5 MWh/hari (rasio ~1,0 ke target) karena mart **menjumlahkan** kedua meter. Konfirmasi lapangan: **terlalu besar** — dikembalikan ke **EM-POI saja**, lalu `mart_site_performance_daily` di-refresh ulang Apr–16 Mei.

---

## 5. Tindak lanjut


| #   | Site   | Tindakan                                                            | Pemilik           |
| --- | ------ | ------------------------------------------------------------------- | ----------------- |
| 1   | Weiss  | Isi **kapasitas POA** Meteo Station 1 → `daily_poa_weighted` terisi | Ops / Engineering |
| 2   | Weiss  | Keputusan **sumber GHI** (jika PR GHI dibutuhkan)                   | Bisnis            |
| 3   | Gelora | Isi **kapasitas POA** per sensor EM011/021/031                      | Ops / Engineering |
| 4   | Gelora | Cek **ingest EM-POI** pasca 30 Apr 2026 (energi Mei)                | Ops / DE          |
| 5   | —      | **Jangan** seed Meteo Station 2 (`1789614_5_9_1`)                   | —                 |


---

## 6. Verifikasi SQL

```sql
-- Tipe sensor per device (Weiss & Gelora)
SELECT site_name, asset_id, sensor_type, COUNT(*) AS days
FROM mart.mart_sensor_daily
WHERE site_name IN ('PLTS Rooftop Weiss Tech', 'PT Gelora Djaja 1 MWp')
  AND date_key >= '2026-04-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Site performance ringkas
SELECT site_name,
       COUNT(*) FILTER (WHERE daily_energy_mwh > 0) AS days_energy,
       COUNT(*) FILTER (WHERE daily_ghi_kwh_m2 IS NOT NULL) AS days_ghi,
       COUNT(*) FILTER (WHERE daily_poa_weighted_kwh_m2 IS NOT NULL) AS days_poa
FROM mart.mart_site_performance_daily
WHERE site_name IN ('PLTS Rooftop Weiss Tech', 'PT Gelora Djaja 1 MWp')
  AND date_key BETWEEN '2026-04-01' AND '2026-05-16'
GROUP BY site_name;
```

---

*Dokumen ini menggantikan bagian Weiss/Gelora yang sudah tidak akurat di draft Implementation Plan (16 Mei) terkait sensor GHI Weiss dan revenue EM-MVSWG Gelora.*