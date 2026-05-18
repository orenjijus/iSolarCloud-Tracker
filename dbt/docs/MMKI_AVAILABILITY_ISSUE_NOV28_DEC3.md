# Issue: MMKI 2 & 3 Availability = 0 dari 28 November - 3 Desember 2025

## Ringkasan Masalah

Availability untuk MMKI 2 dan MMKI 3 bernilai 0 (atau NULL) untuk periode 28 November - 3 Desember 2025.

## Root Cause Analysis

### 0. UPDATE: POA Override Sudah Diterapkan di fact_sensor_calculations_5min

**Perubahan**: POA override logic sekarang sudah diterapkan di `fact_sensor_calculations_5min.sql` (Step 3: `poa_with_override`).

**Catatan Penting**: 
- POA override di `seed_sensor_site_mapping.csv` memiliki `effective_date_start = 2025-09-15`
- Untuk periode 2025-11-28 sampai 2025-12-03, override **sudah efektif** (karena 2025-11-28 > 2025-09-15)
- Jika availability masih 0, kemungkinan POA data tidak ada atau ter-filter oleh validation logic

### 1. Alur Perhitungan Availability

Availability dihitung melalui chain berikut:
1. `mart_sensor_measurements_5min` → Data sensor raw (POA/GHI)
2. `fact_sensor_calculations_5min` → Hitung MIT (Minimum Irradiance Threshold)
3. `fact_inverter_calculations_5min` → Join inverter power dengan MIT (INNER JOIN)
4. `fact_site_calculations_5min` → Aggregate ke site level
5. `mart_site_performance_daily` → Aggregate ke daily dan hitung availability

### 2. Masalah di Chain

**Critical Point**: `fact_inverter_calculations_5min` menggunakan **INNER JOIN** antara inverter power dan MIT data.

```sql
FROM inverter_power ip
INNER JOIN site_mit sm  -- INNER JOIN: only count when BOTH inverter and MIT data exist
    ON ip.timestamp = sm.timestamp
    AND ip.site_id = sm.site_id
```

Ini berarti:
- Jika **tidak ada MIT data** untuk suatu timestamp, maka **tidak ada row** di `fact_inverter_calculations_5min`
- Jika tidak ada row di `fact_inverter_calculations_5min`, maka tidak ada data di `fact_site_calculations_5min`
- Jika tidak ada data di `fact_site_calculations_5min`, maka availability = NULL atau 0

### 3. Kenapa MIT Data Tidak Ada?

**Root Cause**: POA sensors untuk MMKI 2 & 3 secara fisik tersimpan di MMKI I, tapi secara logis milik MMKI 2 & 3. **POA override belum diterapkan di `fact_sensor_calculations_5min.sql`** (sebelum fix ini).

**Sebelum Fix**:
- POA sensors masih ter-assign ke MMKI I (physical site) di `fact_sensor_calculations_5min`
- Ketika mencari POA untuk MMKI 2 & 3, tidak ketemu karena POA masih di-assign ke MMKI I
- Akibatnya tidak ada MIT data untuk MMKI 2 & 3
- Availability = 0

**Setelah Fix**:
- POA override sudah diterapkan di `fact_sensor_calculations_5min.sql` (Step 3: `poa_with_override`)
- POA sensors sekarang ter-assign ke logical site (MMKI 2 & 3) berdasarkan `seed_sensor_site_mapping.csv`
- **Catatan**: Override hanya berlaku jika `effective_date_start` sudah tercapai atau NULL
- Untuk periode 2025-11-28 sampai 2025-12-03, override **sudah efektif** (karena 2025-11-28 > 2025-09-15)

**Kesimpulan**: MMKI 2 & 3 **HARUS** pakai POA sensor mereka sendiri untuk MIT calculation, dengan POA override diterapkan.

### 4. Kemungkinan Penyebab

Jika availability = 0 untuk periode 28 Nov - 3 Des, kemungkinan besar:

1. **POA sensor data tidak ada** untuk periode tersebut
   - Sensor tidak mengirim data
   - Data tidak di-fetch dari iSolarCloud
   - Data tidak di-ingest ke database

2. **POA sensor data ada tapi invalid** (ter-filter oleh validation logic)
   - Sensor stuck (nilai sama terus)
   - Nilai tidak realistis (terlalu tinggi/rendah)

## Diagnosis

Jalankan query diagnostik:

```bash
cd dbt
dbt compile --select debug_mmki_availability_nov28_dec3
```

Atau langsung query di database:

```sql
-- Lihat file: dbt/analyses/debug_mmki_availability_nov28_dec3.sql
```

Query ini akan menampilkan:
1. Apakah ada inverter data untuk periode tersebut
2. Apakah ada POA sensor data untuk periode tersebut
3. Apakah ada MIT data di `fact_sensor_calculations_5min`
4. Apakah ada data di `fact_inverter_calculations_5min`
5. Apakah ada data di `fact_site_calculations_5min`
6. Status availability di `mart_site_performance_daily`

## Solusi

### Solusi 1: Pastikan POA Override Diterapkan dengan Benar (Recommended)

1. **Check apakah POA override effective_date_start sudah sesuai**:
   ```sql
   SELECT 
       mapping_type,
       device_id,
       logical_site_id,
       effective_date_start,
       effective_date_end
   FROM "MMSR"."mart"."seed_sensor_site_mapping"
   WHERE mapping_type = 'POA_OVERRIDE'
       AND logical_site_id IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3');
   ```

2. **Check apakah POA sensor data sudah di-fetch**:
   ```sql
   SELECT 
       date_key,
       site_name,
       COUNT(*) as records,
       MIN(timestamp) as first_ts,
       MAX(timestamp) as last_ts
   FROM "MMSR"."mart"."mart_sensor_measurements_5min"
   WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
       AND metric_name = 'irradiance'
       AND date_key >= '2025-11-28'
       AND date_key <= '2025-12-03'
   GROUP BY date_key, site_name;
   ```

2. **Jika data tidak ada, fetch ulang dari iSolarCloud**:
   - Update `fetch_historical_device_data.py` untuk periode tersebut
   - Atau jalankan manual fetch untuk periode 28 Nov - 3 Des

3. **Re-run dbt models**:
   ```bash
   cd dbt
   dbt run --select fact_sensor_calculations_5min+ --vars '{"reingest_start_date": "2025-11-28", "reingest_end_date": "2025-12-03"}'
   ```

### Solusi 2: Enable GHI Fallback (Jika POA Sensor Bermasalah)

Jika POA sensor memang bermasalah untuk periode tersebut, bisa pertimbangkan untuk enable GHI fallback:

1. **Hapus atau comment logic skip GHI fallback** di `fact_sensor_calculations_5min.sql` (line 173-186)
2. **Re-run dbt models**
3. **Catatan**: GHI MMKI I mungkin bermasalah, jadi perlu validasi hasilnya

### Solusi 3: Temporary Fix - Set Availability Manual

Jika data memang tidak tersedia dan tidak bisa di-fetch, bisa set availability manual:

```sql
UPDATE "MMSR"."mart"."mart_site_performance_daily"
SET availability_percent = <nilai_yang_diinginkan>
WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
    AND date_key >= '2025-11-28'
    AND date_key <= '2025-12-03'
    AND availability_percent IS NULL;
```

**Catatan**: Ini hanya temporary fix, solusi yang benar adalah memastikan data sensor tersedia.

## Prevention

Untuk mencegah issue serupa di masa depan:

1. **Monitoring**: Set up alert jika availability = 0 atau NULL untuk site tertentu
2. **Data Quality Check**: Validasi bahwa POA/GHI sensor data tersedia sebelum hitung availability
3. **Documentation**: Dokumentasikan dependency antara sensor data dan availability calculation

## Referensi

- `dbt/models/facts/fact_sensor_calculations_5min.sql` - MIT calculation logic
- `dbt/models/facts/fact_inverter_calculations_5min.sql` - Inverter availability dengan MIT
- `dbt/models/facts/fact_site_calculations_5min.sql` - Site-level aggregation
- `dbt/models/marts/mart_site_performance_daily.sql` - Daily availability calculation
- `dbt/seeds/seed_sensor_site_mapping.csv` - GHI fallback configuration

