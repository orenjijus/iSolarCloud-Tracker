# Inverter Site Mapping Implementation

**Date**: 2025-01-XX  
**Site**: Garuda Metalindo 1 (PS ID: 1458125)  
**Issue**: Inverter address replacement dan name correction

---

## Problem Statement

### Kasus Garuda Metalindo 1

1. **Old Inverters** (Inverter 101-106 tanpa titik):
   - Device IDs: `1458125_1_6_1`, `1458125_1_1_1`, `1458125_1_5_1`, `1458125_1_4_1`, `1458125_1_2_1`, `1458125_1_3_1`
   - Data period: 2024-10-26 sampai 2025-11-18
   - Masalah: Nama salah (harusnya Inverter.101-106 dengan titik)

2. **New Inverters** (Inverter.101-106 dengan titik):
   - Device IDs: `1458125_1_33_1`, `1458125_1_29_1`, `1458125_1_31_1`, `1458125_1_32_1`, `1458125_1_30_1`, `1458125_1_34_1`
   - Data period: 2025-07-02/03 sampai 2025-11-18
   - Status: Yang benar (6 inverter aktif)

3. **Overlap Period**: 2025-07-02/03 sampai 2025-11-18
   - Old dan new inverter sama-sama punya data
   - Perlu konsolidasi: data dari old ID di-map ke new ID

4. **Other Inverters** (Inverter103, Inverter7-8, Inverter16-29):
   - Device IDs: `1458125_1_27_1`, `1458125_1_25_1`, `1458125_1_26_1`, `1458125_1_35_1` sampai `1458125_1_48_1`
   - Data period: 2025-07-02/03 sampai 2025-11-18
   - Status: Tidak terpakai (kesalahan instalasi, sudah dihapus di platform)

---

## Solution: Inverter Site Mapping

### File: `dbt/seeds/seed_inverter_site_mapping.csv`

**Mapping Types:**
- `ID_CONSOLIDATION`: Map old device ID ke new device ID untuk konsolidasi data
- `NAME_OVERRIDE`: Override nama device untuk konsistensi

**Structure:**
```csv
mapping_type;device_id;logical_device_id;logical_device_name;effective_date_start;effective_date_end;notes
```

### Mapping Rules

1. **ID Consolidation** (Old → New):
   - `1458125_1_6_1` (Inverter 101) → `1458125_1_33_1` (Inverter.101) mulai 2025-07-03
   - `1458125_1_1_1` (Inverter 102) → `1458125_1_29_1` (Inverter.102) mulai 2025-07-02
   - `1458125_1_5_1` (Inverter 103) → `1458125_1_31_1` (Inverter.103) mulai 2025-07-03
   - `1458125_1_4_1` (Inverter 104) → `1458125_1_32_1` (Inverter.104) mulai 2025-07-03
   - `1458125_1_2_1` (Inverter 105) → `1458125_1_30_1` (Inverter.105) mulai 2025-07-02
   - `1458125_1_3_1` (Inverter 106) → `1458125_1_34_1` (Inverter.106) mulai 2025-07-03

2. **Name Override**:
   - Semua new inverter (Inverter.101-106) di-ensure namanya konsisten

---

## Implementation

### Updated Files

1. **`dbt/seeds/seed_inverter_site_mapping.csv`** (NEW)
   - Mapping configuration untuk ID consolidation dan name override

2. **`dbt/models/marts/mart_inverter_performance_5min.sql`** (UPDATED)
   - Added CTE `inverters_with_mapping` untuk apply mapping
   - Apply ID consolidation: old device_id → new device_id
   - Apply name override: ensure nama konsisten

3. **`dbt/dbt_project.yml`** (UPDATED)
   - Added configuration untuk `seed_inverter_site_mapping`

### Logic Flow

```
unified_inverters (raw data)
    ↓
inverters_with_metrics (add metric names)
    ↓
inverters_with_mapping (apply ID consolidation & name override)
    ↓
dim_assets join (get asset_id using consolidated device_ps_key)
    ↓
mart_inverter_performance_5min (final output)
```

### Mapping Application

**ID Consolidation:**
- Jika ada mapping `ID_CONSOLIDATION` dan tanggal dalam range `effective_date_start` - `effective_date_end`
- Ganti `device_ps_key` dari old ID ke new ID
- Data dari old ID akan di-consolidate ke new ID

**Name Override:**
- Jika ada mapping `NAME_OVERRIDE` atau `ID_CONSOLIDATION` dengan `logical_device_name`
- Ganti `device_name` dengan `logical_device_name`
- Ensure nama konsisten

---

## Expected Results

### Before Mapping:
- Old inverter (Inverter 101-106) muncul sebagai device terpisah
- New inverter (Inverter.101-106) muncul sebagai device terpisah
- Overlap period: data duplikat (old + new)

### After Mapping:
- Data dari old inverter (mulai 2025-07-02/03) di-consolidate ke new inverter
- Semua data untuk Inverter.101-106 menggunakan new device ID
- Nama konsisten: semua menggunakan format "Inverter.XXX"
- Site aggregation akan menggunakan 6 inverter yang benar

---

## Next Steps

1. **Load Seed**:
   ```bash
   dbt seed --select seed_inverter_site_mapping
   ```

2. **Re-run Model**:
   ```bash
   dbt run --select mart_inverter_performance_5min
   ```

3. **Verify Results**:
   - Check bahwa data old inverter sudah di-consolidate ke new inverter
   - Verify nama konsisten
   - Check site aggregation menggunakan 6 inverter yang benar

4. **Update Dependent Models** (if needed):
   - `mart_inverter_performance_daily`
   - `fact_site_calculations_5min` (if uses inverter data)
   - `mart_site_performance_daily` (if uses inverter data)

---

## Notes

- Mapping hanya berlaku untuk iSolarCloud system (dapat di-extend untuk FusionSolar jika needed)
- Date-aware: mapping hanya berlaku dalam range `effective_date_start` - `effective_date_end`
- Old inverter data sebelum `effective_date_start` tetap menggunakan old ID (tidak di-consolidate)
- Other inverters (Inverter103, Inverter7-8, Inverter16-29) tidak di-mapping karena tidak terpakai

---

**Last Updated**: 2025-01-XX  
**Status**: Implementation completed, waiting for seed load and model re-run

