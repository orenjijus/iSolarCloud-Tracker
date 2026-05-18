# Shoetown Sensor Repositioning Summary

**Date**: 2025-01-XX  
**Site**: Shoetown Ligung Indonesia (1479456)  
**Status**: ✅ Config Updated

---

## 📋 Key Understanding

**Important**: Semua sensor reposisi menggunakan **sensor fisik yang sama**, hanya dipindahkan posisinya. Oleh karena itu:
- **Capacity tetap sama** (452.4 kWp untuk semua sensor POA yang direposisi)
- Yang berubah hanya **posisi fisik** dan **device_id** di sistem

---

## 🔄 Sensor Repositioning Details

### 1. SLI-IRR-1-Aold → SLI-IRR-1-A

| Aspect | Old Sensor | New Sensor |
|--------|------------|------------|
| **Device ID** | `1479456_5_16_2` | `1479456_5_24_1` |
| **Device Name** | SLI-IRR-1-Aold | SLI-IRR-1-A |
| **Capacity** | 452.4 kWp | 452.4 kWp ✅ (sensor fisik sama) |
| **Deactivated** | Oct 1, 2025 | - |
| **Activated** | - | Oct 3, 2025 |
| **Note** | Sensor fisik sama, hanya reposisi | - |

### 2. SLI-IRR-2-Aold → SLI-IRR-2-A

| Aspect | Old Sensor | New Sensor |
|--------|------------|------------|
| **Device ID** | `1479456_5_15_2` | `1479456_5_25_1` |
| **Device Name** | SLI-IRR-2-Aold | SLI-IRR-2-A |
| **Capacity** | 452.4 kWp | 452.4 kWp ✅ (sensor fisik sama) |
| **Deactivated** | Oct 1, 2025 | - |
| **Activated** | - | Oct 3, 2025 |
| **Note** | Sensor fisik sama, hanya reposisi | - |

### 3. SLI-IRR-3-F → Meteo Station16

| Aspect | Old Sensor | New Sensor |
|--------|------------|------------|
| **Device ID** | `1479456_5_17_1` | `1479456_5_27_1` |
| **Device Name** | SLI-IRR-3-F | Meteo Station16 |
| **Capacity** | 928 kWp | 928 kWp ✅ (sensor fisik sama) |
| **Repositioned** | Oct 3, 2025 | Oct 3, 2025 |
| **Grid Connection Date** | 2024-11-26 | 2025-11-12 (updated later) |
| **Note** | Sensor fisik sama, hanya reposisi | - |

### 4. SLI-PYR-01-F → SLI-PYR (Pyrano)

| Aspect | Old Sensor | New Sensor |
|--------|------------|------------|
| **Device ID** | `1479456_5_21_1` | `1479456_5_26_1` |
| **Device Name** | SLI-PYR-01-F | SLI-PYR |
| **Type** | GHI | GHI |
| **Dicabut** | Oct 3, 2025 | - |
| **Nyala Lagi** | - | Oct 12, 2025 |
| **Gap Period** | - | Oct 4-11, 2025 (no GHI data) |
| **Note** | Pyrano dicabut Oct 3, baru nyala lagi Oct 12 | - |

---

## ✅ Config Updates

### Sensor Config (`seed_sensor_config.csv`)

Semua sensor sudah diupdate dengan capacity yang benar:

```csv
iSolarCloud;1479456;SLI-IRR-1-Aold;1479456_5_16_2;POA;452.4
iSolarCloud;1479456;SLI-IRR-2-Aold;1479456_5_15_2;POA;452.4
iSolarCloud;1479456;SLI-IRR-1-A;1479456_5_24_1;POA;452.4
iSolarCloud;1479456;SLI-IRR-2-A;1479456_5_25_1;POA;452.4
iSolarCloud;1479456;SLI-IRR-3-F;1479456_5_17_1;POA;928
iSolarCloud;1479456;SLI-IRR-4-F;1479456_5_18_1;POA;763.28
iSolarCloud;1479456;Meteo Station16;1479456_5_27_1;POA;928
iSolarCloud;1479456;SLI-PYR-01-F;1479456_5_21_1;GHI;
iSolarCloud;1479456;SLI-PYR;1479456_5_26_1;GHI;
```

---

## 📅 Timeline Summary

| Date | Event |
|------|-------|
| **Oct 1, 2025** | SLI-IRR-1-Aold dan SLI-IRR-2-Aold deactivated |
| **Oct 3, 2025** | SLI-IRR-1-A dan SLI-IRR-2-A activated (reposisi) |
| **Oct 3, 2025** | SLI-IRR-3-F reposisi → Meteo Station16 |
| **Oct 3, 2025** | Pyrano (SLI-PYR-01-F) dicabut |
| **Oct 4-11, 2025** | Gap period - no GHI data |
| **Oct 12, 2025** | Pyrano (SLI-PYR) nyala lagi |
| **Nov 12, 2025** | Meteo Station16 grid connection date (updated later) |

---

## 🎯 Key Points

1. **Sensor Fisik Sama**: Semua sensor reposisi menggunakan sensor fisik yang sama, hanya dipindahkan posisinya
2. **Capacity per Sensor**:
   - SLI-IRR-1-A dan SLI-IRR-2-A: 452.4 kWp (sensor fisik sama dengan old sensors)
   - SLI-IRR-3-F dan Meteo Station16: 928 kWp (sensor fisik sama, hanya reposisi)
   - SLI-IRR-4-F: 763.28 kWp (tidak direposisi)
3. **Pyrano Gap**: Ada gap 8 hari (Oct 4-11) tanpa GHI data karena pyrano dicabut Oct 3 dan baru nyala lagi Oct 12
4. **Grid Connection Date**: Meteo Station16 punya grid_connection_date Nov 12, tapi reposisi terjadi Oct 3 (date diupdate later)

---

## 📋 Next Steps

1. [x] ✅ Update sensor config dengan capacity yang benar (452.4 kWp)
2. [x] ✅ Update Meteo Station16 ke config
3. [x] ✅ Update pyrano activation date (Oct 12, bukan Oct 13)
4. [ ] Reload seed: `dbt seed --select seed_sensor_config`
5. [ ] Re-run models: `dbt run --select mart_sensor_daily mart_site_performance_daily`
6. [ ] Validate POA calculation with Excel

---

**Last Updated**: 2025-01-XX  
**Status**: Config updated, ready for validation

