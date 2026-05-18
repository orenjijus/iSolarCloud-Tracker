# Excel Validation — Format & Template

Panduan validasi data POA/weighted average antara database dan Excel: format yang disarankan, masalah umum (capacity), dan template struktur Excel. Digabung dari: EXCEL_FORMAT_VALIDATION_GUIDE, EXCEL_VALIDATION_TEMPLATE.

---

## 1. Format Validation Guide

### 1.1 Masalah umum: Capacity

**Contoh (Shoetown)**: Capacity 76,328 kWp di DB seharusnya 763.28 kWp (100x). Penyebab: seed pakai koma desimal (763,28), parsing koma salah. Dampak: weighted average DB salah.

**Perbaikan**:
1. Cek seed (`dbt/seeds/seed_sensor_config.csv`) — pastikan nilai pakai koma atau format benar.
2. `dbt seed --select seed_sensor_config`
3. `dbt run --select mart_sensor_daily`
4. Validasi ulang dengan Excel.

### 1.2 Format Excel yang aman

- Column names: underscore (SLI_IRR_1_A) — OK  
- Date: DD/MM/YYYY atau YYYY-MM-DD — OK  
- Weighted_Avg_POA: number, 6–9 decimals — OK  
- Pastikan capacity di Excel sama dengan seed (763.28, bukan 76328).

### 1.3 Kolom validasi (opsional)

| Kolom | Formula | Tujuan |
|-------|---------|--------|
| Manual Weighted Avg | `=SUMPRODUCT(POA_range, Capacity_range)/SUM(Capacity_range)` | Cross-check |
| Difference | `=DB_Value - Excel_Value` | Selisih |
| Validation | `=IF(ABS(Diff)<0.001, "✓ OK", "✗ CHECK")` | Flag |

---

## 2. Template Struktur Excel

### 2.1 Sheet 1: Daily Summary (Main Comparison)

| Column | Header | Format | Description |
|--------|--------|--------|-------------|
| A | Date | Date (YYYY-MM-DD) | Tanggal |
| B | Site | Text | Nama Site |
| C | **Weighted_Avg_POA** | Number (6 decimals) | Nilai utama untuk dibandingkan |
| D | Sum_POA_x_Capacity | Number (2 decimals) | Sum(POA × Capacity) |
| E | Sum_Capacity | Number (2 decimals) | Sum Capacity |
| F | Sensor_Count | Integer | Jumlah sensor |
| G–J | Device POA columns | Number (6 decimals) | POA per device |
| K–L | Min_POA, Max_POA | Number (6 decimals) | Min/Max POA |

**Validation columns**: Manual Weighted Avg (M), Difference (N), Abs Diff (O), Flag (P): `=IF(O2<0.001, "✓ OK", "✗ CHECK")`.

### 2.2 Sheet 2: Per Device Detail

| Column | Header | Format |
|--------|--------|--------|
| A | Date | Date |
| B | Site | Text |
| C | Device | Text |
| D | Asset_ID | Text |
| E | POA_kWh_m2 | Number (6 decimals) |
| F | Capacity_kWp | Number (2 decimals) |
| G | Weighted_Contribution | Number (2 decimals) |

### 2.3 Sheet 3 (opsional): Device Capacity Reference

Device | Asset_ID | Capacity_kWp | Notes — untuk referensi SUMPRODUCT.

---

## 3. Langkah Validasi

1. **Export dari DB**: Jalankan query export POA (sesuai site); copy hasil ke Excel Sheet 1.
2. **Format**: Date, number decimals, bold header, freeze row 1.
3. **Tambahkan kolom validasi**: M = manual weighted avg, N = C−M, O = ABS(N), P = IF(O<0.001,"OK","CHECK").
4. **Per device**: Paste detail ke Sheet 2; pivot jika perlu.
5. **Conditional formatting**: Hijau jika diff < 0.001, merah jika > 0.001.

---

## 4. Quick Start Checklist

- [ ] Jalankan query export POA untuk site
- [ ] Copy ke Sheet 1, format kolom
- [ ] Tambah kolom validasi (M, N, O, P)
- [ ] Bandingkan Weighted_Avg_POA DB vs Excel
- [ ] Jika capacity salah: perbaiki seed, reload seed, re-run mart_sensor_daily

---

*Dokumen master Excel validation. File asli: EXCEL_FORMAT_VALIDATION_GUIDE, EXCEL_VALIDATION_TEMPLATE — diarsipkan di docs/archive/.*
