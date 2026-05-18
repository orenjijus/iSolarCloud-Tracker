# Excel Template untuk Validasi POA Calculation

## Format Excel yang Disarankan

### Struktur Sheet 1: Daily Summary (Main Comparison)

| Column | Header | Format | Description |
|--------|--------|--------|-------------|
| A | Date | Date (YYYY-MM-DD) | Tanggal |
| B | Site | Text | Nama Site |
| C | **Weighted_Avg_POA** | Number (6 decimals) | **Nilai utama untuk dibandingkan** |
| D | Sum_POA_x_Capacity | Number (2 decimals) | Sum dari (POA × Capacity) |
| E | Sum_Capacity | Number (2 decimals) | Sum dari Capacity |
| F | Sensor_Count | Integer | Jumlah sensor |
| G | IRR_NE_B | Number (6 decimals) | POA device IRR-NE-B |
| H | IRR_NW_A | Number (6 decimals) | POA device IRR-NW-A |
| I | IRR_SE_A | Number (6 decimals) | POA device IRR-SE-A |
| J | IRR_SW_B | Number (6 decimals) | POA device IRR-SW-B |
| K | Min_POA | Number (6 decimals) | POA minimum |
| L | Max_POA | Number (6 decimals) | POA maximum |

### Struktur Sheet 2: Per Device Detail (For Verification)

| Column | Header | Format | Description |
|--------|--------|--------|-------------|
| A | Date | Date | Tanggal |
| B | Site | Text | Nama Site |
| C | Device | Text | Nama Device |
| D | Asset_ID | Text | Asset ID |
| E | POA_kWh_m2 | Number (6 decimals) | POA per device |
| F | Capacity_kWp | Number (2 decimals) | Capacity per device |
| G | Weighted_Contribution | Number (2 decimals) | POA × Capacity |

---

## Langkah-langkah Setup Excel

### Step 1: Copy Data dari Query

1. Jalankan query `export_poa_excel_format.sql`
2. Select all results (Ctrl+A)
3. Copy (Ctrl+C)

### Step 2: Paste ke Excel

1. Buka Excel baru
2. Klik cell A1
3. Paste (Ctrl+V)
4. Excel akan auto-detect format

### Step 3: Format Excel Sheet

#### Sheet 1: Daily Summary

**Header Row (Row 1):**
- Format: Bold, Background Color (Light Blue)
- Freeze Panes: Row 2

**Data Columns:**
- Column A (Date): Format as Date (YYYY-MM-DD)
- Column C (Weighted_Avg_POA): **Highlight in Yellow** - ini yang dibandingkan
- Columns G-J (Device POA): Format as Number, 6 decimals
- Columns D-E: Format as Number, 2 decimals

**Add Validation Columns:**

| Column | Formula | Purpose |
|--------|---------|---------|
| M | `=D2/E2` | Manual Weighted Avg (untuk cross-check) |
| N | `=C2-M2` | Difference (DB vs Excel) |
| O | `=ABS(N2)` | Absolute Difference |
| P | `=IF(O2<0.001, "OK", "CHECK")` | Validation Flag |

#### Sheet 2: Per Device Detail

**Create Pivot Table:**
1. Select all data in Sheet 2
2. Insert → Pivot Table
3. Rows: Date, Device
4. Values: 
   - Sum of POA_kWh_m2 (Average)
   - Sum of Capacity_kWp (Sum)
   - Sum of Weighted_Contribution (Sum)

---

## Excel Formula untuk Validasi

### Formula 1: Manual Weighted Average Calculation

```excel
=SUMPRODUCT(POA_range, Capacity_range) / SUM(Capacity_range)
```

**Example:**
```excel
=SUMPRODUCT(G2:J2, Capacity_G2:J2) / SUM(Capacity_G2:J2)
```

### Formula 2: Difference Check

```excel
=ABS(Weighted_Avg_POA_DB - Weighted_Avg_POA_Excel)
```

**Example:**
```excel
=ABS(C2 - M2)
```

### Formula 3: Validation Flag

```excel
=IF(ABS(Difference) < 0.001, "OK", "CHECK")
```

**Example:**
```excel
=IF(ABS(C2 - M2) < 0.001, "OK", "CHECK")
```

### Formula 4: Percentage Difference

```excel
=ABS((DB_Value - Excel_Value) / Excel_Value) * 100
```

**Example:**
```excel
=ABS((C2 - M2) / M2) * 100
```

---

## Template Excel Structure

```
┌─────────────────────────────────────────────────────────────┐
│ Sheet 1: Daily Summary                                       │
├─────────────────────────────────────────────────────────────┤
│ Date | Site | Weighted_Avg | Sum_POA×Cap | ... | Validation │
├─────────────────────────────────────────────────────────────┤
│ 2025-11-01 | GM1 | 5.322553 | 326905.87 | ... | OK          │
│ 2025-11-02 | GM1 | 5.722513 | 351471.05 | ... | OK          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Sheet 2: Per Device Detail                                  │
├─────────────────────────────────────────────────────────────┤
│ Date | Device | POA | Capacity | Weighted_Contribution      │
├─────────────────────────────────────────────────────────────┤
│ 2025-11-01 | IRR-NE-B | 5.048479 | 31211 | 157568.08        │
│ 2025-11-01 | IRR-NW-A | 5.467033 | 10148 | 55479.45         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Sheet 3: Device Capacity Reference                         │
├─────────────────────────────────────────────────────────────┤
│ Device | Asset_ID | Capacity_kWp | Notes                   │
├─────────────────────────────────────────────────────────────┤
│ IRR-NE-B | ISO_1458125_5_21_1 | 31211 | ...                │
│ IRR-NW-A | ISO_1458125_5_18_1 | 10148 | ...                │
└─────────────────────────────────────────────────────────────┘
```

---

## Tips untuk Validasi

1. **Use Conditional Formatting:**
   - Highlight cells where difference > 0.001 in red
   - Highlight cells where difference < 0.001 in green

2. **Create Summary Dashboard:**
   - Total days checked
   - Days with difference > 0.001
   - Average difference
   - Max difference

3. **Device-by-Device Analysis:**
   - Use Pivot Table to see which device has consistent differences
   - Check if specific device always reads higher/lower

4. **Date Range Analysis:**
   - Group by week to see patterns
   - Check if differences are consistent or random

---

## Quick Start Checklist

- [ ] Run query `export_poa_excel_format.sql` with your site name
- [ ] Copy results to Excel Sheet 1
- [ ] Format columns (Date, Number with correct decimals)
- [ ] Add validation formulas (Column M, N, O, P)
- [ ] Copy per-device data to Sheet 2
- [ ] Create Pivot Table in Sheet 2
- [ ] Add Device Capacity Reference in Sheet 3
- [ ] Apply conditional formatting for differences
- [ ] Compare with your Excel file

---

## Example Excel Formulas

### In Column M (Manual Weighted Avg):
```excel
=SUMPRODUCT(G2:J2, $G$100:$J$100) / SUM($G$100:$J$100)
```
*Note: Row 100 contains device capacities (create reference table)*

### In Column N (Difference):
```excel
=C2-M2
```

### In Column O (Absolute Difference):
```excel
=ABS(N2)
```

### In Column P (Validation):
```excel
=IF(O2<0.001, "✓ OK", "✗ CHECK")
```

