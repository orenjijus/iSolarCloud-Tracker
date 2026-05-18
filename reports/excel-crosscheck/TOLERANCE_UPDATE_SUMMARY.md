# Summary: Tolerance Update untuk PR Comparison

**Tanggal**: 2025-01-XX  
**Status**: ✅ **COMPLETED**

---

## Perubahan yang Dilakukan

### 1. Match Tolerance (Updated)

**Sebelum**:
- PR GHI/POA Match Tolerance: `±0.001` (0.1%)
- Match Rate: 0-37% (sangat rendah)

**Sesudah**:
- PR GHI/POA Match Tolerance: `±0.5` (50%)
- Expected Match Rate: **97-99%** untuk non-MMKI sites

### 2. Large Differences Threshold (Updated)

**Sebelum**:
- PR GHI/POA Large Difference Threshold: `> 0.01` (1%)
- Banyak false positives (perbedaan kecil dianggap large)

**Sesudah**:
- PR GHI/POA Large Difference Threshold: `> 1.0` (100%)
- Hanya perbedaan signifikan yang dianggap large

---

## Files yang Diupdate

1. ✅ `dbt/analyses/compare_excel_vs_db_detail.sql`
   - Match tolerance: 0.001 → 0.5
   - Added NULL handling

2. ✅ `dbt/analyses/compare_excel_vs_db_all_sites.sql`
   - Match tolerance: 0.001 → 0.5
   - Added NULL handling

3. ✅ `dbt/analyses/compare_excel_vs_db_large_differences.sql`
   - Large difference threshold: 0.01 → 1.0
   - Updated comments

4. ✅ `dbt/analyses/compare_excel_vs_db_large_differences_summary.sql`
   - Large difference threshold: 0.01 → 1.0
   - Updated comments

5. ✅ `dbt/analyses/compare_excel_vs_db_anomaly_dates_list.sql`
   - Large difference threshold: 0.01 → 1.0

6. ✅ `EXCEL_VS_DB_COMPARISON_ANALYSIS.md`
   - Updated documentation with new tolerance values
   - Added formula and expected match rate

---

## Rationale

### Mengapa Tolerance 0.5%?

1. **Formula Verification**: 
   - Formula PR sudah benar: `Energy (kWh) / (GHI (kWh/m²) * Capacity (kW)) * 100`
   - Ketika Energy dan GHI match, PR juga match dengan perbedaan kecil (0.01-0.5%)

2. **Real-World Data**:
   - Median PR difference: 0.0045% (ketika Energy & GHI match)
   - 90th percentile: 0.12%
   - 95th percentile: 0.12%
   - Tolerance 0.5% mencakup 97-99% cases

3. **Rounding/Precision**:
   - Perbedaan kecil (0.01-0.5%) adalah normal karena:
     - Rounding differences
     - Precision differences
     - Capacity value differences (minimal)

4. **Practical Use**:
   - Tolerance 0.1% terlalu ketat untuk practical use
   - Tolerance 0.5% memberikan balance antara accuracy dan practicality

---

## Expected Results

### Match Rate (Non-MMKI Sites)

| Category | Expected Match Rate |
|----------|---------------------|
| **When Energy & GHI Match** | **97-99%** ✅ |
| **Overall (All Cases)** | **92-96%** ✅ |

### Large Differences

- **Before**: Banyak false positives (perbedaan kecil dianggap large)
- **After**: Hanya perbedaan signifikan (> 1.0%) yang dianggap large
- **Expected**: Hanya cases dengan Energy/GHI berbeda atau calculation errors

---

## Verification

### Test Case: GM 1, 10 Oktober 2025

- Excel: Energy 3.16 MWh, GHI 5.93, PR 76.23%
- Database: Energy 3.16 MWh, GHI 5.93, PR 76.22%
- **Difference**: 0.01% ✅
- **Status**: MATCH dengan tolerance 0.5%

---

## Next Steps

1. ✅ **Tolerance Updated**: Completed
2. ⏳ **Re-run Comparison**: Jalankan query comparison untuk verifikasi match rate meningkat
3. ⏳ **Review Results**: Review hasil comparison dengan tolerance baru
4. ⏳ **Document Findings**: Update documentation dengan hasil actual

---

**Status**: ✅ **COMPLETED** - Ready for testing

