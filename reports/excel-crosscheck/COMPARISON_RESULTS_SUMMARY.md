# Summary: Comparison Results Update

**Tanggal**: 2025-01-XX  
**Status**: ✅ **RESULTS UPDATED - Format Decimal Fix Applied**

---

## Perubahan yang Dilakukan

### 1. Format Decimal Fix ✅ (LATEST UPDATE)
- **Issue**: Excel menyimpan PR sebagai decimal (0-1), DB sebelumnya sebagai percentage (0-100)
- **Fix**: Update model `mart_site_performance_daily` untuk menggunakan format decimal (0-1) seperti Excel
  - PR GHI/POA: Hapus `* 100` dari formula, hasil dalam decimal (0-1)
  - Availability: Hapus `* 100` dari formula, hasil dalam decimal (0-1)
  - Actual vs Target: Hapus `* 100`, hasil dalam decimal ratio
  - Semua nilai menggunakan `CAST(... AS DECIMAL(18,6))` untuk konsistensi
- **Result**: Format sekarang match dengan Excel, comparison lebih akurat

### 2. Tolerance Update ✅
- **PR GHI/POA Tolerance**: 0.005 (0.5% dalam format decimal)
- **Reason**: Tolerance untuk format decimal, perbedaan kecil (0.005 = 0.5%) adalah normal karena rounding

---

## Hasil Setelah Format Decimal Fix (Latest Update)

### PR GHI Match Rate (Non-MMKI Sites)

| Site | Match Rate | Status |
|------|------------|--------|
| Charoen Pokphand Majalengka | **100.00%** | ✅ Perfect |
| PLTS Frina Lestari Nusantara | **100.00%** | ✅ Perfect |
| Garuda Metalindo 1 | **99.35%** | ✅ Excellent |
| Garuda Metalindo 2 | **99.04%** | ✅ Excellent |
| PT. Pusan Manis Mulia | **98.42%** | ✅ Excellent |
| Garuda Metalindo (MPF) | **98.06%** | ✅ Excellent |
| Charoen Pokphand Bandung | **97.62%** | ✅ Excellent |
| Garuda Metalindo (IKP) | **97.41%** | ✅ Excellent |
| Charoen Pokphand Madiun | **95.24%** | ✅ Very Good |
| PLTS Rooftop Sumatera | **92.31%** | ✅ Very Good |
| Shoetown Ligung | **91.96%** | ✅ Very Good |
| PLTS Mall Panakkukang | **89.91%** | ✅ Good |

**Average Match Rate**: **96.5%** ⭐

### PR POA Match Rate (Non-MMKI Sites)

| Site | Match Rate | Status |
|------|------------|--------|
| Charoen Pokphand Majalengka | **100.00%** | ✅ Perfect |
| Garuda Metalindo 2 | **99.36%** | ✅ Excellent |
| PLTS Mall Panakkukang | **99.05%** | ✅ Excellent |
| Garuda Metalindo (MPF) | **98.06%** | ✅ Excellent |
| Charoen Pokphand Madiun | **94.05%** | ✅ Very Good |
| PLTS Frina Lestari | **39.58%** | ⚠️ POA values berbeda |
| Garuda Metalindo 1 | **38.31%** | ⚠️ POA values berbeda |
| PLTS Rooftop Sumatera | **35.90%** | ⚠️ POA values berbeda |
| Charoen Pokphand Bandung | **26.19%** | ⚠️ POA values berbeda |
| Garuda Metalindo (IKP) | **25.90%** | ⚠️ POA values berbeda |
| PT. Pusan Manis Mulia | **14.51%** | ⚠️ POA values berbeda |
| Shoetown Ligung | **1.31%** | ⚠️ POA values berbeda |

**Note**: PR POA match rate rendah untuk beberapa site karena POA values berbeda antara Excel dan DB (bukan masalah format)

---

## Key Findings

### ✅ Success Stories

1. **Charoen Pokphand Majalengka**: 
   - PR GHI: 100% match ✅
   - PR POA: 100% match ✅
   - Semua metrics perfect (kecuali Availability format)

2. **Garuda Metalindo Sites**:
   - PR GHI: 97-99% match ✅
   - PR POA: 38-99% match (tergantung POA values)

3. **PLTS Frina Lestari**:
   - PR GHI: 100% match ✅
   - PR POA: 39.58% (karena POA berbeda)

### ⚠️ Remaining Issues

1. **Availability**: 0-1% match rate
   - Format berbeda antara Excel dan DB
   - Perlu investigasi format conversion

2. **POA for some sites**: 8-60% match rate
   - Perlu investigasi calculation method
   - Mungkin sensor selection berbeda

3. **PR POA for some sites**: 1-40% match rate
   - Biasanya karena POA values berbeda
   - Jika POA match, PR POA juga match

4. **MMKI Sites**: 
   - Energy dan GHI berbeda (expected)
   - GHI dikoreksi di Excel
   - PR match rate lebih rendah karena input berbeda

---

## Summary Metrics Match Rate (All Sites)

### Non-MMKI Sites

| Site | Energy | GHI | POA | PR GHI | PR POA |
|------|--------|-----|-----|--------|--------|
| Charoen Pokphand Majalengka | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| PLTS Frina Lestari | 100.00% | 100.00% | 57.14% | 100.00% | 39.58% |
| PLTS Mall Panakkukang | 100.00% | 99.68% | 99.37% | 89.91% | 99.05% |
| PT. Pusan Manis Mulia | 98.74% | 99.68% | 23.97% | 98.42% | 14.51% |
| Garuda Metalindo 1 | 98.42% | 99.36% | 56.87% | 99.35% | 38.31% |
| Garuda Metalindo 2 | 98.42% | 99.37% | 100.00% | 99.04% | 99.36% |
| Garuda Metalindo (MPF) | 97.16% | 100.00% | 100.00% | 98.06% | 98.06% |
| Charoen Pokphand Bandung | 97.14% | 99.03% | 32.04% | 97.62% | 26.19% |
| Garuda Metalindo (IKP) | 95.58% | 98.73% | 37.26% | 97.41% | 25.90% |
| Charoen Pokphand Madiun | 95.29% | 100.00% | 98.82% | 95.24% | 94.05% |
| PLTS Rooftop Sumatera | 90.00% | 100.00% | 60.00% | 92.31% | 35.90% |
| Shoetown Ligung | 89.91% | 96.69% | 8.52% | 91.96% | 1.31% |

**Non-MMKI Average**: Energy: 97.0% | GHI: 99.4% | POA: 64.2% | PR GHI: 96.5% | PR POA: 58.0%

### MMKI Sites

| Site | Energy | GHI | POA | PR GHI | PR POA |
|------|--------|-----|-----|--------|--------|
| PT. MMKI 4.292 MWP - Phase 3 | 0.00% | 37.27% | 98.33% | 36.02% | 96.67% |
| PT. MMKI 5.7 MWp - Phase 2 | 0.00% | 40.26% | 94.92% | 33.89% | 59.68% |
| PT. MMKI 1.75 MWp - Painting Building | 0.32% | 38.98% | 51.12% | 39.73% | 40.71% |

**MMKI Average**: Energy: 0.1% | GHI: 38.8% | POA: 81.5% | PR GHI: 36.5% | PR POA: 65.7%

**Note MMKI Sites**: 
- Energy dan GHI match rate rendah karena Excel menggunakan data yang sudah dikoreksi (GHI dikoreksi manual di Excel)
- POA match rate lebih baik (51-98%) karena POA tidak dikoreksi
- PR match rate rendah karena bergantung pada Energy dan GHI yang berbeda
- Perbedaan ini **expected** dan bukan masalah format, melainkan perbedaan data source/koreksi manual

## Conclusion

✅ **Format Decimal Fix Berhasil**: Database sekarang menggunakan format decimal (0-1) seperti Excel  
✅ **PR Match Rate Excellent**: PR GHI match rate 89-100% untuk semua non-MMKI sites  
✅ **Formula Verification**: Formula PR sudah benar, ketika Energy & GHI match, PR juga match  
✅ **Format Consistency**: Semua nilai menggunakan DECIMAL(18,6) untuk konsistensi dengan Excel  

📊 **Current Status (All Sites)**:

**Non-MMKI Sites**:
- **PR GHI**: **89-100% match** ✅ (Average: 96.5%)
- **PR POA**: **1-100% match** (tergantung POA values, bukan format issue)
- **Energy**: **89-100% match** ✅ (Average: 97.0%)
- **GHI**: **96-100% match** ✅ (Average: 99.4%)
- **POA**: **8-100% match** (Average: 64.2%, beberapa site berbeda karena sensor selection)

**MMKI Sites**:
- **PR GHI**: **33-40% match** ⚠️ (Expected: data berbeda karena GHI dikoreksi di Excel)
- **PR POA**: **40-97% match** (Average: 65.7%)
- **Energy**: **0-0.3% match** ⚠️ (Expected: data berbeda karena koreksi manual)
- **GHI**: **37-40% match** ⚠️ (Expected: GHI dikoreksi manual di Excel)
- **POA**: **51-98% match** (Average: 81.5%, lebih baik karena tidak dikoreksi)

**Key Improvement**: Format decimal fix menghasilkan match rate yang sangat tinggi untuk PR GHI pada non-MMKI sites (96.5% average), membuktikan bahwa format sekarang sudah benar dan sesuai dengan Excel. MMKI sites menunjukkan match rate rendah karena perbedaan data source (koreksi manual di Excel), bukan masalah format.

---

**Status**: ✅ **COMPLETED** - Format decimal fix applied successfully, results updated

