# Analisis Inverter Garuda Metalindo 1

**Date**: 2025-01-XX  
**Site**: Garuda Metalindo 1 (PS ID: 1458125)  
**Analysis**: Status dan Data Availability untuk 12 Inverter

---

## 📊 Tabel 1: Status dan Data Availability Semua 12 Inverter

| No | Device Name | Device ID | Type | rel_state | Status | First Data | Last Data | Days with Data | Total Records |
|----|-------------|-----------|------|-----------|--------|------------|-----------|----------------|---------------|
| **OLD INVERTERS (tanpa titik)** |
| 1 | Inverter 101 | 1458125_1_6_1 | OLD | **1** | **ACTIVE** | 2024-10-26 | 2025-11-18 | 387 | 344,241 |
| 2 | Inverter 102 | 1458125_1_1_1 | OLD | 0 | INACTIVE | 2024-10-26 | 2025-11-18 | 387 | 309,031 |
| 3 | Inverter 103 | 1458125_1_5_1 | OLD | **1** | **ACTIVE** | 2024-10-26 | 2025-11-18 | 387 | 343,977 |
| 4 | Inverter 104 | 1458125_1_4_1 | OLD | 0 | INACTIVE | 2024-10-26 | 2025-11-18 | 387 | 309,865 |
| 5 | Inverter 105 | 1458125_1_2_1 | OLD | 0 | INACTIVE | 2024-10-26 | 2025-11-18 | 387 | 308,944 |
| 6 | Inverter 106 | 1458125_1_3_1 | OLD | **1** | **ACTIVE** | 2024-10-26 | 2025-11-18 | 387 | 344,232 |
| **NEW INVERTERS (dengan titik)** |
| 7 | Inverter.101 | 1458125_1_33_1 | NEW | 0 | INACTIVE | 2025-07-03 | 2025-11-18 | 139 | 99,413 |
| 8 | Inverter.102 | 1458125_1_29_1 | NEW | **1** | **ACTIVE** | 2025-07-02 | 2025-11-18 | 140 | 153,589 |
| 9 | Inverter.103 | 1458125_1_31_1 | NEW | 0 | INACTIVE | 2025-07-03 | 2025-11-18 | 139 | 99,473 |
| 10 | Inverter.104 | 1458125_1_32_1 | NEW | **1** | **ACTIVE** | 2025-07-03 | 2025-11-18 | 139 | 153,475 |
| 11 | Inverter.105 | 1458125_1_30_1 | NEW | **1** | **ACTIVE** | 2025-07-02 | 2025-11-18 | 140 | 153,760 |
| 12 | Inverter.106 | 1458125_1_34_1 | NEW | 0 | INACTIVE | 2025-07-03 | 2025-11-18 | 139 | 99,446 |

---

## ✅ Tabel 2: 6 Inverter yang Aktif (rel_state = 1)

| No | Device Name | Device ID | Type | First Data | Last Data | Days | Records |
|----|-------------|-----------|------|------------|-----------|------|---------|
| 1 | **Inverter 101** | 1458125_1_6_1 | OLD | 2024-10-26 | 2025-11-18 | 387 | 344,241 |
| 2 | **Inverter 103** | 1458125_1_5_1 | OLD | 2024-10-26 | 2025-11-18 | 387 | 343,977 |
| 3 | **Inverter 106** | 1458125_1_3_1 | OLD | 2024-10-26 | 2025-11-18 | 387 | 344,232 |
| 4 | **Inverter.102** | 1458125_1_29_1 | NEW | 2025-07-02 | 2025-11-18 | 140 | 153,589 |
| 5 | **Inverter.104** | 1458125_1_32_1 | NEW | 2025-07-03 | 2025-11-18 | 139 | 153,475 |
| 6 | **Inverter.105** | 1458125_1_30_1 | NEW | 2025-07-02 | 2025-11-18 | 140 | 153,760 |

**Kesimpulan**: 6 inverter yang aktif terdiri dari:
- **3 OLD inverter** (tanpa titik): Inverter 101, 103, 106
- **3 NEW inverter** (dengan titik): Inverter.102, .104, .105

---

## ❌ Tabel 3: 6 Inverter yang Tidak Aktif (rel_state = 0)

| No | Device Name | Device ID | Type | First Data | Last Data | Days | Records | Keterangan |
|----|-------------|-----------|------|------------|-----------|------|---------|------------|
| 1 | Inverter 102 | 1458125_1_1_1 | OLD | 2024-10-26 | 2025-11-18 | 387 | 309,031 | Ada data tapi tidak aktif |
| 2 | Inverter 104 | 1458125_1_4_1 | OLD | 2024-10-26 | 2025-11-18 | 387 | 309,865 | Ada data tapi tidak aktif |
| 3 | Inverter 105 | 1458125_1_2_1 | OLD | 2024-10-26 | 2025-11-18 | 387 | 308,944 | Ada data tapi tidak aktif |
| 4 | Inverter.101 | 1458125_1_33_1 | NEW | 2025-07-03 | 2025-11-18 | 139 | 99,413 | Data error/kesalahan instalasi |
| 5 | Inverter.103 | 1458125_1_31_1 | NEW | 2025-07-03 | 2025-11-18 | 139 | 99,473 | Data error/kesalahan instalasi |
| 6 | Inverter.106 | 1458125_1_34_1 | NEW | 2025-07-03 | 2025-11-18 | 139 | 99,446 | Data error/kesalahan instalasi |

**Kesimpulan**: 
- 3 OLD inverter (102, 104, 105) tidak aktif, diganti dengan NEW inverter (.102, .104, .105)
- 3 NEW inverter (.101, .103, .106) tidak aktif, kemungkinan data error/kesalahan instalasi

---

## 🔄 Tabel 4: Mapping Old → New Inverter

| Old Inverter | Old ID | Old Status | New Inverter | New ID | New Status | Overlap Start | Overlap End | Mapping Needed? |
|--------------|--------|------------|--------------|--------|------------|---------------|-------------|-----------------|
| Inverter 101 | 1458125_1_6_1 | **ACTIVE** | Inverter.101 | 1458125_1_33_1 | INACTIVE | 2025-07-03 | 2025-11-18 | ❌ **NO** - Old masih aktif |
| Inverter 102 | 1458125_1_1_1 | INACTIVE | Inverter.102 | 1458125_1_29_1 | **ACTIVE** | 2025-07-02 | 2025-11-18 | ✅ **YES** - Old tidak aktif, New aktif |
| Inverter 103 | 1458125_1_5_1 | **ACTIVE** | Inverter.103 | 1458125_1_31_1 | INACTIVE | 2025-07-03 | 2025-11-18 | ❌ **NO** - Old masih aktif |
| Inverter 104 | 1458125_1_4_1 | INACTIVE | Inverter.104 | 1458125_1_32_1 | **ACTIVE** | 2025-07-03 | 2025-11-18 | ✅ **YES** - Old tidak aktif, New aktif |
| Inverter 105 | 1458125_1_2_1 | INACTIVE | Inverter.105 | 1458125_1_30_1 | **ACTIVE** | 2025-07-02 | 2025-11-18 | ✅ **YES** - Old tidak aktif, New aktif |
| Inverter 106 | 1458125_1_3_1 | **ACTIVE** | Inverter.106 | 1458125_1_34_1 | INACTIVE | 2025-07-03 | 2025-11-18 | ❌ **NO** - Old masih aktif |

---

## 📅 Tabel 5: Timeline Data Availability (Daily Yield Analysis)

### Summary per Pasangan:

| Pair | Old First | Old Last | New First | New Last | Overlap Start | Overlap End | Old Only Days | New Only Days | Overlap Days |
|------|-----------|----------|-----------|----------|---------------|-------------|---------------|---------------|--------------|
| **101** | 2024-10-26 | 2025-11-18 | 2025-07-03 | 2025-09-11 | 2025-07-03 | 2025-07-03 | 278 | 70 | 1 |
| **102** | 2024-10-26 | 2025-07-03 | 2025-07-02 | 2025-11-18 | 2025-07-02 | 2025-07-03 | 234 | 134 | 2 |
| **103** | 2024-10-26 | 2025-11-18 | 2025-07-03 | 2025-09-11 | 2025-07-03 | 2025-07-03 | 278 | 70 | 1 |
| **104** | 2024-10-26 | 2025-07-03 | 2025-07-03 | 2025-11-18 | 2025-07-03 | 2025-07-03 | 235 | 134 | 1 |
| **105** | 2024-10-26 | 2025-07-03 | 2025-07-02 | 2025-11-18 | 2025-07-02 | 2025-07-03 | 234 | 134 | 2 |
| **106** | 2024-10-26 | 2025-11-18 | 2025-07-03 | 2025-09-11 | 2025-07-03 | 2025-07-03 | 278 | 70 | 1 |

### Pattern per Periode:

| Period | Old Inverters (6) | New Inverters (6) | Keterangan |
|--------|-------------------|-------------------|------------|
| **2024-10-26 s/d 2025-07-01** | ✅ Semua 6 aktif | ❌ Belum ada | Hanya old inverters yang punya data |
| **2025-07-02** | ✅ Semua 6 aktif | ✅ 2 aktif (.102, .105) | Transisi mulai: .102 dan .105 mulai |
| **2025-07-03** | ⚠️ Mixed | ✅ Semua 6 aktif | **OVERLAP DAY**: Semua new mulai, old 101/103/106 nilai 0, old 102/104/105 berhenti |
| **2025-07-04 s/d 2025-09-11** | ⚠️ Gap | ✅ Semua 6 aktif | Old 101/103/106 gap, New .101/.103/.106 aktif |
| **2025-09-12 s/d 2025-09-30** | ⚠️ Gap | ⚠️ Mixed | Old 101/103/106 gap, New .101/.103/.106 berhenti, New .102/.104/.105 aktif |
| **2025-10-01 s/d 2025-11-18** | ✅ 3 aktif (101/103/106) | ✅ 3 aktif (.102/.104/.105) | Old 101/103/106 kembali aktif, New .102/.104/.105 tetap aktif |

**Catatan Detail**: 
- **OLD Inverter 101, 103, 106**: Punya gap di Agustus-September (tidak ada data), lalu kembali aktif di Oktober-November
- **OLD Inverter 102, 104, 105**: Berhenti di 2025-07-03, tidak kembali aktif
- **NEW Inverter.101, .103, .106**: Aktif 2025-07-03 sampai 2025-09-11 (71 hari), lalu berhenti
- **NEW Inverter.102, .104, .105**: Aktif dari 2025-07-02/03 sampai sekarang (134-136 hari)

---

## 📊 Tabel 6: Monthly Data Availability Pattern (Per Bulan)

| Month | Old 101 | New .101 | Old 102 | New .102 | Old 103 | New .103 | Old 104 | New .104 | Old 105 | New .105 | Old 106 | New .106 |
|-------|---------|----------|---------|----------|---------|----------|---------|----------|---------|----------|---------|----------|
| **2024-10** | 1 | 0 | 1 | 0 | 1 | 0 | 1 | 0 | 1 | 0 | 1 | 0 |
| **2024-11** | 21 | 0 | 21 | 0 | 21 | 0 | 21 | 0 | 21 | 0 | 21 | 0 |
| **2024-12** | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 |
| **2025-01** | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 |
| **2025-02** | 27 | 0 | 27 | 0 | 27 | 0 | 27 | 0 | 27 | 0 | 27 | 0 |
| **2025-03** | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 | 30 | 0 | 31 | 0 |
| **2025-04** | 30 | 0 | 30 | 0 | 30 | 0 | 30 | 0 | 30 | 0 | 30 | 0 |
| **2025-05** | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 | 31 | 0 |
| **2025-06** | 30 | 0 | 30 | 0 | 30 | 0 | 30 | 0 | 30 | 0 | 30 | 0 |
| **2025-07** | **2** | **29** | **2** | **29** | **2** | **29** | **2** | **29** | **2** | **29** | **2** | **29** |
| **2025-08** | **0** | **31** | **0** | **31** | **0** | **31** | **0** | **31** | **0** | **31** | **0** | **31** |
| **2025-09** | **0** | **9** | **0** | **29** | **0** | **9** | **0** | **29** | **0** | **29** | **0** | **9** |
| **2025-10** | **29** | **0** | **0** | **31** | **29** | **0** | **0** | **31** | **0** | **31** | **29** | **0** |
| **2025-11** | **14** | **0** | **0** | **14** | **14** | **0** | **0** | **14** | **0** | **14** | **14** | **0** |

**Keterangan** (angka = hari dengan data positive):
- **Juli 2025**: Old hanya 2 hari (1-2 Juli), New mulai aktif (29 hari)
- **Agustus 2025**: Old 101/103/106 gap (0 hari), New semua aktif (31 hari)
- **September 2025**: Old 101/103/106 gap (0 hari), New .101/.103/.106 berhenti (9 hari), New .102/.104/.105 aktif (29 hari)
- **Oktober 2025**: Old 101/103/106 kembali aktif (29 hari), New .101/.103/.106 berhenti (0 hari), New .102/.104/.105 aktif (31 hari)
- **November 2025**: Old 101/103/106 aktif (14 hari), New .102/.104/.105 aktif (14 hari)

---

## 📊 Tabel 7: Daily Yield Pattern Analysis (Per Hari)

### Pattern Transisi (Juli 2025):

| Date | Pair 101 | Pair 102 | Pair 103 | Pair 104 | Pair 105 | Pair 106 | Keterangan |
|------|----------|----------|----------|----------|----------|----------|------------|
| **2025-07-01** | OLD_ONLY | OLD_ONLY | OLD_ONLY | OLD_ONLY | OLD_ONLY | OLD_ONLY | Semua old aktif |
| **2025-07-02** | OLD_ONLY | **OVERLAP** | OLD_ONLY | OLD_ONLY | **OVERLAP** | OLD_ONLY | .102 dan .105 mulai |
| **2025-07-03** | **OVERLAP** | **OVERLAP** | **OVERLAP** | **OVERLAP** | **OVERLAP** | **OVERLAP** | Semua new mulai (old 101/103/106 nilai 0) |
| **2025-07-04+** | NEW_ONLY | NEW_ONLY | NEW_ONLY | NEW_ONLY | NEW_ONLY | NEW_ONLY | Old 101/103/106 gap, semua new aktif |

### Detail Yield Values (Sample Juli 2025):

| Date | Old 101 | New .101 | Old 102 | New .102 | Old 103 | New .103 | Old 104 | New .104 | Old 105 | New .105 | Old 106 | New .106 |
|------|---------|----------|---------|----------|---------|----------|---------|----------|---------|----------|---------|----------|
| **2025-07-01** | 84,009 | - | 82,130 | - | 87,478 | - | 82,161 | - | 48,902 | - | 56,784 | - |
| **2025-07-02** | 90,111 | - | 82,542 | 0 | 91,098 | - | 88,100 | - | 52,681 | 0 | 60,655 | - |
| **2025-07-03** | **0** | 80,229 | **0** | 66,478 | **0** | 77,474 | **0** | 77,473 | **0** | 45,421 | **0** | 53,501 |
| **2025-07-04** | - | 88,713 | - | 83,311 | - | 97,989 | - | 92,367 | - | 54,272 | - | 62,310 |
| **2025-07-05** | - | 88,670 | - | 77,990 | - | 88,005 | - | 87,368 | - | 51,869 | - | 62,455 |

**Keterangan**:
- **2025-07-03**: Old 101, 103, 106 punya nilai 0 (tidak lengkap, hanya 134-147 records vs normal 288)
- **2025-07-04+**: Old 101, 103, 106 tidak ada data (gap) sampai Oktober
- **2025-10-01+**: Old 101, 103, 106 kembali aktif (29 hari di Okt, 14 hari di Nov)

---

## 📊 Tabel 8: Data Quality Analysis (Setelah Juli 2025)

| Inverter | Type | Days with Data | Days with Positive Value | Max Value | Avg Value | Status |
|----------|------|----------------|--------------------------|-----------|-----------|--------|
| **OLD Inverters** |
| Inverter 101 | OLD | 46 | 45 | 107,256 | 18,008 | ✅ **Masih aktif** |
| Inverter 102 | OLD | **3** | **2** | 82,542 | 14,733 | ❌ **Berhenti** (hanya residual) |
| Inverter 103 | OLD | 46 | 45 | 110,152 | 19,763 | ✅ **Masih aktif** |
| Inverter 104 | OLD | **3** | **2** | 88,100 | 15,767 | ❌ **Berhenti** (hanya residual) |
| Inverter 105 | OLD | **3** | **2** | 52,681 | 8,686 | ❌ **Berhenti** (hanya residual) |
| Inverter 106 | OLD | 46 | 45 | 110,476 | 19,580 | ✅ **Masih aktif** |
| **NEW Inverters** |
| Inverter.101 | NEW | 71 | 69 | 103,222 | 18,643 | ⚠️ Ada data tapi tidak aktif |
| Inverter.102 | NEW | **136** | **134** | 108,672 | 18,625 | ✅ **Aktif** |
| Inverter.103 | NEW | 71 | 69 | 110,170 | 18,980 | ⚠️ Ada data tapi tidak aktif |
| Inverter.104 | NEW | **135** | **134** | 110,153 | 19,558 | ✅ **Aktif** |
| Inverter.106 | NEW | 71 | 69 | 101,770 | 17,143 | ⚠️ Ada data tapi tidak aktif |
| Inverter.105 | NEW | **136** | **134** | 110,048 | 18,322 | ✅ **Aktif** |

**Kesimpulan**:
- **OLD Inverter 102, 104, 105**: Hanya punya data 2-3 hari setelah Juli, lalu berhenti total → **Diganti oleh NEW**
- **OLD Inverter 101, 103, 106**: Masih punya data meaningful (46 hari) setelah Juli → **Tetap digunakan**
- **NEW Inverter.102, .104, .105**: Punya data lengkap (136 hari) → **Aktif, menggantikan OLD**
- **NEW Inverter.101, .103, .106**: Punya data (71 hari) tapi tidak aktif → **Data error, diabaikan**

---

## 🔍 Tabel 9: Analisis Data Inverter.101, .103, .106 (Tidak Aktif tapi Ada Data)

| Inverter | Total Timestamps | Overlapping dengan Old | Unique Timestamps | Max Value | Avg Value | Keterangan |
|----------|------------------|------------------------|-------------------|-----------|-----------|------------|
| Inverter.101 | 19,890 | 143 | 19,747 | 103,222 | 18,643 | ⚠️ **Data valid tapi tidak aktif** |
| Inverter.103 | 19,910 | 133 | 19,777 | 110,170 | 18,980 | ⚠️ **Data valid tapi tidak aktif** |
| Inverter.106 | 19,901 | 140 | 19,761 | 101,770 | 17,143 | ⚠️ **Data valid tapi tidak aktif** |

### Analisis Detail:

**Fakta**:
- Inverter.101, .103, .106 punya **19,890-19,910 timestamps** dengan data valid (bukan hanya 0)
- Hanya **133-143 timestamps** yang overlap dengan old inverter (0.7% overlap)
- Sebagian besar (**19,747-19,777 timestamps**) adalah **unique timestamps** yang tidak ada di old inverter
- Data punya nilai yang meaningful (max 80k-110k, avg 17k-19k)

**Kemungkinan Penyebab**:
1. **Kesalahan Instalasi**: Device fisik terpasang tapi tidak terdaftar aktif di platform (rel_state = 0)
2. **Device Duplikat**: Device yang sama terpasang 2 kali (old dan new) tapi hanya old yang aktif
3. **Data dari Device Lain**: Data dari device fisik lain yang salah ter-mapping ke ID ini
4. **Residual Data**: Data sisa dari proses instalasi/testing yang tidak dihapus

**Kesimpulan**:
- Data ini **BUKAN duplikat** dari old inverter (hanya 0.7% overlap)
- Data ini **valid** (punya nilai meaningful)
- Tapi device **tidak aktif** di platform (rel_state = 0)
- **Tidak perlu digunakan** karena old inverter (101, 103, 106) masih aktif dan punya data yang lebih lengkap

---

## 📋 Tabel 10: Summary Pattern - Kosong, Overlap, dan Override

### Pair 101 (Inverter 101 vs Inverter.101):

| Period | Old 101 | New .101 | Status | Keterangan |
|--------|---------|----------|--------|------------|
| 2024-10-26 s/d 2025-07-01 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Hanya old aktif |
| 2025-07-02 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Old masih aktif |
| 2025-07-03 | ⚠️ Nilai 0 | ✅ Aktif | **OVERLAP** | Old nilai 0 (tidak lengkap), New mulai |
| 2025-07-04 s/d 2025-09-11 | ❌ Kosong | ✅ Aktif | NEW_ONLY | Old gap, New aktif |
| 2025-09-12 s/d 2025-09-30 | ❌ Kosong | ❌ Kosong | NO_DATA | Keduanya gap |
| 2025-10-01 s/d 2025-11-18 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Old kembali aktif, New berhenti |

**Kesimpulan**: ✅ **PERLU MAPPING** - Old 101 tetap digunakan, tapi di periode gap (Agustus-September) data dari New .101 digunakan untuk melengkapi. Mapping dengan `effective_date_end = 2025-09-30`.

---

### Pair 102 (Inverter 102 vs Inverter.102):

| Period | Old 102 | New .102 | Status | Keterangan |
|--------|---------|----------|--------|------------|
| 2024-10-26 s/d 2025-07-01 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Hanya old aktif |
| 2025-07-02 | ✅ Aktif | ✅ Aktif | **OVERLAP** | **SALING MELENGKAPI** - Data old dan new punya timestamps berbeda |
| 2025-07-03 | ⚠️ Nilai 0 | ✅ Aktif | **OVERLAP** | Old nilai 0 (tidak lengkap), New aktif |
| 2025-07-04 s/d 2025-11-18 | ❌ Kosong | ✅ Aktif | **OVERRIDE** | Old berhenti, New aktif |

**Kesimpulan**: ✅ **PERLU MAPPING** - Old 102 → New .102 mulai 2025-07-02
**Catatan Penting**: Di periode overlap (2025-07-02), data old dan new **saling melengkapi** (timestamps berbeda), bukan duplikat. Mapping ID_CONSOLIDATION akan menggabungkan data dari old ke new ID, sehingga data lengkap tersedia.

---

### Pair 103 (Inverter 103 vs Inverter.103):

| Period | Old 103 | New .103 | Status | Keterangan |
|--------|---------|----------|--------|------------|
| 2024-10-26 s/d 2025-07-01 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Hanya old aktif |
| 2025-07-02 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Old masih aktif |
| 2025-07-03 | ⚠️ Nilai 0 | ✅ Aktif | **OVERLAP** | Old nilai 0 (tidak lengkap), New mulai |
| 2025-07-04 s/d 2025-09-11 | ❌ Kosong | ✅ Aktif | NEW_ONLY | Old gap, New aktif |
| 2025-09-12 s/d 2025-09-30 | ❌ Kosong | ❌ Kosong | NO_DATA | Keduanya gap |
| 2025-10-01 s/d 2025-11-18 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Old kembali aktif, New berhenti |

**Kesimpulan**: ✅ **PERLU MAPPING** - Old 103 tetap digunakan, tapi di periode gap (Agustus-September) data dari New .103 digunakan untuk melengkapi. Mapping dengan `effective_date_end = 2025-09-30`.

---

### Pair 104 (Inverter 104 vs Inverter.104):

| Period | Old 104 | New .104 | Status | Keterangan |
|--------|---------|----------|--------|------------|
| 2024-10-26 s/d 2025-07-02 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Hanya old aktif |
| 2025-07-03 | ⚠️ Nilai 0 | ✅ Aktif | **OVERLAP** | Old nilai 0 (tidak lengkap), New mulai |
| 2025-07-04 s/d 2025-11-18 | ❌ Kosong | ✅ Aktif | **OVERRIDE** | Old berhenti, New aktif |

**Kesimpulan**: ✅ **PERLU MAPPING** - Old 104 → New .104 mulai 2025-07-03
**Catatan Penting**: Di periode overlap (2025-07-03), old punya nilai 0 (data tidak lengkap), jadi new mengambil alih sepenuhnya.

---

### Pair 105 (Inverter 105 vs Inverter.105):

| Period | Old 105 | New .105 | Status | Keterangan |
|--------|---------|----------|--------|------------|
| 2024-10-26 s/d 2025-07-01 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Hanya old aktif |
| 2025-07-02 | ✅ Aktif | ✅ Aktif | **OVERLAP** | **SALING MELENGKAPI** - Data old dan new punya timestamps berbeda |
| 2025-07-03 | ⚠️ Nilai 0 | ✅ Aktif | **OVERLAP** | Old nilai 0 (tidak lengkap), New aktif |
| 2025-07-04 s/d 2025-11-18 | ❌ Kosong | ✅ Aktif | **OVERRIDE** | Old berhenti, New aktif |

**Kesimpulan**: ✅ **PERLU MAPPING** - Old 105 → New .105 mulai 2025-07-02
**Catatan Penting**: Di periode overlap (2025-07-02), data old dan new **saling melengkapi** (timestamps berbeda), bukan duplikat. Mapping ID_CONSOLIDATION akan menggabungkan data dari old ke new ID, sehingga data lengkap tersedia.

---

### Pair 106 (Inverter 106 vs Inverter.106):

| Period | Old 106 | New .106 | Status | Keterangan |
|--------|---------|----------|--------|------------|
| 2024-10-26 s/d 2025-07-01 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Hanya old aktif |
| 2025-07-02 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Old masih aktif |
| 2025-07-03 | ⚠️ Nilai 0 | ✅ Aktif | **OVERLAP** | Old nilai 0 (tidak lengkap), New mulai |
| 2025-07-04 s/d 2025-09-11 | ❌ Kosong | ✅ Aktif | NEW_ONLY | Old gap, New aktif |
| 2025-09-12 s/d 2025-09-30 | ❌ Kosong | ❌ Kosong | NO_DATA | Keduanya gap |
| 2025-10-01 s/d 2025-11-18 | ✅ Aktif | ❌ Kosong | OLD_ONLY | Old kembali aktif, New berhenti |

**Kesimpulan**: ✅ **PERLU MAPPING** - Old 106 tetap digunakan, tapi di periode gap (Agustus-September) data dari New .106 digunakan untuk melengkapi. Mapping dengan `effective_date_end = 2025-09-30`.

---

## 🎯 Kesimpulan dan Rekomendasi Mapping

### Mapping yang Diperlukan (6 Inverter):

#### 1. Mapping Permanen (Old Berhenti Total) - 3 Inverter:

| Mapping Type | Old Device ID | Old Name | New Device ID | New Name | Effective Date Start | Effective Date End | Reason |
|--------------|---------------|----------|---------------|----------|---------------------|-------------------|--------|
| ID_CONSOLIDATION | 1458125_1_1_1 | Inverter 102 | 1458125_1_29_1 | Inverter.102 | 2025-07-02 | NULL | Old berhenti 2025-07-03, New aktif sampai sekarang |
| ID_CONSOLIDATION | 1458125_1_4_1 | Inverter 104 | 1458125_1_32_1 | Inverter.104 | 2025-07-03 | NULL | Old berhenti 2025-07-03, New aktif sampai sekarang |
| ID_CONSOLIDATION | 1458125_1_2_1 | Inverter 105 | 1458125_1_30_1 | Inverter.105 | 2025-07-02 | NULL | Old berhenti 2025-07-03, New aktif sampai sekarang |

#### 2. Mapping Sementara (Mengisi Gap) - 3 Inverter:

| Mapping Type | New Device ID (Source) | New Name | Old Device ID (Target) | Old Name | Effective Date Start | Effective Date End | Reason |
|--------------|------------------------|----------|------------------------|----------|---------------------|-------------------|--------|
| ID_CONSOLIDATION | 1458125_1_33_1 | Inverter.101 | 1458125_1_6_1 | Inverter 101 | 2025-08-01 | 2025-09-30 | New mengisi gap old di Agustus-September, data new di-consolidate ke old ID, Old kembali aktif di Oktober |
| ID_CONSOLIDATION | 1458125_1_31_1 | Inverter.103 | 1458125_1_5_1 | Inverter 103 | 2025-08-01 | 2025-09-30 | New mengisi gap old di Agustus-September, data new di-consolidate ke old ID, Old kembali aktif di Oktober |
| ID_CONSOLIDATION | 1458125_1_34_1 | Inverter.106 | 1458125_1_3_1 | Inverter 106 | 2025-08-01 | 2025-09-30 | New mengisi gap old di Agustus-September, data new di-consolidate ke old ID, Old kembali aktif di Oktober |

**Catatan Penting untuk Mapping Gap**:
- Mapping dilakukan dari **New → Old** (bukan Old → New)
- Artinya: Data dari new inverter di periode gap (Agustus-September) akan di-consolidate ke old ID
- Hasilnya: Old ID akan punya data lengkap (data old di semua periode + data new di periode gap)
- Di Oktober, old kembali aktif, jadi data old tetap dengan old ID

**Catatan Mapping**:
- **Pair 102**: Overlap 2 hari (2025-07-02 dan 2025-07-03), mapping mulai 2025-07-02
  - **2025-07-02**: Data old dan new **saling melengkapi** (timestamps berbeda) → Mapping akan menggabungkan keduanya
  - **2025-07-03**: Old nilai 0 (tidak lengkap), New aktif → New mengambil alih
- **Pair 104**: Overlap 1 hari (2025-07-03), mapping mulai 2025-07-03
  - **2025-07-03**: Old nilai 0 (tidak lengkap), New aktif → New mengambil alih
- **Pair 105**: Overlap 2 hari (2025-07-02 dan 2025-07-03), mapping mulai 2025-07-02
  - **2025-07-02**: Data old dan new **saling melengkapi** (timestamps berbeda) → Mapping akan menggabungkan keduanya
  - **2025-07-03**: Old nilai 0 (tidak lengkap), New aktif → New mengambil alih

**⚠️ PENTING - Data Saling Melengkapi**:
- Di periode overlap (khususnya 2025-07-02 untuk Pair 102 dan 105), data dari old dan new inverter **saling melengkapi**, bukan duplikat
- Artinya: Old punya data di timestamps tertentu, New punya data di timestamps yang berbeda
- Dengan mapping `ID_CONSOLIDATION`, data dari old akan di-consolidate ke new ID, sehingga:
  - Data old di timestamps yang tidak ada di new → akan muncul dengan new ID
  - Data new di timestamps yang tidak ada di old → tetap dengan new ID
  - Hasilnya: Data lengkap tersedia dengan new ID, menggabungkan data dari kedua inverter

**Catatan Penting**:
- **Pair 101, 103, 106**: Mapping hanya untuk periode gap (Agustus-September), karena old kembali aktif di Oktober
- **Pair 102, 104, 105**: Mapping permanen (tanpa end date), karena old berhenti total dan tidak kembali aktif

---

## 📋 Summary

### 6 Inverter yang Aktif (Final):
1. **Inverter 101** (1458125_1_6_1) - OLD, tetap digunakan
2. **Inverter 103** (1458125_1_5_1) - OLD, tetap digunakan
3. **Inverter 106** (1458125_1_3_1) - OLD, tetap digunakan
4. **Inverter.102** (1458125_1_29_1) - NEW, menggantikan Inverter 102
5. **Inverter.104** (1458125_1_32_1) - NEW, menggantikan Inverter 104
6. **Inverter.105** (1458125_1_30_1) - NEW, menggantikan Inverter 105

### Mapping Logic:
- **Inverter 101, 103, 106** (OLD): Tetap digunakan, tidak perlu mapping
  - Punya gap di Agustus-September, lalu kembali aktif di Oktober-November
  - Data lebih lengkap (279 hari) dibanding new (71 hari)
- **Inverter 102, 104, 105** (OLD): Diganti dengan Inverter.102, .104, .105 (NEW)
  - Old berhenti di 2025-07-03, tidak kembali aktif
  - New aktif dari 2025-07-02/03 sampai sekarang (134-136 hari)
- **Inverter.101, .103, .106** (NEW): Tidak aktif, tapi **MENGISI GAP** old inverter di Agustus-September
  - Aktif 2025-07-03 sampai 2025-09-11 (71 hari)
  - Mengisi gap old inverter di periode Agustus-September (31 hari di Agustus, 9 hari di September)
  - Lalu berhenti total, tidak kembali aktif
  - **PERLU MAPPING** untuk periode gap (Agustus-September) dengan `effective_date_end = 2025-09-30`

### Penjelasan Data Inverter.101, .103, .106:
- Punya data valid (19,890-19,910 timestamps) tapi **tidak aktif** (rel_state = 0)
- Hanya 0.7% overlap dengan old inverter → **BUKAN duplikat**
- Aktif hanya 71 hari (2025-07-03 sampai 2025-09-11), lalu berhenti
- **MENGISI GAP** old inverter di periode Agustus-September 2025
- **PERLU MAPPING** untuk periode gap (Agustus-September), kemudian old kembali aktif di Oktober

### Penjelasan Gap Old Inverter 101, 103, 106:
- **Juli 2025**: Hanya 2 hari (1-2 Juli), di 3 Juli nilai 0 (data tidak lengkap)
- **Agustus-September 2025**: **GAP** - tidak ada data sama sekali
  - **Data gap diisi oleh New .101, .103, .106** (31 hari di Agustus, 9 hari di September)
- **Oktober-November 2025**: Kembali aktif (29 hari di Okt, 14 hari di Nov)
- Kemungkinan: Maintenance, perbaikan, atau masalah komunikasi sementara
- **Strategi Mapping**: Old tetap digunakan, tapi di periode gap (Agustus-September) data dari new digunakan untuk melengkapi

---

**Last Updated**: 2025-01-XX  
**Status**: Analysis completed, ready for mapping implementation
