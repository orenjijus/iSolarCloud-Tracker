# Clarification Meeting — Ringkasan Audit Sistem MMSR (Minto Pyramid)

**Tanggal dokumen:** 16 Mei 2026  
**Tujuan rapat:** Klarifikasi temuan audit, kesepakatan definisi bisnis, dan prioritas perbaikan — **bukan** implementasi teknis hari ini.

---

## Pesan utama (jawaban di depan)

**Data mentah dari API masih masuk setiap malam, tetapi laporan site performance saat ini tidak dapat dipakai untuk keputusan operasional secara penuh:** ada **kegagalan transformasi dbt sejak 14 Mei**, **delapan site dengan metrik nol/kosong** karena konfigurasi belum lengkap, dan **aturan sensor/meter/KPI (termasuk a_KPI) tersebar di banyak seed** sehingga hanya tim teknis yang memahaminya — **kita perlu konfirmasi Anda pada definisi metrik, pemilik data seed, dan urutan perbaikan.**

---

## Tiga poin pendukung (logika piramida)

### 0. April 2026 — perlu dicek juga (bukan hanya Mei)


| Temuan                          | Detail                                                                                                                                                                                                                                                                                                             |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Pipeline / mart April**       | Secara platform **April relatif sehat**: ~17 dari ~25 site punya energi & GHI di mart setiap hari. **Bukan** bulan yang seluruhnya gagal.                                                                                                                                                                          |
| **Log pipeline April**          | Tidak ada file `daily_pipeline_*` di `scripts/logs` untuk April (log tersimpan mulai run **Mei**). Data April di mart kemungkinan dari backfill/reingest sebelumnya (`dbt/logs` ada run 27–28 Apr).                                                                                                                |
| **8 site bermasalah**           | Masalah **sama di April**: baris mart ada (~30 hari), tetapi **energi & GHI = 0** sepanjang April — akar masalah **seed meter/sensor**, bukan kegagalan dbt 14 Mei.                                                                                                                                                |
| **Shoetown & Majalengka April** | **OK di April**: energi & GHI terisi (30/30 hari). GHI Majalengka April **sama** dengan Shoetown di banyak hari → sesuai aturan `GHI_FALLBACK` (Apr 2026).                                                                                                                                                         |
| **Transisi akhir April → Mei**  | **1 Mei:** aturan WST (`GHI_ACTUAL_OVERRIDE`) aktif. **Shoetown** Mei 1–12: GHI terisi (~4.2 kWh/m² rata-rata). **Majalengka** Mei 1–12: **energi OK, GHI NULL** di mart padahal sensor WST (`ISO_1479456_5_10_2`) punya data di `mart_sensor_daily` → **isu khusus konfigurasi/refresh**, perlu dibahas di rapat. |
| **30 Apr anomali**              | Shoetown energi ~0.016 MWh; Majalengka GHI mulai NULL — hari transisi perlu validasi manual.                                                                                                                                                                                                                       |


**Kesimpulan untuk rapat:** Audit harus mencakup **April + Mei**. April membuktikan 8 site sudah salah sebelum insiden dbt; Shoetown/Majalengka April bagus tetapi **Majalengka GHI hilang sejak aturan WST Mei**.

---

### 1. Pipeline harian: ingest jalan, laporan mart berhenti sejak 14 Mei


|                            |                                                                                             |
| -------------------------- | ------------------------------------------------------------------------------------------- |
| **Yang berjalan**          | Scheduler 01:00, FusionSolar + iSolarCloud ingest ~~sukses (~~39k baris/hari)               |
| **Yang gagal**             | Tahap dbt gagal 3 malam berturut-turut (14–16 Mei) pada model `mart_site_performance_daily` |
| **Penyebab teknis**        | Ketidaksesuaian kolom `issue_date_remarks` (schema drift model vs tabel database)           |
| **Dampak bisnis**          | KPI site/string di Power BI **tidak ter-update** setelah 13 Mei; 24 model lain tetap jalan  |
| **Butuh konfirmasi rapat** | Apakah perbaikan dbt + backfill 13–15 Mei **prioritas P0** sebelum topik lain?              |


---

### 2. Delapan site: data ada di database, angka di mart salah (bukan masalah API)

Site: Weiss Tech, Gelora Djaja, 6× Samator (+ Bali terbatas tanggal).


| Pilar data                  | Kondisi                                                                                   | Akar masalah (ringkas)                                                      |
| --------------------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| **Energi**                  | Meter 5-menit **ada**, `daily_energy_mwh = 0`                                             | Tidak ada baris **Revenue** di `seed_meter_config`                          |
| **Iradiasi**                | Samator: tidak ada sensor cuaca di portal; Weiss/Gelora: sensor ada, GHI/POA NULL di mart | Seed sensor/POA belum lengkap; override WST belum terbaca tanpa dokumentasi |
| **Inverter / availability** | Inverter 5-menit ada, availability ~0                                                     | `seed_site_config` / inverter config belum untuk site ini                   |


**Butuh konfirmasi rapat:** Apakah delapan site ini **wajib** masuk dashboard bulan ini? Siapa pemilik yang mengisi seed meter/sensor (Ops vs Engineering)?

---

### 3. Perubahan device & definisi KPI: perlu kesepakatan bisnis, bukan hanya perbaikan kode

**Contoh untuk didiskusikan (Shoetown & CP Majalengka):**

- Sejak **Mei 2026**, GHI operasional memakai **WST di plant Shoetown** (`ISO_1479456_5_10_2`) — termasuk untuk Majalengka (bukan sensor fisik di lokasi Majalengka).
- Aturan ini ada di `**seed_sensor_site_mapping`** dengan tanggal efektif dan kolom `notes` — tidak terlihat dari tabel mart saja.

**Kolom yang sering membingungkan: a_KPI (`energy_a_kpi_daily_mwh`)**


| Istilah    | Bukan ini           | Ini sebenarnya                               |
| ---------- | ------------------- | -------------------------------------------- |
| a_KPI      | Energi aktual plant | Target harian × (KPI bulan ÷ simulasi bulan) |
| KPI harian | Export meter        | KPI bulan × (target harian ÷ target bulan)   |


**Butuh konfirmasi rapat:**

1. Apakah definisi a_KPI di atas **sesuai pemahaman Finance/Ops**?
2. Siapa yang **menyetujui** perubahan sensor (mis. PYR → WST) ke depan?
3. Apakah perlu **satu dokumen kamus metrik** resmi sebelum rollout ke user luas? *(draft sudah disiapkan untuk dibahas, belum disosialisasikan)*

---

## Diagram piramida (untuk slide)

```
                    ┌─────────────────────────────────────────┐
                    │  MESAJ UTAMA: Laporan site performance   │
                    │  tidak siap dipakai penuh — perlu        │
                    │  fix dbt + seed + konfirmasi definisi KPI │
                    └─────────────────────────────────────────┘
                           /              |              \
                          /               |               \
           ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐
           │ 1. Pipeline  │   │ 2. 8 site    │   │ 3. Device & KPI  │
           │ ingest OK    │   │ mart kosong  │   │ sulit diaudit    │
           │ dbt gagal    │   │ seed belum   │   │ butuh agreement  │
           │ sejak 14 Mei │   │ lengkap      │   │ user (a_KPI…)    │
           └──────────────┘   └──────────────┘   └──────────────────┘
```

---

## Agenda usulan (60–90 menit)


| #   | Topik                            | Output yang diharapkan                                      |
| --- | -------------------------------- | ----------------------------------------------------------- |
| 1   | Validasi pesan utama & prioritas | Setuju P0: dbt 14 Mei + backfill?                           |
| 2   | Delapan site                     | Wajib atau phased? Pemilik pengisian seed?                  |
| 3   | Shoetown / Majalengka WST        | Setuju aturan Mei 2026? Cara dokumentasi ke depan?          |
| 4   | a_KPI & KPI harian               | Konfirmasi rumus vs Excel; siapa sign-off?                  |
| 5   | Next step                        | Jadwal perbaikan; apakah perlu Metric Catalog / kamus resmi |


---

## Yang **belum** dibahas sebagai keputusan final hari ini

- Implementasi Streamlit Metric Catalog  
- Pengisian massal semua seed  
- Perubahan rumus di model dbt (selain fix `issue_date_remarks`)

*Detail teknis tersedia di laporan audit lengkap bila diperlukan saat rapat: `2026-05-16_Laporan_Masalah_Sistem_MMSR.md`.*

---

## Lampiran cepat (jika ditanya)


| Pertanyaan user               | Jawaban satu kalimat                                                                                               |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Apakah API mati?              | Tidak — ingest malam masih jalan.                                                                                  |
| Kenapa dashboard kosong?      | dbt gagal sejak 14 Mei + banyak site tanpa seed config.                                                            |
| Apakah April juga bermasalah? | **Sebagian:** 8 site energi/GHI 0 sepanjang April; ~17 site lain OK. Bukan seluruh April rusak.                    |
| Kenapa Samator energi 0?      | Meter ada, belum terdaftar sebagai Revenue di seed (berlaku April & Mei).                                          |
| Apa itu a_KPI?                | Target harian yang diskalakan KPI bulan vs simulasi bulan — bukan produksi aktual.                                 |
| Shoetown & Majalengka?        | **April:** Majalengka GHI dari Shoetown (fallback). **Mei+:** WST Shoetown; Majalengka GHI NULL di mart perlu fix. |


---

*Dokumen ini hanya untuk clarification meeting. Revisi setelah rapat berdasarkan keputusan peserta.*