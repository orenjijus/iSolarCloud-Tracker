---
marp: true
theme: default
paginate: true
header: 'MMSR — Clarification Meeting'
footer: '16 Mei 2026 | Draft audit sistem'
style: |
  section { font-size: 28px; }
  section.lead h1 { font-size: 48px; }
  section.small { font-size: 22px; }
  table { font-size: 20px; }
---

<!-- _class: lead -->

# Clarification Meeting
## Audit Sistem MMSR — Site Performance & Pipeline

**16 Mei 2026**

Klarifikasi temuan · Konfirmasi definisi metrik · Prioritas perbaikan

*Bukan sesi implementasi teknis hari ini*

---

## Tujuan rapat hari ini

1. **Validasi** pesan utama audit (April + Mei 2026)
2. **Konfirmasi** definisi bisnis (termasuk **a_KPI**)
3. **Kesepakatan** pemilik data & prioritas perbaikan
4. **Keputusan** apa yang wajib vs bisa ditunda

**Referensi detail:** `2026-05-16_Laporan_Masalah_Sistem_MMSR.md`

---

<!-- _class: lead -->

# Pesan utama
### (jawaban di depan — Minto Pyramid)

---

## Pesan utama — 1 kalimat

**Data mentah dari API masih masuk setiap malam**, tetapi **laporan site performance belum dapat dipakai penuh** untuk keputusan operasional karena:

- Kegagalan **dbt sejak 14 Mei**
- **8 site** metrik nol/kosong (masalah **konfigurasi seed**)
- Aturan sensor/KPI **tersebar** — sulit diaudit user

**Kita butuh konfirmasi Anda:** definisi metrik · pemilik seed · urutan perbaikan

---

## Tiga pilar masalah

| # | Topik | Inti masalah |
|---|--------|----------------|
| **1** | **Pipeline** | Ingest OK · dbt gagal 14–16 Mei |
| **2** | **8 site** | Data mentah ada · mart energi/GHI = 0 |
| **3** | **Device & KPI** | Override sensor (WST) · a_KPI tidak jelas |

```
         [ Pesan utama: laporan belum siap dipakai penuh ]
              /              |              \
        Pipeline          8 site         KPI & sensor
```

---

## Linimasa: April vs Mei

| Periode | Platform (~25 site) | 8 site bermasalah | Shoetown & Majalengka |
|---------|---------------------|-------------------|------------------------|
| **April** | **~17 site OK** di mart | Energi & GHI **= 0** (30 hari) | **OK** — GHI fallback Shoetown |
| **Mei 1–13** | dbt **sukses** | Tetap **= 0** | Shoetown GHI OK · **Majalengka GHI NULL** |
| **Mei 14–16** | dbt **gagal** | Tidak ter-update | Tidak ter-update setelah 13 Mei |

**Kesimpulan:** Audit = **April + Mei**. Bukan hanya insiden 14 Mei.

---

## April 2026 — apa yang kita temukan?

**April tidak “mati” seluruhnya**

- ~**17 dari ~25** site punya energi & GHI di mart (harian)
- Log pipeline harian di folder `scripts/logs` **mulai Mei** (April dari backfill/reingest)

**8 site (Samator, Weiss, Gelora, …)**

- Baris mart **ada** (~30 hari)
- **Energi & GHI = 0** sepanjang April
- Meter mentah **ada** → masalah **seed**, bukan API

---

## Mei: Pipeline harian

| | Status |
|---|--------|
| **Scheduler 01:00** | Berjalan |
| **Ingest FusionSolar + iSolarCloud** | ~sukses (~39k baris/hari) |
| **dbt transformasi** | **Gagal 14, 15, 16 Mei** |
| **Penyebab** | Kolom `issue_date_remarks` (schema drift) |
| **Dampak** | Mart KPI **berhenti 13 Mei** · 3 model downstream SKIP |

**Pertanyaan rapat:** Fix dbt + backfill 13–15 Mei = **P0**?

---

## 8 site — data ada, mart salah

**Site:** Weiss Tech · Gelora Djaja · 6× Samator (+ Bali terbatas)

| Pilar | Kondisi di mart | Akar masalah |
|-------|----------------|--------------|
| **Energi** | 0 MWh | `seed_meter_config` — tidak ada meter **Revenue** |
| **Iradiasi** | NULL | Seed sensor / POA belum lengkap |
| **Inverter / availability** | ~0 | `seed_site_config` / inverter config kosong |

**Berlaku April dan Mei** — bukan efek dbt 14 Mei saja

**Pertanyaan rapat:** Wajib bulan ini? **Siapa** isi seed (Ops / Engineering)?

---

## Contoh: Shoetown & CP Majalengka

**April 2026**

- Keduanya **OK** (energi + GHI, 30/30 hari)
- GHI Majalengka **sama** dengan Shoetown → aturan **GHI_FALLBACK**

**≥ 1 Mei 2026**

- Aturan baru: **WST Shoetown** (`ISO_1479456_5_10_2`)
- **Shoetown:** GHI terisi (Mei 1–12)
- **Majalengka:** energi OK · **GHI NULL** di mart (sensor WST ada di layer bawah)

**Pertanyaan rapat:** Setuju aturan WST? Majalengka harus ikut GHI Shoetown?

---

## a_KPI — definisi untuk konfirmasi

| Istilah | **Bukan** | **Adalah** |
|---------|-----------|------------|
| **KPI harian** | Energi aktual | KPI bulan × (target harian ÷ target bulan) |
| **a_KPI** | Availability | Target harian × (KPI bulan ÷ simulasi bulan) |

**a_KPI** = target harian disesuaikan rasio KPI vs simulasi (exclude unavailability di logika bisnis)

**Pertanyaan rapat:** Sesuai Excel / pemahaman Finance-Ops?

---

## Agenda & keputusan yang dibutuhkan

| # | Topik | Output rapat |
|---|--------|--------------|
| 1 | Validasi pesan utama | Setuju prioritas? |
| 2 | April vs Mei | Cakupan audit disetujui? |
| 3 | 8 site | Wajib / phased? Pemilik seed? |
| 4 | Shoetown / Majalengka | Aturan WST & fix Majalengka GHI? |
| 5 | a_KPI & kamus metrik | Sign-off definisi? Dokumen resmi? |
| 6 | Next step | Jadwal perbaikan |

**Durasi usulan:** 60–90 menit

---

## Prioritas perbaikan (usulan DE)

| Prioritas | Aksi | Dampak |
|-----------|------|--------|
| **P0** | Fix `issue_date_remarks` + dbt run | Mart ter-update lagi |
| **P0** | Backfill mart **13–15 Mei** | Tutup gap pipeline |
| **P1** | Seed meter Revenue — 8 site | Energi terisi |
| **P1** | Fix GHI Majalengka (WST Mei) | Konsisten Shoetown |
| **P2** | Seed sensor/site + dokumentasi | Iradiasi & audit trail |

*Urutan final — keputusan rapat*

---

<!-- _class: small -->

## Bukan keputusan hari ini

- Implementasi aplikasi Metric Catalog
- Pengisian massal semua seed tanpa pemilik
- Ubah rumus dbt (selain fix `issue_date_remarks`)

---

<!-- _class: small -->

## Q&A cepat

| Pertanyaan | Jawaban singkat |
|------------|-----------------|
| API mati? | **Tidak** — ingest malam jalan |
| April rusak semua? | **Tidak** — 8 site salah; ~17 site OK |
| Kenapa Samator energi 0? | Meter ada; belum seed **Revenue** |
| Apa itu a_KPI? | Target harian × (KPI÷simulasi bulan) |
| Majalengka sensor? | Apr: dari Shoetown · Mei: WST (GHI mart perlu fix) |

---

<!-- _class: lead -->

# Terima kasih

**Diskusi & keputusan rapat**

Catat: pemilik seed · prioritas P0/P1 · sign-off a_KPI

Dokumen: `docs/reports/2026-05-16_Laporan_Masalah_Sistem_MMSR.md`
