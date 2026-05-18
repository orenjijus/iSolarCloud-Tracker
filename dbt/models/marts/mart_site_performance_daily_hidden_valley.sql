{{ config(
    materialized='view',
    schema='mart'
) }}

-- Performa site harian khusus PLTS + baterai Hidden Valley.
-- Site ini tidak masuk mart_site_performance_daily (logika PV standar tidak cocok).
-- Ganti SELECT di bawah saat aturan perhitungan (battery / grid / meter) sudah final.
-- Skema kolom mengikuti mart_site_performance_daily agar laporan / union konsisten.

SELECT *
FROM {{ ref('mart_site_performance_daily') }}
WHERE FALSE
