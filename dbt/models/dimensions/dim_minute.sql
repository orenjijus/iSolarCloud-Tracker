{{ config(
    materialized='table',
    schema='dimensions'
) }}

-- Dimensi 5 menit: satu baris per slot dari 2024 sampai 2030.
-- Langsung bisa relationship di Power BI: fact[timestamp] → dim_minute[timestamp], dan dim_minute[date_key] → dim_date_generated[date_key].
WITH spine AS (
    SELECT generate_series(
        '2024-01-01 00:00:00'::timestamp,
        '2030-12-31 23:55:00'::timestamp,
        interval '5 minute'
    ) AS timestamp
)

SELECT
    spine.timestamp,
    spine.timestamp::date AS date_key,
    (EXTRACT(HOUR FROM spine.timestamp) * 60 + EXTRACT(MINUTE FROM spine.timestamp))::int / 5 AS minute_key,
    TO_CHAR(spine.timestamp, 'HH24:MI') AS time_label
FROM spine
ORDER BY spine.timestamp
