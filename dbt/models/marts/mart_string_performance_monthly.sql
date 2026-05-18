{{ config(
    materialized='table',
    schema='mart'
) }}

-- Reconstructed model (restored): Monthly string performance from daily mart.
-- Grain: year x month x inverter_id x string_number

WITH daily AS (
    SELECT
        date_key,
        EXTRACT(YEAR FROM date_key)::int AS year,
        EXTRACT(MONTH FROM date_key)::int AS month,
        inverter_id,
        inverter_name,
        site_id,
        site_name,
        system,
        string_number,
        string_id,
        daily_energy_wh,
        daily_energy_kwh,
        interval_count_5min,
        layout_module_qty,
        layout_orient_code,
        string_dc_capacity_kw_stc,
        daily_ghi_kwh_m2,
        daily_poa_kwh_m2
    FROM {{ ref('mart_string_performance_daily') }}
),

monthly AS (
    SELECT
        year,
        month,
        inverter_id,
        inverter_name,
        site_id,
        site_name,
        system,
        string_number,
        string_id,
        SUM(daily_energy_wh) AS monthly_energy_wh,
        SUM(daily_energy_kwh) AS monthly_energy_kwh,
        SUM(interval_count_5min) AS interval_count_5min,
        MAX(layout_module_qty) AS layout_module_qty,
        MAX(layout_orient_code) AS layout_orient_code,
        MAX(string_dc_capacity_kw_stc) AS string_dc_capacity_kw_stc,
        SUM(COALESCE(daily_ghi_kwh_m2, 0)) AS monthly_ghi_kwh_m2,
        SUM(COALESCE(daily_poa_kwh_m2, 0)) AS monthly_poa_kwh_m2
    FROM daily
    GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT
    year,
    month,
    inverter_id,
    inverter_name,
    site_id,
    site_name,
    system,
    string_number,
    string_id,
    CAST(monthly_energy_wh AS numeric(38,10)) AS monthly_energy_wh,
    CAST(monthly_energy_kwh AS numeric(38,12)) AS monthly_energy_kwh,
    interval_count_5min,
    layout_module_qty,
    layout_orient_code,
    CAST(string_dc_capacity_kw_stc AS numeric(18,6)) AS string_dc_capacity_kw_stc,
    CAST(monthly_ghi_kwh_m2 AS numeric(18,6)) AS monthly_ghi_kwh_m2,
    CAST(monthly_poa_kwh_m2 AS numeric(18,6)) AS monthly_poa_kwh_m2,
    CASE
        WHEN monthly_ghi_kwh_m2 > 0
         AND monthly_energy_kwh > 0
         AND string_dc_capacity_kw_stc > 0
        THEN LEAST(
            CAST((monthly_energy_kwh / 1000.0)
                / NULLIF(monthly_ghi_kwh_m2 * string_dc_capacity_kw_stc, 0) AS numeric(18,6)),
            10.0
        )
        ELSE NULL
    END AS pr_ghi_string,
    CASE
        WHEN monthly_poa_kwh_m2 > 0
         AND monthly_energy_kwh > 0
         AND string_dc_capacity_kw_stc > 0
        THEN LEAST(
            CAST((monthly_energy_kwh / 1000.0)
                / NULLIF(monthly_poa_kwh_m2 * string_dc_capacity_kw_stc, 0) AS numeric(18,6)),
            10.0
        )
        ELSE NULL
    END AS pr_poa_string
FROM monthly
