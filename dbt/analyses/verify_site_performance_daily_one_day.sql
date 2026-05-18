/*
  Verifikasi satu site — satu hari — mart_site_performance_daily

  Cara pakai:
    1. Set site_name dan check_date di CTE params.
    2. Jalankan seluruh file.
    3. Bandingkan blok "mart_output" dengan komponen upstream di blok lain.

  Tujuan: user/Ops bisa mengoreksi seed tanpa membaca SQL model dbt.
*/

WITH params AS (
    SELECT
        'Shoetown Ligung Indonesia'::text AS site_name,
        '2026-05-12'::date AS check_date
),

-- Output mart (yang dilihat di Power BI)
mart_output AS (
    SELECT
        m.date_key,
        m.site_name,
        m.site_id,
        m.daily_energy_mwh,
        m.energy_actual,
        m.energy_adjusted,
        m.daily_ghi_kwh_m2,
        m.ghi_actual,
        m.ghi_adjusted,
        m.daily_poa_weighted_kwh_m2,
        m.power_available_hours,
        m.unavailability_hours,
        m.mit_hours,
        m.availability_percent,
        m.pr_ghi_actual,
        m.pr_adjusted,
        m.pr_poa_actual,
        m.energy_target_mwh,
        m.ghi_target,
        m.poa_target,
        m.energy_actual_vs_target_pct,
        m.energy_kpi_daily_mwh,
        m.energy_a_kpi_daily_mwh,
        m.energy_kpi_monthly_mwh,
        m.energy_target_monthly_mwh,
        m.energy_simulation_monthly_mwh,
        m.actual_capacity_kw
    FROM mart.mart_site_performance_daily m
    CROSS JOIN params p
    WHERE m.site_name = p.site_name
      AND m.date_key::date = p.check_date
),

-- Komponen KPI / target (sumber a_KPI)
kpi_target_components AS (
    SELECT
        p.check_date,
        p.site_name,
        t.energy_target_mwh AS target_daily_mwh,
        t.ghi AS ghi_target_daily,
        t.poa AS poa_target_daily,
        mkpi.energy_kpi_mwh AS kpi_monthly_mwh,
        SUM(t.energy_target_mwh) OVER (
            PARTITION BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
        ) AS target_monthly_mwh_calc,
        SUM(t.energy_simulation_mwh) OVER (
            PARTITION BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
        ) AS simulation_monthly_mwh_calc,
        CASE
            WHEN SUM(t.energy_target_mwh) OVER (
                PARTITION BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
            ) > 0
            AND mkpi.energy_kpi_mwh IS NOT NULL
            THEN mkpi.energy_kpi_mwh * (
                t.energy_target_mwh / SUM(t.energy_target_mwh) OVER (
                    PARTITION BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
                )
            )
        END AS kpi_daily_recalc_mwh,
        CASE
            WHEN SUM(t.energy_simulation_mwh) OVER (
                PARTITION BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
            ) > 0
            AND mkpi.energy_kpi_mwh IS NOT NULL
            THEN t.energy_target_mwh * (
                mkpi.energy_kpi_mwh / SUM(t.energy_simulation_mwh) OVER (
                    PARTITION BY COALESCE(t.site_code, da.site_id::text), dd.year, dd.month
                )
            )
        END AS a_kpi_daily_recalc_mwh
    FROM params p
    JOIN mart.mart_simulation_targets_daily t
        ON t.date_key::date = p.check_date
    LEFT JOIN dimensions.dim_assets da
        ON (da.site_id::text = t.site_code::text OR da.site_name = t.site_name)
        AND da.asset_level = 'Site'
    LEFT JOIN dimensions.dim_date_generated dd ON dd.date_key = t.date_key::date
    LEFT JOIN mart.mart_site_kpi_monthly mkpi
        ON mkpi.site_name = p.site_name
        AND mkpi.year = dd.year
        AND mkpi.month = dd.month
    WHERE COALESCE(t.site_name, da.site_name) = p.site_name
    LIMIT 1
),

-- Sensor override aktif pada tanggal cek
sensor_overrides AS (
    SELECT
        ssm.mapping_type,
        ssm.device_id AS target_site,
        ssm.logical_site_id AS source_ref,
        ssm.effective_date_start,
        ssm.effective_date_end,
        ssm.notes
    FROM staging.seed_sensor_site_mapping ssm
    CROSS JOIN params p
    WHERE ssm.device_id = p.site_name
      AND (ssm.effective_date_end IS NULL OR ssm.effective_date_end::date >= p.check_date)
      AND (ssm.effective_date_start IS NULL OR ssm.effective_date_start::date <= p.check_date)
)

SELECT '1_mart_output' AS section, to_jsonb(mo.*) AS payload
FROM mart_output mo
UNION ALL
SELECT '2_kpi_target_recalc', to_jsonb(kt.*)
FROM kpi_target_components kt
UNION ALL
SELECT '3_sensor_overrides_active', jsonb_agg(to_jsonb(so.*))
FROM sensor_overrides so;
