{{ config(
    materialized='incremental',
    schema='mart',
    unique_key=['date_key', 'inverter_id', 'string_number'],
    on_schema_change='sync_all_columns'
) }}

-- Reconstructed model (restored): Daily string performance.
-- Grain: date_key x inverter_id x string_number
-- Main outputs: daily_energy_wh, daily_energy_kwh, daily_ghi_kwh_m2, daily_poa_kwh_m2, pr_ghi_string, pr_poa_string

WITH watermark AS (
    SELECT
    {% if is_incremental() %}
        COALESCE(MAX(date_key), DATE '2020-01-01') AS start_date
    FROM {{ this }}
    {% else %}
        DATE '2020-01-01' AS start_date
    {% endif %}
),

fs_string_raw AS (
    SELECT
        p.timestamp,
        p.dev_id,
        p.device_name,
        p.plant_code,
        p.plant_name,
        p.metric_id,
        p.metric_value::numeric AS metric_value
    FROM "{{ target.database }}"."staging"."stg_fusionsolar__perf_unpivoted" p
    JOIN "{{ target.database }}"."staging"."stg_fusionsolar__devices" d
      ON d.dev_id = p.dev_id
    CROSS JOIN watermark w
    WHERE d.device_category = 'Inverter'
      AND DATE(p.timestamp) >= w.start_date
),

iso_string_raw AS (
    SELECT
        p.timestamp,
        p.device_ps_key,
        p.device_name,
        p.ps_id,
        p.ps_name,
        p.metric_id,
        p.metric_value::numeric AS metric_value
    FROM "{{ target.database }}"."staging"."stg_isolarcloud__perf_unpivoted" p
    JOIN "{{ target.database }}"."staging"."stg_isolarcloud__devices" d
      ON d.device_ps_key = p.device_ps_key
    CROSS JOIN watermark w
    WHERE d.device_category = 'Inverter'
      AND DATE(p.timestamp) >= w.start_date
),

fs_string_metrics AS (
    SELECT
        r.timestamp,
        DATE(r.timestamp) AS date_key,
        CONCAT('FS_', r.dev_id) AS inverter_id,
        r.device_name AS inverter_name,
        r.plant_code AS site_id,
        r.plant_name AS site_name,
        'fusionsolar'::text AS system,
        CAST((regexp_match(mm.unified_name, 'string_(\d+)_(voltage|current)'))[1] AS integer) AS string_number,
        CASE
            WHEN mm.unified_name LIKE 'string_%_voltage' THEN 'voltage'
            WHEN mm.unified_name LIKE 'string_%_current' THEN 'current'
            ELSE NULL
        END AS metric_type,
        r.metric_value
    FROM fs_string_raw r
    JOIN {{ ref('seed_metric_mapper') }} mm
      ON mm.platform = 'fusionsolar'
     AND mm.metric_id = r.metric_id
    WHERE mm.unified_name ~ '^string_[0-9]+_(voltage|current)$'
),

iso_string_metrics AS (
    SELECT
        r.timestamp,
        DATE(r.timestamp) AS date_key,
        CONCAT('ISO_', r.device_ps_key) AS inverter_id,
        r.device_name AS inverter_name,
        r.ps_id AS site_id,
        r.ps_name AS site_name,
        'isolarcloud'::text AS system,
        CAST((regexp_match(mm.unified_name, 'string_(\d+)_(voltage|current)'))[1] AS integer) AS string_number,
        CASE
            WHEN mm.unified_name LIKE 'string_%_voltage' THEN 'voltage'
            WHEN mm.unified_name LIKE 'string_%_current' THEN 'current'
            ELSE NULL
        END AS metric_type,
        r.metric_value
    FROM iso_string_raw r
    JOIN {{ ref('seed_metric_mapper') }} mm
      ON mm.platform = 'isolarcloud'
     AND mm.metric_id = r.metric_id
    WHERE mm.unified_name ~ '^string_[0-9]+_(voltage|current)$'
),

string_metrics AS (
    SELECT * FROM fs_string_metrics
    UNION ALL
    SELECT * FROM iso_string_metrics
),

pivot_5min AS (
    SELECT
        timestamp,
        date_key,
        inverter_id,
        inverter_name,
        site_id,
        site_name,
        system,
        string_number,
        MAX(CASE WHEN metric_type = 'voltage' THEN metric_value END) AS string_voltage_v,
        MAX(CASE WHEN metric_type = 'current' THEN metric_value END) AS string_current_a
    FROM string_metrics
    WHERE string_number IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

energy_5min AS (
    SELECT
        date_key,
        inverter_id,
        inverter_name,
        site_id,
        site_name,
        system,
        string_number,
        -- E(Wh) = V * I * dt(hour), dt = 5/60
        COALESCE(string_voltage_v, 0) * COALESCE(string_current_a, 0) * (5.0 / 60.0) AS energy_wh_5min,
        CASE
            WHEN string_voltage_v IS NOT NULL AND string_current_a IS NOT NULL THEN 1
            ELSE 0
        END AS valid_interval
    FROM pivot_5min
),

daily_string_energy AS (
    SELECT
        date_key,
        inverter_id,
        inverter_name,
        site_id,
        site_name,
        system,
        string_number,
        SUM(energy_wh_5min) AS daily_energy_wh,
        SUM(energy_wh_5min) / 1000.0 AS daily_energy_kwh,
        SUM(valid_interval) AS interval_count_5min
    FROM energy_5min
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- Canonical string layout: staging.seed_inverter_string_layout (module_qty, orient_code).
-- Telemetry join key: COALESCE(seed.inverter_id, seed_inverter_config.inverter_id) must match
-- REGEXP_REPLACE(mart.inverter_id, '^(FS_|ISO_)', '').
module_wp_by_site AS (
    SELECT
        site_id::text AS site_id,
        MAX(pv_module_p_nom_wp_master) AS module_wp
    FROM "{{ target.database }}"."dimensions"."dim_inverter_string_layout"
    WHERE COALESCE(is_active, TRUE) = TRUE
      AND pv_module_p_nom_wp_master IS NOT NULL
    GROUP BY 1
),

module_wp_override AS (
    SELECT
        site_id::text AS site_id,
        inverter_id::text AS inverter_id,
        string_no AS string_number,
        MAX(module_wp_override) AS module_wp_override
    FROM "{{ target.database }}"."staging"."seed_string_module_override"
    GROUP BY 1, 2, 3
),

layout_dedup AS (
    SELECT
        s.site_id::text AS site_id,
        s.inverter_no,
        s.string_no AS string_number,
        MAX(s.module_qty) AS layout_module_qty,
        MAX(s.orient_code) AS layout_orient_code,
        MAX(NULLIF(TRIM(s.inverter_id::text), '')) AS seed_inverter_id,
        MAX(NULLIF(TRIM(c.inverter_id::text), '')) AS cfg_inverter_id
    FROM "{{ target.database }}"."staging"."seed_inverter_string_layout" s
    LEFT JOIN "{{ target.database }}"."staging"."seed_inverter_config" c
      ON c.site_id::text = s.site_id::text
     AND c.inverter_no = s.inverter_no
    WHERE COALESCE(s.is_active, TRUE) = TRUE
      AND s.string_no IS NOT NULL
    GROUP BY s.site_id::text, s.inverter_no, s.string_no
),

layout_capacity AS (
    SELECT
        d.site_id,
        COALESCE(d.seed_inverter_id, d.cfg_inverter_id) AS layout_inverter_key,
        d.string_number,
        d.layout_module_qty,
        d.layout_orient_code,
        (
            COALESCE(d.layout_module_qty, 0)
            * COALESCE(
                ov.module_wp_override,
                mw.module_wp,
                0
            )
        ) / 1000.0 AS string_dc_capacity_kw_stc
    FROM layout_dedup d
    LEFT JOIN module_wp_by_site mw
      ON mw.site_id = d.site_id
    LEFT JOIN module_wp_override ov
      ON ov.site_id = d.site_id
     AND ov.inverter_id = COALESCE(d.seed_inverter_id, d.cfg_inverter_id)
     AND ov.string_number = d.string_number
),

-- Match POA sensors: seeds often store bare device key; mart_sensor_daily uses FS_ / ISO_ prefixes.
poa_by_site_orient AS (
    SELECT
        psc.site_id,
        psc.orient_code,
        sd.date_key,
        AVG(sd.daily_irradiance_kwh_m2) AS daily_poa_kwh_m2
    FROM "{{ target.database }}"."staging"."seed_poa_sensor_config" psc
    JOIN {{ ref('mart_sensor_daily') }} sd
      ON sd.sensor_type = 'POA'
     AND (
            sd.asset_id::text = psc.poa_sensor_id::text
         OR sd.asset_id::text = CONCAT('FS_', REGEXP_REPLACE(psc.poa_sensor_id::text, '^(FS_|ISO_)', ''))
         OR sd.asset_id::text = CONCAT('ISO_', REGEXP_REPLACE(psc.poa_sensor_id::text, '^(FS_|ISO_)', ''))
        )
    WHERE COALESCE(psc.is_active, TRUE) = TRUE
      AND sd.daily_irradiance_kwh_m2 IS NOT NULL
    GROUP BY 1, 2, 3
),

poa_by_site AS (
    SELECT
        psc.site_id,
        sd.date_key,
        AVG(sd.daily_irradiance_kwh_m2) AS daily_poa_site_kwh_m2
    FROM "{{ target.database }}"."staging"."seed_poa_sensor_config" psc
    JOIN {{ ref('mart_sensor_daily') }} sd
      ON sd.sensor_type = 'POA'
     AND (
            sd.asset_id::text = psc.poa_sensor_id::text
         OR sd.asset_id::text = CONCAT('FS_', REGEXP_REPLACE(psc.poa_sensor_id::text, '^(FS_|ISO_)', ''))
         OR sd.asset_id::text = CONCAT('ISO_', REGEXP_REPLACE(psc.poa_sensor_id::text, '^(FS_|ISO_)', ''))
        )
    WHERE COALESCE(psc.is_active, TRUE) = TRUE
      AND sd.daily_irradiance_kwh_m2 IS NOT NULL
    GROUP BY 1, 2
),

site_irradiance AS (
    SELECT
        date_key,
        site_id,
        site_name,
        daily_ghi_kwh_m2
    FROM {{ ref('mart_site_performance_daily') }} si
    CROSS JOIN watermark w
    WHERE si.date_key >= w.start_date
),

site_canonical AS (
    SELECT
        d.date_key,
        d.inverter_id,
        d.inverter_name,
        d.site_id AS source_site_id,
        d.site_name,
        d.system,
        d.string_number,
        COALESCE(da.asset_id, d.site_id) AS canonical_site_id
    FROM daily_string_energy d
    LEFT JOIN "{{ target.database }}"."dimensions"."dim_assets" da
      ON da.asset_level = 'Site'
     AND UPPER(TRIM(da.site_name)) = UPPER(TRIM(d.site_name))
),

base AS (
    SELECT
        d.date_key,
        d.inverter_id,
        d.inverter_name,
        d.source_site_id AS site_id,
        d.site_name,
        d.system,
        d.string_number,
        CONCAT(d.inverter_id, '_STR', LPAD(d.string_number::text, 2, '0')) AS string_id,
        CAST(e.daily_energy_wh AS numeric(38,10)) AS daily_energy_wh,
        CAST(e.daily_energy_kwh AS numeric(38,12)) AS daily_energy_kwh,
        e.interval_count_5min,
        l.layout_module_qty,
        l.layout_orient_code,
        CAST(l.string_dc_capacity_kw_stc AS numeric(18,6)) AS string_dc_capacity_kw_stc,
        -- Prefer exact site_id join, fallback to site_name
        COALESCE(
            si_id.daily_ghi_kwh_m2,
            si_name.daily_ghi_kwh_m2
        ) AS daily_ghi_kwh_m2,
        COALESCE(
            poa.daily_poa_kwh_m2,
            poa_site.daily_poa_site_kwh_m2
        ) AS daily_poa_kwh_m2
    FROM site_canonical d
    JOIN daily_string_energy e
      ON e.date_key = d.date_key
     AND e.inverter_id = d.inverter_id
     AND e.string_number = d.string_number
    LEFT JOIN layout_capacity l
      ON l.layout_inverter_key = REGEXP_REPLACE(d.inverter_id, '^(FS_|ISO_)', '')
     AND l.string_number = d.string_number
     AND (
           l.site_id::text IN (d.canonical_site_id::text, d.source_site_id::text)
        OR REGEXP_REPLACE(l.site_id::text, '^(FS_|ISO_)', '') IN (
               REGEXP_REPLACE(d.canonical_site_id::text, '^(FS_|ISO_)', ''),
               REGEXP_REPLACE(d.source_site_id::text, '^(FS_|ISO_)', '')
           )
         )
    LEFT JOIN site_irradiance si_id
      ON si_id.date_key = d.date_key
     AND si_id.site_id = d.canonical_site_id
    LEFT JOIN site_irradiance si_name
      ON si_name.date_key = d.date_key
     AND UPPER(TRIM(si_name.site_name)) = UPPER(TRIM(d.site_name))
    LEFT JOIN poa_by_site_orient poa
      ON poa.date_key = d.date_key
     AND poa.orient_code = l.layout_orient_code
     AND (
           poa.site_id::text IN (d.canonical_site_id::text, d.source_site_id::text)
        OR REGEXP_REPLACE(poa.site_id::text, '^(FS_|ISO_)', '') IN (
               REGEXP_REPLACE(d.canonical_site_id::text, '^(FS_|ISO_)', ''),
               REGEXP_REPLACE(d.source_site_id::text, '^(FS_|ISO_)', '')
           )
         )
    LEFT JOIN poa_by_site poa_site
      ON poa_site.date_key = d.date_key
     AND (
           poa_site.site_id::text IN (d.canonical_site_id::text, d.source_site_id::text)
        OR REGEXP_REPLACE(poa_site.site_id::text, '^(FS_|ISO_)', '') IN (
               REGEXP_REPLACE(d.canonical_site_id::text, '^(FS_|ISO_)', ''),
               REGEXP_REPLACE(d.source_site_id::text, '^(FS_|ISO_)', '')
           )
         )
)

SELECT
    date_key,
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
    CAST(daily_ghi_kwh_m2 AS numeric(18,6)) AS daily_ghi_kwh_m2,
    CAST(daily_poa_kwh_m2 AS numeric(18,6)) AS daily_poa_kwh_m2,
    CASE
        WHEN daily_ghi_kwh_m2 IS NOT NULL
         AND daily_ghi_kwh_m2 >= 0.1
         AND daily_energy_kwh IS NOT NULL
         AND daily_energy_kwh >= 0.01
         AND string_dc_capacity_kw_stc IS NOT NULL
         AND string_dc_capacity_kw_stc > 0
        THEN LEAST(
            CAST((daily_energy_kwh / 1000.0)
                / NULLIF(daily_ghi_kwh_m2 * string_dc_capacity_kw_stc, 0) AS numeric(18,6)),
            10.0
        )
        ELSE NULL
    END AS pr_ghi_string,
    CASE
        WHEN daily_poa_kwh_m2 IS NOT NULL
         AND daily_poa_kwh_m2 >= 0.1
         AND daily_energy_kwh IS NOT NULL
         AND daily_energy_kwh >= 0.01
         AND string_dc_capacity_kw_stc IS NOT NULL
         AND string_dc_capacity_kw_stc > 0
        THEN LEAST(
            CAST((daily_energy_kwh / 1000.0)
                / NULLIF(daily_poa_kwh_m2 * string_dc_capacity_kw_stc, 0) AS numeric(18,6)),
            10.0
        )
        ELSE NULL
    END AS pr_poa_string
FROM base
