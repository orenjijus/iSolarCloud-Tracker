{{ config(
    materialized='table',
    schema='dimensions',
    tags=['dimensions', 'string_layout']
) }}

WITH layout AS (
    SELECT
        source,
        site_id,
        site_name,
        inverter_id AS inverter_id_seed,
        inverter_name AS inverter_name_seed,
        inverter_no,
        mppt_no,
        string_no,
        module_qty,
        orient_code,
        is_active,
        commission_date,
        decommission_date,
        notes
    FROM {{ ref('seed_inverter_string_layout') }}
),
inverter_config AS (
    SELECT
        source::text AS source,
        site_id::text AS site_id,
        site_name,
        inverter_no,
        CASE
            WHEN COALESCE(TRIM(inverter_no::text), '') ~ '^[0-9]+$' THEN TRIM(inverter_no::text)::int
            ELSE NULL
        END AS inverter_no_int,
        NULLIF(TRIM(inverter_sn::text), '') AS inverter_sn,
        NULLIF(TRIM(inverter_id::text), '') AS inverter_id_seed,
        NULLIF(TRIM(inverter_name::text), '') AS inverter_name_seed,
        inverter_ac_capacity_kw,
        inverter_model,
        inverter_manufacturer,
        inverter_dc_capacity_kw,
        mppt_count,
        is_active,
        valid_from,
        valid_to,
        notes
    FROM {{ ref('seed_inverter_config') }}
    WHERE COALESCE(TRIM(is_active::text), 'true') IN ('true', 'TRUE', '1')
),
site_equipment_config AS (
    SELECT
        source::text AS source,
        site_id::text AS site_id,
        site_name,
        NULLIF(TRIM(pv_module_model::text), '') AS pv_module_model,
        NULLIF(TRIM(inverter_model::text), '') AS site_inverter_model,
        is_active,
        valid_from,
        valid_to,
        notes
    FROM {{ ref('seed_site_equipment_config') }}
    WHERE COALESCE(TRIM(is_active::text), 'true') IN ('true', 'TRUE', '1')
),
inverter_model_master AS (
    SELECT
        NULLIF(TRIM(manufacturer::text), '') AS inverter_model_manufacturer,
        NULLIF(TRIM(model::text), '') AS inverter_model,
        p_nom_kw,
        p_max_kw,
        mppt_count,
        input_count
    FROM {{ ref('seed_inverter_model_master') }}
),
pv_module_model_master AS (
    SELECT
        NULLIF(TRIM(manufacturer::text), '') AS pv_module_manufacturer,
        NULLIF(TRIM(model::text), '') AS pv_module_model,
        p_nom,
        year
    FROM {{ ref('seed_pv_module_model_master') }}
),
fusionsolar_inverters AS (
    SELECT
        'FusionSolar' AS source,
        d.plant_code::text AS site_id,
        d.dev_id::text AS inverter_id,
        d.dev_name::text AS inverter_name,
        d.dev_id::text AS inverter_sn,
        (regexp_match(d.dev_name, '([0-9]+)'))[1]::int AS inverter_no_extracted
    FROM {{ ref('stg_fusionsolar__devices') }} d
    WHERE d.dev_type_id = 1
      AND regexp_match(d.dev_name, '([0-9]+)') IS NOT NULL
),
isolarcloud_inverters AS (
    SELECT
        'iSolarCloud' AS source,
        d.ps_id::text AS site_id,
        d.device_ps_key::text AS inverter_id,
        d.device_name::text AS inverter_name,
        NULLIF(TRIM(d.device_sn::text), '') AS inverter_sn,
        (regexp_match(d.device_name, '([0-9]+)'))[1]::int AS inverter_no_extracted
    FROM {{ ref('stg_isolarcloud__devices') }} d
    WHERE d.device_type = 1
      AND regexp_match(d.device_name, '([0-9]+)') IS NOT NULL
),
inverter_candidates AS (
    SELECT * FROM fusionsolar_inverters
    UNION ALL
    SELECT * FROM isolarcloud_inverters
),
ranked_candidates AS (
    SELECT
        c.*,
        COUNT(*) OVER (
            PARTITION BY c.source, c.site_id, c.inverter_no_extracted
        ) AS candidate_count
    FROM inverter_candidates c
),
ranked_candidates_by_sn AS (
    SELECT
        c.*,
        COUNT(*) OVER (
            PARTITION BY c.source, c.site_id, c.inverter_sn
        ) AS candidate_count_by_sn
    FROM inverter_candidates c
    WHERE c.inverter_sn IS NOT NULL
),
mapped AS (
    SELECT
        l.*,
        ic.inverter_sn AS inverter_sn_config,
        ic.inverter_id_seed AS inverter_id_from_config,
        ic.inverter_name_seed AS inverter_name_from_config,
        ic.inverter_ac_capacity_kw,
        ic.inverter_model,
        ic.inverter_manufacturer,
        ic.inverter_dc_capacity_kw,
        ic.mppt_count,
        sec.pv_module_model,
        sec.site_inverter_model,
        imm.inverter_model_manufacturer AS inverter_model_manufacturer_master,
        imm.p_nom_kw AS inverter_ac_capacity_kw_master,
        imm.p_max_kw AS inverter_ac_capacity_max_kw_master,
        imm.mppt_count AS mppt_count_master,
        imm.input_count AS inverter_input_count_master,
        pmm.pv_module_manufacturer,
        pmm.p_nom AS pv_module_p_nom_wp_master,
        pmm.year AS pv_module_model_year,
        rc_cfg_no.inverter_id AS inverter_id_from_config_no,
        rc_cfg_no.inverter_name AS inverter_name_from_config_no,
        rc_cfg_no.inverter_sn AS inverter_sn_from_config_no,
        COALESCE(rc_cfg_no.candidate_count, 0) AS candidate_count_config_no,
        rc_cfg_sn.inverter_id AS inverter_id_from_config_sn,
        rc_cfg_sn.inverter_name AS inverter_name_from_config_sn,
        rc_cfg_sn.inverter_sn AS inverter_sn_from_config_sn,
        COALESCE(rc_cfg_sn.candidate_count_by_sn, 0) AS candidate_count_config_sn,
        rc_auto.inverter_id AS inverter_id_auto,
        rc_auto.inverter_name AS inverter_name_auto,
        rc_auto.inverter_sn AS inverter_sn_auto,
        COALESCE(rc_auto.candidate_count, 0) AS candidate_count_auto
    FROM layout l
    LEFT JOIN inverter_config ic
        ON l.source::text = ic.source::text
       AND l.site_id::text = ic.site_id::text
       AND (
            CASE
                WHEN COALESCE(TRIM(l.inverter_no::text), '') ~ '^[0-9]+$' THEN TRIM(l.inverter_no::text)::int
                ELSE NULL
            END
       ) = ic.inverter_no_int
    LEFT JOIN site_equipment_config sec
        ON l.source::text = sec.source::text
       AND l.site_id::text = sec.site_id::text
    LEFT JOIN inverter_model_master imm
        ON COALESCE(NULLIF(ic.inverter_model::text, ''), sec.site_inverter_model) = imm.inverter_model
    LEFT JOIN pv_module_model_master pmm
        ON sec.pv_module_model = pmm.pv_module_model
    LEFT JOIN ranked_candidates rc_cfg_no
        ON ic.source::text = rc_cfg_no.source::text
       AND ic.site_id::text = rc_cfg_no.site_id::text
       AND ic.inverter_no_int = rc_cfg_no.inverter_no_extracted
       AND rc_cfg_no.candidate_count = 1
    LEFT JOIN ranked_candidates_by_sn rc_cfg_sn
        ON ic.source::text = rc_cfg_sn.source::text
       AND ic.site_id::text = rc_cfg_sn.site_id::text
       AND ic.inverter_sn = rc_cfg_sn.inverter_sn
       AND rc_cfg_sn.candidate_count_by_sn = 1
    LEFT JOIN ranked_candidates rc_auto
        ON l.source = rc_auto.source
       AND l.site_id::text = rc_auto.site_id::text
       AND (
            CASE
                WHEN COALESCE(TRIM(l.inverter_no::text), '') ~ '^[0-9]+$' THEN TRIM(l.inverter_no::text)::int
                ELSE NULL
            END
       ) = rc_auto.inverter_no_extracted
       AND rc_auto.candidate_count = 1
)
SELECT
    source,
    site_id,
    site_name,
    COALESCE(
        inverter_id_from_config,
        inverter_id_from_config_sn,
        inverter_id_from_config_no,
        NULLIF(inverter_id_seed::text, ''),
        inverter_id_auto
    ) AS inverter_id,
    COALESCE(
        inverter_name_from_config,
        inverter_name_from_config_sn,
        inverter_name_from_config_no,
        NULLIF(inverter_name_seed::text, ''),
        inverter_name_auto
    ) AS inverter_name,
    COALESCE(
        inverter_sn_config,
        inverter_sn_from_config_sn,
        inverter_sn_from_config_no,
        inverter_sn_auto
    ) AS inverter_sn,
    inverter_no,
    mppt_no,
    string_no,
    module_qty,
    orient_code,
    COALESCE(NULLIF(inverter_ac_capacity_kw::text, ''), inverter_ac_capacity_kw_master::text)::numeric AS inverter_ac_capacity_kw,
    COALESCE(NULLIF(inverter_model::text, ''), site_inverter_model) AS inverter_model,
    COALESCE(NULLIF(inverter_manufacturer::text, ''), inverter_model_manufacturer_master) AS inverter_manufacturer,
    inverter_dc_capacity_kw,
    COALESCE(NULLIF(mppt_count::text, ''), mppt_count_master::text)::numeric AS mppt_count,
    pv_module_model,
    pv_module_manufacturer,
    pv_module_p_nom_wp_master,
    pv_module_model_year,
    inverter_ac_capacity_max_kw_master,
    inverter_input_count_master,
    is_active,
    commission_date,
    decommission_date,
    notes,
    candidate_count_auto AS candidate_count,
    CASE
        WHEN inverter_id_from_config IS NOT NULL THEN 'config_inverter_id'
        WHEN inverter_id_from_config_sn IS NOT NULL THEN 'config_sn'
        WHEN inverter_id_from_config_no IS NOT NULL THEN 'config_inverter_no'
        WHEN NULLIF(inverter_id_seed::text, '') IS NOT NULL THEN 'seed'
        WHEN candidate_count_auto = 1 THEN 'auto_unique_name_number'
        WHEN candidate_count_auto = 0 THEN 'not_found'
        ELSE 'ambiguous'
    END AS inverter_mapping_status
FROM mapped
