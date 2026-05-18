{{ config(
    materialized='table',
    schema='staging',
    alias='meter_config'
) }}

-- Meter config: device list from raw (sync devices), manual attributes from override seed.
-- Sync devices updates raw; dbt run updates this table. Only override has Revenue, voltage_level, polarity_swapped.
WITH fusionsolar_meters AS (
    SELECT
        'FusionSolar' AS source,
        d.plant_code AS site_id,
        d.dev_name,
        d.esn_code,
        ov.voltage_level,
        ov.meter_type,
        ov.polarity_swapped,
        CASE WHEN UPPER(TRIM(COALESCE(ov.phase_a_swapped::text, ''))) = 'TRUE' THEN TRUE ELSE FALSE END AS phase_a_swapped,
        CASE WHEN UPPER(TRIM(COALESCE(ov.phase_c_swapped::text, ''))) = 'TRUE' THEN TRUE ELSE FALSE END AS phase_c_swapped
    FROM {{ source('raw', 'fusionsolar_devices') }} d
    LEFT JOIN {{ ref('seed_meter_config_override') }} ov
        ON TRIM(ov.source) = 'FusionSolar' AND ov.esn_code = d.esn_code
    WHERE d.dev_type_id = 17
),

isolarcloud_meters AS (
    SELECT
        'iSolarCloud' AS source,
        d.ps_id::VARCHAR AS site_id,
        d.device_name AS dev_name,
        d.device_ps_key AS esn_code,
        ov.voltage_level,
        ov.meter_type,
        ov.polarity_swapped,
        FALSE AS phase_a_swapped,
        FALSE AS phase_c_swapped
    FROM {{ source('raw', 'isolarcloud_devices') }} d
    LEFT JOIN {{ ref('seed_meter_config_override') }} ov
        ON TRIM(ov.source) = 'iSolarCloud' AND ov.esn_code = d.device_ps_key
    WHERE d.device_type = 7
)

SELECT source AS "Source", site_id, dev_name, esn_code, voltage_level, meter_type, polarity_swapped, phase_a_swapped, phase_c_swapped
FROM fusionsolar_meters
UNION ALL
SELECT source AS "Source", site_id, dev_name, esn_code, voltage_level, meter_type, polarity_swapped, phase_a_swapped, phase_c_swapped
FROM isolarcloud_meters
