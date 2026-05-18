/*
  Site Device Configuration Registry (read-only analysis)

  Purpose:
    Satu query untuk melihat baseline device + override + rentang berlaku + alasan,
    tanpa membuka banyak seed CSV terpisah.

  Usage (psql / DBeaver / MCP):
    -- Ganti nama site di CTE params, atau comment filter untuk semua site
    -- Contoh: Shoetown + CP Majalengka

  Layers:
    1. dim_assets_live     = apa yang terlihat di API hari ini
    2. seed_*_config       = baseline manual (tipe meter/sensor/inverter)
    3. seed_*_site_mapping = override temporal (POA_OVERRIDE, GHI_*, dll.)
    4. seed_*_adjustment   = koreksi harian poin (energy/ghi)
*/

WITH params AS (
    SELECT NULL::text AS site_name_filter
    -- Contoh filter satu site:
    -- SELECT 'Shoetown Ligung Indonesia'::text AS site_name_filter
    -- SELECT 'Charoen Pokphand Majalengka'::text AS site_name_filter
),

-- ---------------------------------------------------------------------------
-- Live devices from dimension (ingest/API)
-- ---------------------------------------------------------------------------
dim_live AS (
    SELECT
        da.site_name,
        da.asset_id,
        da.system,
        da.asset_level,
        'dim_assets_live' AS config_layer,
        CASE
            WHEN da.asset_id LIKE '%\_7\_%' ESCAPE '\' THEN 'meter'
            WHEN da.asset_id LIKE '%\_5\_%' ESCAPE '\' THEN 'sensor'
            WHEN da.asset_id LIKE '%\_1\_%' ESCAPE '\' THEN 'inverter'
            ELSE 'other'
        END AS device_category,
        da.asset_id AS device_ref,
        'API / dim_assets' AS role_description,
        NULL::date AS effective_date_start,
        NULL::date AS effective_date_end,
        'Device terdaftar di portal & dim_assets (kondisi live)' AS notes,
        TRUE AS is_baseline
    FROM dimensions.dim_assets da
    CROSS JOIN params p
    WHERE da.asset_level = 'Device'
      AND (p.site_name_filter IS NULL OR da.site_name = p.site_name_filter)
),

-- ---------------------------------------------------------------------------
-- Baseline: meters
-- ---------------------------------------------------------------------------
meter_baseline AS (
    SELECT
        mc.dev_name AS site_name,
        CONCAT('ISO_', mc.esn_code) AS asset_id,
        'isolarcloud' AS system,
        'Device' AS asset_level,
        'seed_meter_config' AS config_layer,
        'meter' AS device_category,
        mc.esn_code AS device_ref,
        CONCAT('meter_type=', COALESCE(mc.meter_type, '(unset)'),
               CASE WHEN UPPER(TRIM(COALESCE(mc.polarity_swapped::text, ''))) = 'TRUE'
                    THEN ', polarity_swapped=TRUE' ELSE '' END) AS role_description,
        NULL::date AS effective_date_start,
        NULL::date AS effective_date_end,
        'Baseline meter dari seed_meter_config' AS notes,
        TRUE AS is_baseline
    FROM staging.seed_meter_config mc
    CROSS JOIN params p
    WHERE (p.site_name_filter IS NULL OR mc.dev_name = p.site_name_filter)
),

-- ---------------------------------------------------------------------------
-- Baseline: sensors
-- ---------------------------------------------------------------------------
sensor_baseline AS (
    SELECT
        sc.dev_name AS site_name,
        CONCAT(
            CASE WHEN sc.source ILIKE '%fusion%' THEN 'FS_' ELSE 'ISO_' END,
            sc.device_id
        ) AS asset_id,
        CASE WHEN sc.source ILIKE '%fusion%' THEN 'fusionsolar' ELSE 'isolarcloud' END AS system,
        'Device' AS asset_level,
        'seed_sensor_config' AS config_layer,
        'sensor' AS device_category,
        sc.device_id AS device_ref,
        CONCAT('sensor_type=', COALESCE(sc.sensor_type, '(unset)'),
               CASE WHEN sc.sensor_capacity IS NOT NULL AND TRIM(sc.sensor_capacity::text) != ''
                    THEN ', capacity=' || sc.sensor_capacity::text ELSE '' END) AS role_description,
        NULL::date AS effective_date_start,
        NULL::date AS effective_date_end,
        'Baseline sensor dari seed_sensor_config' AS notes,
        TRUE AS is_baseline
    FROM staging.seed_sensor_config sc
    CROSS JOIN params p
    WHERE (p.site_name_filter IS NULL OR sc.dev_name = p.site_name_filter)
),

-- ---------------------------------------------------------------------------
-- Baseline: inverters
-- ---------------------------------------------------------------------------
inverter_baseline AS (
    SELECT
        ic.site_name,
        CONCAT(
            CASE WHEN ic.source ILIKE '%fusion%' THEN 'FS_' ELSE 'ISO_' END,
            COALESCE(ic.inverter_id, ic.inverter_sn)
        ) AS asset_id,
        CASE WHEN ic.source ILIKE '%fusion%' THEN 'fusionsolar' ELSE 'isolarcloud' END AS system,
        'Device' AS asset_level,
        'seed_inverter_config' AS config_layer,
        'inverter' AS device_category,
        COALESCE(ic.inverter_id, ic.inverter_sn) AS device_ref,
        CONCAT('inverter_no=', ic.inverter_no::text,
               ', active=', COALESCE(ic.is_active::text, 'null')) AS role_description,
        ic.valid_from::date AS effective_date_start,
        ic.valid_to::date AS effective_date_end,
        COALESCE(ic.notes, 'Baseline inverter dari seed_inverter_config') AS notes,
        TRUE AS is_baseline
    FROM staging.seed_inverter_config ic
    CROSS JOIN params p
    WHERE (p.site_name_filter IS NULL OR ic.site_name = p.site_name_filter)
),

-- ---------------------------------------------------------------------------
-- Temporal overrides: sensor site mapping (GHI / POA logic)
-- ---------------------------------------------------------------------------
sensor_mapping AS (
    SELECT
        ssm.device_id AS site_name,
        CASE
            WHEN ssm.logical_site_id LIKE 'ISO_%' OR ssm.logical_site_id LIKE 'FS_%'
                THEN ssm.logical_site_id
            ELSE NULL
        END AS asset_id,
        NULL::text AS system,
        'Site' AS asset_level,
        'seed_sensor_site_mapping' AS config_layer,
        'sensor_logic' AS device_category,
        ssm.logical_site_id AS device_ref,
        ssm.mapping_type AS role_description,
        ssm.effective_date_start::date AS effective_date_start,
        ssm.effective_date_end::date AS effective_date_end,
        ssm.notes,
        FALSE AS is_baseline
    FROM staging.seed_sensor_site_mapping ssm
    CROSS JOIN params p
    WHERE (p.site_name_filter IS NULL OR ssm.device_id = p.site_name_filter)
),

-- ---------------------------------------------------------------------------
-- Point adjustments (optional trace)
-- ---------------------------------------------------------------------------
ghi_adjustment AS (
    SELECT
        g.site_name,
        NULL::text AS asset_id,
        NULL::text AS system,
        'Site' AS asset_level,
        'seed_ghi_adjustment_daily' AS config_layer,
        'sensor_logic' AS device_category,
        g.ghi_adjusted_value::text AS device_ref,
        'GHI_ADJUSTMENT_DAILY' AS role_description,
        g.date_key::date AS effective_date_start,
        g.date_key::date AS effective_date_end,
        COALESCE(g.notes, 'Manual GHI adjustment harian') AS notes,
        FALSE AS is_baseline
    FROM staging.seed_ghi_adjustment_daily g
    CROSS JOIN params p
    WHERE (p.site_name_filter IS NULL OR TRIM(g.site_name) = p.site_name_filter)
),

energy_adjustment AS (
    SELECT
        e.site_name,
        NULL::text AS asset_id,
        NULL::text AS system,
        'Site' AS asset_level,
        'seed_energy_adjustment_daily' AS config_layer,
        'meter_logic' AS device_category,
        e.energy_adjusted_value::text AS device_ref,
        'ENERGY_ADJUSTMENT_DAILY' AS role_description,
        e.date_key::date AS effective_date_start,
        e.date_key::date AS effective_date_end,
        COALESCE(e.notes, 'Manual energy adjustment harian') AS notes,
        FALSE AS is_baseline
    FROM staging.seed_energy_adjustment_daily e
    CROSS JOIN params p
    WHERE (p.site_name_filter IS NULL OR TRIM(e.site_name) = p.site_name_filter)
),

unioned AS (
    SELECT * FROM dim_live
    UNION ALL SELECT * FROM meter_baseline
    UNION ALL SELECT * FROM sensor_baseline
    UNION ALL SELECT * FROM inverter_baseline
    UNION ALL SELECT * FROM sensor_mapping
    UNION ALL SELECT * FROM ghi_adjustment
    UNION ALL SELECT * FROM energy_adjustment
),

with_status AS (
    SELECT
        u.*,
        CASE
            WHEN u.effective_date_start IS NULL AND u.effective_date_end IS NULL THEN 'always'
            WHEN u.effective_date_start IS NOT NULL
                 AND u.effective_date_start <= CURRENT_DATE
                 AND (u.effective_date_end IS NULL OR u.effective_date_end >= CURRENT_DATE)
                THEN 'active_today'
            WHEN u.effective_date_end IS NOT NULL AND u.effective_date_end < CURRENT_DATE THEN 'expired'
            WHEN u.effective_date_start IS NOT NULL AND u.effective_date_start > CURRENT_DATE THEN 'future'
            ELSE 'check_dates'
        END AS validity_status
    FROM unioned u
)

SELECT
    site_name,
    device_category,
    config_layer,
    role_description,
    device_ref,
    asset_id,
    effective_date_start,
    effective_date_end,
    validity_status,
    is_baseline,
    notes
FROM with_status
ORDER BY
    site_name,
    device_category,
    config_layer,
    effective_date_start NULLS FIRST,
    device_ref;
