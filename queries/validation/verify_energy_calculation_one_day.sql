-- ============================================
-- Verification Query: Energy Calculation for One Day
-- ============================================
-- Query ini memverifikasi perhitungan energy untuk satu hari
-- dimana GHI dan POA sudah match, sehingga kita bisa fokus
-- melihat apakah perhitungan energy sudah benar setelah polarity swap
-- ============================================

-- Step 1: Find a day where GHI and POA match for MMKI II
WITH matched_days AS (
    SELECT 
        db.date_key,
        db.site_name,
        db.daily_energy_mwh as db_energy,
        db.daily_ghi_kwh_m2 as db_ghi,
        db.daily_poa_weighted_kwh_m2 as db_poa,
        db.pr_ghi_actual as db_pr_ghi,
        db.pr_poa_actual as db_pr_poa,
        excel.daily_energy_mwh as excel_energy,
        excel.daily_ghi_kwh_m2 as excel_ghi,
        excel.daily_poa_weighted_kwh_m2 as excel_poa,
        excel.pr_ghi_actual as excel_pr_ghi,
        excel.pr_poa_actual as excel_pr_poa,
        ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) as ghi_diff,
        ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff,
        ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) as energy_diff
    FROM "MMSR"."mart"."mart_site_performance_daily" db
    LEFT JOIN (
        SELECT 
            TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
            site_name,
            energy_actual_mwh::NUMERIC as daily_energy_mwh,
            ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
            poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2,
            CASE 
                WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
                ELSE CAST(REPLACE(REPLACE(pr_ghi_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
            END as pr_ghi_actual,
            CASE 
                WHEN pr_poa_actual IS NULL OR TRIM(pr_poa_actual) = '' THEN NULL
                ELSE CAST(REPLACE(REPLACE(pr_poa_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
            END as pr_poa_actual
        FROM public.site_daily_performance_excel_mmki
    ) excel ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
    WHERE db.site_name = 'PT. MMKI 5.7 MWp - Phase 2'
        AND db.date_key >= '2025-01-01'
        AND db.date_key <= '2025-11-13'
        -- GHI and POA must match (within 0.01 tolerance)
        AND ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) <= 0.01
        AND ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) <= 0.01
        -- Both must have values
        AND db.daily_ghi_kwh_m2 IS NOT NULL
        AND db.daily_poa_weighted_kwh_m2 IS NOT NULL
        AND excel.daily_ghi_kwh_m2 IS NOT NULL
        AND excel.daily_poa_weighted_kwh_m2 IS NOT NULL
    ORDER BY energy_diff DESC
    LIMIT 1
),

-- Step 2: Get detailed meter data for that day
meter_details AS (
    SELECT 
        m.date_key,
        m.site_name,
        m.asset_id,
        mc.dev_name,
        mc.esn_code,
        -- Hardcode polarity_swapped for now (until seed is reloaded)
        CASE 
            WHEN mc.esn_code = 'AM001023C7355634' THEN 'TRUE'  -- EM-MVMDP-PV3
            ELSE ''
        END as polarity_swapped,
        m.metric_name,
        -- Convert Wh to kWh
        MIN(CASE 
            WHEN m.metric_value > 0 AND m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
            WHEN m.metric_value > 0 THEN m.metric_value
            ELSE NULL
        END) as min_value_kwh,
        MAX(CASE 
            WHEN m.metric_value > 0 AND m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
            WHEN m.metric_value > 0 THEN m.metric_value
            ELSE NULL
        END) as max_value_kwh,
        (ARRAY_AGG(
            CASE 
                WHEN m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
                ELSE m.metric_value
            END
            ORDER BY m.timestamp
        ))[1] as first_value_kwh,
        (ARRAY_AGG(
            CASE 
                WHEN m.metric_unit = 'Wh' THEN m.metric_value / 1000.0
                ELSE m.metric_value
            END
            ORDER BY m.timestamp DESC
        ))[1] as last_value_kwh
    FROM "MMSR"."mart"."mart_meter_performance_5min" m
    JOIN "MMSR"."staging"."seed_meter_config" mc 
        ON m.asset_id = CONCAT(
            CASE 
                WHEN m.system = 'fusionsolar' THEN 'FS'
                WHEN m.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(m.system, 3))
            END, '_', mc.esn_code
        )
    CROSS JOIN matched_days md
    WHERE mc.meter_type = 'Revenue'
        AND m.metric_name IN ('positive_active_energy', 'negative_active_energy')
        AND m.metric_value > 0
        AND m.site_name = md.site_name
        AND m.date_key = md.date_key
    GROUP BY m.date_key, m.site_name, m.asset_id, mc.dev_name, mc.esn_code, 
        CASE 
            WHEN mc.esn_code = 'AM001023C7355634' THEN 'TRUE'
            ELSE ''
        END, m.metric_name
),

-- Step 3: Calculate daily energy per meter with previous day
meter_with_prev AS (
    SELECT 
        d1.*,
        d2.max_value_kwh as prev_day_max_kwh,
        CASE 
            WHEN d2.max_value_kwh IS NOT NULL 
                AND (
                    ABS(d1.first_value_kwh - d2.max_value_kwh) < 0.01
                    OR (d2.max_value_kwh > 0 AND ABS(d1.first_value_kwh - d2.max_value_kwh) / d2.max_value_kwh < 0.001)
                )
            THEN true
            ELSE false
        END as is_cumulative,
        CASE 
            WHEN d2.max_value_kwh IS NOT NULL 
                AND (
                    ABS(d1.first_value_kwh - d2.max_value_kwh) < 0.01
                    OR (d2.max_value_kwh > 0 AND ABS(d1.first_value_kwh - d2.max_value_kwh) / d2.max_value_kwh < 0.001)
                )
            THEN d1.max_value_kwh - d2.max_value_kwh
            ELSE d1.max_value_kwh - d1.min_value_kwh
        END as daily_energy_kwh
    FROM meter_details d1
    LEFT JOIN meter_details d2
        ON d1.asset_id = d2.asset_id
        AND d1.metric_name = d2.metric_name
        AND d1.site_name = d2.site_name
        AND d2.date_key = d1.date_key - INTERVAL '1 day'
),

-- Step 4: Aggregate per meter
meter_energy_summary AS (
    SELECT 
        date_key,
        site_name,
        asset_id,
        dev_name,
        esn_code,
        polarity_swapped,
        SUM(CASE WHEN metric_name = 'positive_active_energy' THEN daily_energy_kwh ELSE 0 END) as positive_energy_kwh,
        SUM(CASE WHEN metric_name = 'negative_active_energy' THEN daily_energy_kwh ELSE 0 END) as negative_energy_kwh,
        -- Calculate energy with polarity correction
        CASE 
            WHEN UPPER(TRIM(COALESCE(polarity_swapped, ''))) = 'TRUE' THEN 
                ABS(SUM(CASE WHEN metric_name = 'negative_active_energy' THEN daily_energy_kwh ELSE 0 END) - 
                    SUM(CASE WHEN metric_name = 'positive_active_energy' THEN daily_energy_kwh ELSE 0 END))
            ELSE 
                ABS(SUM(CASE WHEN metric_name = 'positive_active_energy' THEN daily_energy_kwh ELSE 0 END) - 
                    SUM(CASE WHEN metric_name = 'negative_active_energy' THEN daily_energy_kwh ELSE 0 END))
        END as meter_energy_kwh
    FROM meter_with_prev
    GROUP BY date_key, site_name, asset_id, dev_name, esn_code, polarity_swapped
)

-- Final output
SELECT 
    md.date_key,
    md.site_name,
    
    -- Summary from matched_days
    ROUND(md.db_energy::numeric, 4) as db_energy_mwh,
    ROUND(md.excel_energy::numeric, 4) as excel_energy_mwh,
    ROUND(md.energy_diff::numeric, 4) as energy_diff_mwh,
    ROUND(md.db_ghi::numeric, 4) as db_ghi_kwh_m2,
    ROUND(md.excel_ghi::numeric, 4) as excel_ghi_kwh_m2,
    ROUND(md.db_poa::numeric, 4) as db_poa_kwh_m2,
    ROUND(md.excel_poa::numeric, 4) as excel_poa_kwh_m2,
    ROUND(md.db_pr_ghi::numeric, 4) as db_pr_ghi,
    ROUND(md.excel_pr_ghi::numeric, 4) as excel_pr_ghi,
    ROUND(md.db_pr_poa::numeric, 4) as db_pr_poa,
    ROUND(md.excel_pr_poa::numeric, 4) as excel_pr_poa,
    
    -- Meter details
    mes.dev_name,
    mes.esn_code,
    mes.polarity_swapped,
    ROUND(mes.positive_energy_kwh::numeric, 4) as positive_energy_kwh,
    ROUND(mes.negative_energy_kwh::numeric, 4) as negative_energy_kwh,
    ROUND(mes.meter_energy_kwh::numeric, 4) as meter_energy_kwh,
    ROUND((mes.meter_energy_kwh / 1000.0)::numeric, 4) as meter_energy_mwh,
    
    -- Total from all meters
    ROUND((SUM(mes.meter_energy_kwh) OVER (PARTITION BY md.date_key, md.site_name) / 1000.0)::numeric, 4) as total_site_energy_mwh

FROM matched_days md
LEFT JOIN meter_energy_summary mes
    ON md.date_key = mes.date_key
    AND md.site_name = mes.site_name
ORDER BY mes.dev_name;

