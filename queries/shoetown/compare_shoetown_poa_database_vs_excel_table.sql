-- ============================================
-- Compare Shoetown POA: Database vs Excel Table
-- ============================================
-- This query compares data from mart_sensor_daily with public.shoetown_poa_daily
-- 
-- Key Finding: Excel masih menggunakan SLI-IRR-3-F setelah Oct 3, padahal sudah diganti Meteo Station16
-- Database exclude SLI-IRR-3-F setelah Oct 3, Excel masih include sampai Nov 12
-- After Nov 12 (Meteo Station16 aktif), data match perfectly
-- ============================================

WITH db_data AS (
    SELECT 
        date_key,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        daily_irradiance_kwh_m2,
        daily_irradiance_kwh_m2 * sensor_capacity_kwp as weighted_contribution
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'POA'
        AND site_name = 'Shoetown Ligung Indonesia'
        AND daily_irradiance_kwh_m2 IS NOT NULL
        AND sensor_capacity_kwp IS NOT NULL
),
filtered_sensors AS (
    SELECT 
        date_key,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        daily_irradiance_kwh_m2,
        weighted_contribution,
        CASE 
            WHEN device_id = '1479456_5_16_2' AND date_key > '2025-10-01'::date THEN FALSE
            WHEN device_id = '1479456_5_15_2' AND date_key > '2025-10-01'::date THEN FALSE
            WHEN device_id = '1479456_5_24_1' AND date_key < '2025-10-03'::date THEN FALSE
            WHEN device_id = '1479456_5_25_1' AND date_key < '2025-10-03'::date THEN FALSE
            WHEN device_id = '1479456_5_17_1' AND date_key >= '2025-10-03'::date THEN FALSE
            WHEN device_id = '1479456_5_27_1' AND date_key < '2025-11-12'::date THEN FALSE
            ELSE TRUE
        END as is_active
    FROM db_data
),
db_summary AS (
    SELECT 
        date_key,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-A' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_1_A_DB,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-A' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_2_A_DB,
        ROUND(MAX(CASE 
            WHEN sensor_dev_name = 'SLI-IRR-3-F' AND is_active THEN daily_irradiance_kwh_m2 
            WHEN sensor_dev_name = 'Meteo Station16' AND is_active THEN daily_irradiance_kwh_m2 
            ELSE NULL 
        END)::numeric, 9) as SLI_IRR_3_F_DB,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-4-F' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_4_F_DB,
        ROUND(
            (SUM(CASE WHEN is_active THEN weighted_contribution ELSE 0 END) / 
             NULLIF(SUM(CASE WHEN is_active THEN sensor_capacity_kwp ELSE 0 END), 0))::numeric, 
            9
        ) as Weighted_Avg_POA_DB,
        ROUND(SUM(CASE WHEN is_active THEN sensor_capacity_kwp ELSE 0 END)::numeric, 2) as Sum_Capacity_DB,
        STRING_AGG(DISTINCT CASE WHEN is_active THEN sensor_dev_name END, ', ') as Active_Sensors_DB
    FROM filtered_sensors
    GROUP BY date_key
),
excel_data AS (
    SELECT 
        date_key::date as date_key,
        "SLI_IRR_1_A_Slope Daily Irradiation_W_per_m2"::numeric as SLI_IRR_1_A_Excel,
        "SLI_IRR_2_A_Slope Daily Irradiation_W_per_m2"::numeric as SLI_IRR_2_A_Excel,
        "SLI_IRR_3_F_Slope Daily Irradiation_W_per_m2"::numeric as SLI_IRR_3_F_Excel,
        CASE 
            WHEN "SLI_IRR_4_F_Slope Daily Irradiation_W_per_m2" ~ '^\d+\.?\d*$' 
            THEN "SLI_IRR_4_F_Slope Daily Irradiation_W_per_m2"::numeric
            ELSE NULL
        END as SLI_IRR_4_F_Excel,
        poa_actual_kwh_m2 as Weighted_Avg_POA_Excel
    FROM public.shoetown_poa_daily
    WHERE date_key IS NOT NULL 
        AND date_key != ''
        AND date_key ~ '^\d{4}-\d{2}-\d{2}$'
)
SELECT 
    COALESCE(db.date_key, excel.date_key) as "Date",
    -- Database values
    db.SLI_IRR_1_A_DB,
    db.SLI_IRR_2_A_DB,
    db.SLI_IRR_3_F_DB,
    db.SLI_IRR_4_F_DB,
    db.Weighted_Avg_POA_DB,
    db.Sum_Capacity_DB,
    db.Active_Sensors_DB,
    -- Excel values
    excel.SLI_IRR_1_A_Excel,
    excel.SLI_IRR_2_A_Excel,
    excel.SLI_IRR_3_F_Excel,
    excel.SLI_IRR_4_F_Excel,
    excel.Weighted_Avg_POA_Excel,
    -- Differences
    ROUND((db.SLI_IRR_1_A_DB - excel.SLI_IRR_1_A_Excel)::numeric, 9) as Diff_SLI_IRR_1_A,
    ROUND((db.SLI_IRR_2_A_DB - excel.SLI_IRR_2_A_Excel)::numeric, 9) as Diff_SLI_IRR_2_A,
    ROUND((db.SLI_IRR_3_F_DB - excel.SLI_IRR_3_F_Excel)::numeric, 9) as Diff_SLI_IRR_3_F,
    ROUND((db.SLI_IRR_4_F_DB - excel.SLI_IRR_4_F_Excel)::numeric, 9) as Diff_SLI_IRR_4_F,
    ROUND((db.Weighted_Avg_POA_DB - excel.Weighted_Avg_POA_Excel)::numeric, 9) as Diff_Weighted_Avg,
    -- Ratio (DB / Excel)
    ROUND((db.Weighted_Avg_POA_DB / NULLIF(excel.Weighted_Avg_POA_Excel, 0))::numeric, 4) as Ratio_DB_vs_Excel,
    -- Status
    CASE 
        WHEN db.date_key IS NULL THEN 'MISSING_IN_DB'
        WHEN excel.date_key IS NULL THEN 'MISSING_IN_EXCEL'
        WHEN ABS(db.Weighted_Avg_POA_DB - excel.Weighted_Avg_POA_Excel) <= 0.01 THEN 'MATCH'
        WHEN ABS(db.Weighted_Avg_POA_DB - excel.Weighted_Avg_POA_Excel) <= 0.1 THEN 'CLOSE'
        ELSE 'DIFFERENT'
    END as match_status
FROM db_summary db
FULL OUTER JOIN excel_data excel
    ON db.date_key = excel.date_key
WHERE COALESCE(db.date_key, excel.date_key) >= '2025-10-01'::date
ORDER BY COALESCE(db.date_key, excel.date_key);

