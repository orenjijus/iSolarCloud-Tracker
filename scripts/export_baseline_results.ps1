# Export Baseline Cross-Check Results to CSV
# PowerShell script untuk export hasil baseline comparison ke CSV

$env:PGPASSWORD = "postgres"  # Set password (atau gunakan .pgpass file)

$query = @"
COPY (
WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.site_id,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2,
        d.availability_percent,
        d.pr_ghi_actual,
        d.pr_poa_actual,
        d.system
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 5.7 MWp - Phase 2',
        'PT. MMKI 4.292 MWP - Phase 3'
    )
        AND d.date_key >= COALESCE((
            SELECT MIN(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), '2024-01-01'::date)
        AND d.date_key <= COALESCE((
            SELECT MAX(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), CURRENT_DATE)
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        site_id,
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2,
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(availability_percent, '%', ''), ',', '.') AS NUMERIC)
        END as availability_percent,
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(pr_ghi_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
        END as pr_ghi_actual,
        CASE 
            WHEN pr_poa_actual IS NULL OR TRIM(pr_poa_actual) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(pr_poa_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
        END as pr_poa_actual,
        'fusionsolar' as system
    FROM public.site_daily_performance_excel_mmki
    WHERE site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 5.7 MWp - Phase 2',
        'PT. MMKI 4.292 MWP - Phase 3'
    )
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        COALESCE(db.site_id, excel.site_id) as site_id,
        db.daily_energy_mwh as db_energy_mwh,
        db.daily_ghi_kwh_m2 as db_ghi,
        db.daily_poa_weighted_kwh_m2 as db_poa,
        db.availability_percent as db_availability,
        db.pr_ghi_actual as db_pr_ghi,
        db.pr_poa_actual as db_pr_poa,
        excel.daily_energy_mwh as excel_energy_mwh,
        excel.daily_ghi_kwh_m2 as excel_ghi,
        excel.daily_poa_weighted_kwh_m2 as excel_poa,
        excel.availability_percent as excel_availability,
        excel.pr_ghi_actual as excel_pr_ghi,
        excel.pr_poa_actual as excel_pr_poa,
        COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0) as energy_diff_mwh,
        COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0) as ghi_diff,
        COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0) as poa_diff,
        COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0) as availability_diff,
        CASE 
            WHEN excel.daily_energy_mwh > 0 
            THEN ((COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) / excel.daily_energy_mwh) * 100
            ELSE NULL
        END as energy_diff_pct,
        CASE 
            WHEN excel.daily_ghi_kwh_m2 > 0 
            THEN ((COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) / excel.daily_ghi_kwh_m2) * 100
            ELSE NULL
        END as ghi_diff_pct,
        CASE 
            WHEN excel.daily_poa_weighted_kwh_m2 > 0 
            THEN ((COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) / excel.daily_poa_weighted_kwh_m2) * 100
            ELSE NULL
        END as poa_diff_pct,
        CASE 
            WHEN excel.availability_percent > 0 
            THEN ((COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) / excel.availability_percent) * 100
            ELSE NULL
        END as availability_diff_pct,
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_energy_mwh IS NULL AND excel.daily_energy_mwh IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_energy_mwh IS NOT NULL AND excel.daily_energy_mwh IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as energy_status,
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_ghi_kwh_m2 IS NULL AND excel.daily_ghi_kwh_m2 IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_ghi_kwh_m2 IS NOT NULL AND excel.daily_ghi_kwh_m2 IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as ghi_status,
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_poa_weighted_kwh_m2 IS NULL AND excel.daily_poa_weighted_kwh_m2 IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_poa_weighted_kwh_m2 IS NOT NULL AND excel.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as poa_status,
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 'MISMATCH'
            WHEN db.availability_percent IS NULL AND excel.availability_percent IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.availability_percent IS NOT NULL AND excel.availability_percent IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as availability_status,
        CASE 
            WHEN db.date_key IS NULL THEN 'ONLY_IN_EXCEL'
            WHEN excel.date_key IS NULL THEN 'ONLY_IN_DB'
            ELSE 'BOTH'
        END as data_source,
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 'POA_RELATED'
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 'GHI_RELATED'
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 'AVAILABILITY_RELATED'
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 'ENERGY_RELATED'
            WHEN ABS(COALESCE(db.pr_poa_actual, 0) - COALESCE(excel.pr_poa_actual, 0)) > 0.001 THEN 'PR_RELATED'
            ELSE 'NO_ISSUE'
        END as discrepancy_category
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    date_key,
    site_name,
    site_id,
    data_source,
    discrepancy_category,
    ROUND(db_energy_mwh::numeric, 4) as db_energy_mwh,
    ROUND(excel_energy_mwh::numeric, 4) as excel_energy_mwh,
    ROUND(energy_diff_mwh::numeric, 4) as energy_diff_mwh,
    ROUND(energy_diff_pct::numeric, 2) as energy_diff_pct,
    energy_status,
    ROUND(db_ghi::numeric, 4) as db_ghi,
    ROUND(excel_ghi::numeric, 4) as excel_ghi,
    ROUND(ghi_diff::numeric, 4) as ghi_diff,
    ROUND(ghi_diff_pct::numeric, 2) as ghi_diff_pct,
    ghi_status,
    ROUND(db_poa::numeric, 4) as db_poa,
    ROUND(excel_poa::numeric, 4) as excel_poa,
    ROUND(poa_diff::numeric, 4) as poa_diff,
    ROUND(poa_diff_pct::numeric, 2) as poa_diff_pct,
    poa_status,
    ROUND(db_availability::numeric, 2) as db_availability,
    ROUND(excel_availability::numeric, 2) as excel_availability,
    ROUND(availability_diff::numeric, 2) as availability_diff,
    ROUND(availability_diff_pct::numeric, 2) as availability_diff_pct,
    availability_status,
    ROUND(db_pr_ghi::numeric, 4) as db_pr_ghi,
    ROUND(excel_pr_ghi::numeric, 4) as excel_pr_ghi,
    ROUND(db_pr_poa::numeric, 4) as db_pr_poa,
    ROUND(excel_pr_poa::numeric, 4) as excel_pr_poa,
    CASE 
        WHEN energy_status != 'MATCH' OR ghi_status != 'MATCH' 
            OR poa_status != 'MATCH' OR availability_status != 'MATCH' 
        THEN 'DISCREPANCY'
        WHEN data_source != 'BOTH'
        THEN 'MISSING_DATA'
        ELSE 'MATCH'
    END as overall_status
FROM comparison
ORDER BY site_name, date_key DESC
) TO STDOUT WITH CSV HEADER
"@

$outputFile = "baseline_crosscheck_results_mmki.csv"

Write-Host "Exporting baseline cross-check results to $outputFile..."

psql -h localhost -U postgres -d mmsr -c $query | Out-File -FilePath $outputFile -Encoding utf8

Write-Host "Export completed: $outputFile"
Write-Host "Total lines: $((Get-Content $outputFile | Measure-Object -Line).Lines)"

