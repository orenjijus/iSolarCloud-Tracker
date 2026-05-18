-- Export monthly data for sites that are missing in Excel
-- This data can be used to add missing sites to Excel
-- 
-- Usage: Run this query and export results to CSV/Excel

SELECT 
    year,
    month,
    site_id,
    site_name,
    system,
    actual_capacity_kw,
    
    -- Monthly aggregated values
    daily_energy_mwh as monthly_energy_mwh,  -- Note: column name is "daily_energy_mwh" but contains monthly sum
    daily_ghi_kwh_m2 as monthly_ghi_kwh_m2,  -- Note: column name is "daily_ghi_kwh_m2" but contains monthly sum
    pr_ghi_actual,
    
    -- Targets
    energy_target_mwh,
    ghi_target,
    
    -- Calculated percentages
    energy_actual_vs_target_pct,
    ghi_actual_vs_target_pct,
    energy_vs_ghi_variance_pct,
    
    -- Metadata
    days_with_data,
    days_with_energy,
    days_with_ghi

FROM "MMSR"."mart"."mart_site_performance_monthly"
WHERE year = 2025
    AND site_id IN (
        -- Sites that are missing in Excel for certain months
        'FS_SITE_NE=58630782',  -- PT. MMKI 4.292 MWP - Phase 3 (missing Jan-Jun)
        'ISO_SITE_1680199',     -- PLTS Rooftop Sumatera Prima Fibreboard (missing Oct)
        'ISO_SITE_1614122',     -- Charoen Pokphand Majalengka (missing Jul-Aug)
        'ISO_SITE_1628909',     -- PLTS Frina Lestari Nusantara (missing Jul-Aug)
        'ISO_SITE_1637095',     -- Charoen Pokphand Bandung (missing Jul-Aug)
        'ISO_SITE_1637816'      -- Charoen Pokphand Madiun (missing Aug)
    )
    AND (
        -- Filter for months where site is missing
        (site_id = 'FS_SITE_NE=58630782' AND month BETWEEN 1 AND 6) OR
        (site_id = 'ISO_SITE_1680199' AND month = 10) OR
        (site_id IN ('ISO_SITE_1614122', 'ISO_SITE_1628909', 'ISO_SITE_1637095') AND month IN (7, 8)) OR
        (site_id = 'ISO_SITE_1637816' AND month = 8)
    )
ORDER BY 
    site_id,
    year,
    month

