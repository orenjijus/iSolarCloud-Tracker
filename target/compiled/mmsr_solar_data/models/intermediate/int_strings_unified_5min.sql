

-- Unified string-level data from inverters
SELECT 
    timestamp_5min,
    device_ps_key,
    device_name,
    site_name,
    system,
    CASE 
        WHEN metric_id IN ('p96', 'p97', 'p98', 'p99', 'p100', 'p101', 'p102', 'p103', 'p104', 'p105', 'p106', 'p107', 'p108', 'p109', 'p110', 'p111', 'p112', 'p113') 
        THEN 'Voltage'
        WHEN metric_id IN ('p70', 'p71', 'p72', 'p73', 'p74', 'p75', 'p76', 'p77', 'p78', 'p79', 'p80', 'p81', 'p82', 'p83', 'p84', 'p85', 'p92', 'p93') 
        THEN 'Current'
        ELSE 'Other'
    END as measurement_type,
    metric_id,
    metric_value
FROM "MMSR"."public"."int_inverters_unified_5min"
WHERE metric_id LIKE 'p%'  -- String-level measurements