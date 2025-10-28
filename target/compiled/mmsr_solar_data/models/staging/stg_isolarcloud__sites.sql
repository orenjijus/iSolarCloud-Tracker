

SELECT 
    ps_id,
    ps_name,
    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(ps_name, ' ', '_'), '(', ''), ')', ''), ' ', '_')) as ps_name_clean,
    install_date,
    latitude,
    longitude
FROM "MMSR"."public"."isolarcloud_power_stations"