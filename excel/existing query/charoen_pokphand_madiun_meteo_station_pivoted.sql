-- public.charoen_pokphand_madiun_meteo_station_pivoted source

CREATE OR REPLACE VIEW public.charoen_pokphand_madiun_meteo_station_pivoted
AS SELECT h."timestamp",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_10_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2003'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2003'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GHI_01_PYRN__Transient Horizontal Irradiation _W_per_",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_10_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2001'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2001'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GHI_01_PYRN__Daily Horizontal Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_10_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2005'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2005'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GHI_01_PYRN__Slope Daily Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_10_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2007'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2007'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GHI_01_PYRN__Slope Transient Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_10_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2009'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2009'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GHI_01_PYRN__Ambient Temperature_C",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_10_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2010'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2010'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GHI_01_PYRN__PV Temperature_C",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_10_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2022'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2022'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GHI_01_PYRN__Rainfall_mm",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_12_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2003'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2003'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_01_EAST__Transient Horizontal Irradiation _W_per_",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_12_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2001'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2001'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_01_EAST__Daily Horizontal Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_12_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2005'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2005'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_01_EAST__Slope Daily Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_12_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2007'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2007'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_01_EAST__Slope Transient Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_12_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2009'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2009'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_01_EAST__Ambient Temperature_C",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_12_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2010'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2010'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_01_EAST__PV Temperature_C",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_12_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2022'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2022'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_01_EAST__Rainfall_mm",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_11_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2003'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2003'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_02_WEST__Transient Horizontal Irradiation _W_per_",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_11_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2001'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2001'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_02_WEST__Daily Horizontal Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_11_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2005'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2005'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_02_WEST__Slope Daily Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_11_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2007'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2007'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_02_WEST__Slope Transient Irradiation_W_per_m2",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_11_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2009'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2009'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_02_WEST__Ambient Temperature_C",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_11_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2010'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2010'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_02_WEST__PV Temperature_C",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_5_11_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p2022'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p2022'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_IRR_GTI_02_WEST__Rainfall_mm"
   FROM isolarcloud_historical_data h
     JOIN isolarcloud_devices d ON h.device_ps_key::text = d.device_ps_key::text
     JOIN isolarcloud_power_stations ps ON d.ps_id::text = ps.ps_id::text
  WHERE ps.ps_name::text = 'Charoen Pokphand Madiun'::text AND d.device_type = 5
  GROUP BY h."timestamp"
  ORDER BY h."timestamp";

-- Permissions

ALTER TABLE public.charoen_pokphand_madiun_meteo_station_pivoted OWNER TO postgres;
GRANT ALL ON TABLE public.charoen_pokphand_madiun_meteo_station_pivoted TO postgres;