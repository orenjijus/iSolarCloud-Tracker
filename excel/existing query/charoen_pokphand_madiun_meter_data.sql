-- public.charoen_pokphand_madiun_meter_data source

CREATE OR REPLACE VIEW public.charoen_pokphand_madiun_meter_data
AS SELECT h."timestamp",
    d.device_name,
        CASE
            WHEN (h.measurement_data ->> 'p8018'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8018'::text)::numeric
            ELSE NULL::numeric
        END AS "active_power_W",
        CASE
            WHEN (h.measurement_data ->> 'p8014'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8014'::text)::numeric
            ELSE NULL::numeric
        END AS power_factor,
        CASE
            WHEN (h.measurement_data ->> 'p8031'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8031'::text)::numeric
            ELSE NULL::numeric
        END AS "negative_active_energy_kWh",
        CASE
            WHEN (h.measurement_data ->> 'p8033'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8033'::text)::numeric
            ELSE NULL::numeric
        END AS negative_reactive_energy_kvarh,
        CASE
            WHEN (h.measurement_data ->> 'p8030'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8030'::text)::numeric
            ELSE NULL::numeric
        END AS "positive_active_energy_kWh",
        CASE
            WHEN (h.measurement_data ->> 'p8032'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8032'::text)::numeric
            ELSE NULL::numeric
        END AS positive_reactive_energy_kvarh
   FROM isolarcloud_historical_data h
     JOIN isolarcloud_devices d ON h.device_ps_key::text = d.device_ps_key::text
     JOIN isolarcloud_power_stations ps ON d.ps_id::text = ps.ps_id::text
  WHERE ps.ps_name::text = 'Charoen Pokphand Madiun'::text AND d.device_type = 7;

-- Permissions

ALTER TABLE public.charoen_pokphand_madiun_meter_data OWNER TO postgres;
GRANT ALL ON TABLE public.charoen_pokphand_madiun_meter_data TO postgres;