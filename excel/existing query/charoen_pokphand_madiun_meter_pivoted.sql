-- public.charoen_pokphand_madiun_meter_pivoted source

CREATE OR REPLACE VIEW public.charoen_pokphand_madiun_meter_pivoted
AS SELECT h."timestamp",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_15_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8018'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8018'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_01_active_power_W",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_15_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8014'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8014'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_01_power_factor",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_15_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8031'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8031'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_01_negative_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_15_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8033'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8033'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_01_negative_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_15_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8030'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8030'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_01_positive_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_15_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8032'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8032'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_01_positive_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_16_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8018'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8018'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_02_active_power_W",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_16_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8014'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8014'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_02_power_factor",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_16_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8031'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8031'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_02_negative_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_16_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8033'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8033'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_02_negative_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_16_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8030'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8030'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_02_positive_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_16_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8032'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8032'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_EM_POI_02_positive_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_17_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8018'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8018'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_01_active_power_W",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_17_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8014'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8014'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_01_power_factor",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_17_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8031'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8031'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_01_negative_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_17_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8033'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8033'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_01_negative_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_17_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8030'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8030'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_01_positive_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_17_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8032'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8032'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_01_positive_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_18_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8018'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8018'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_02_active_power_W",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_18_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8014'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8014'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_02_power_factor",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_18_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8031'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8031'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_02_negative_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_18_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8033'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8033'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_02_negative_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_18_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8030'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8030'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_02_positive_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_18_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8032'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8032'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "CPMAD_PM_ACSB_02_positive_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_13_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8018'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8018'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter1_active_power_W",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_13_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8014'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8014'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter1_power_factor",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_13_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8031'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8031'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter1_negative_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_13_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8033'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8033'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter1_negative_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_13_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8030'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8030'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter1_positive_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_13_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8032'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8032'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter1_positive_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_14_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8018'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8018'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter2_active_power_W",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_14_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8014'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8014'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter2_power_factor",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_14_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8031'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8031'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter2_negative_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_14_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8033'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8033'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter2_negative_reactive_energy_kvarh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_14_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8030'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8030'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter2_positive_active_energy_kWh",
    max(
        CASE
            WHEN h.device_ps_key::text = '1637816_7_14_1'::text THEN
            CASE
                WHEN (h.measurement_data ->> 'p8032'::text) ~ '^-?\d+(\.\d+)?$'::text THEN (h.measurement_data ->> 'p8032'::text)::numeric
                ELSE NULL::numeric
            END
            ELSE NULL::numeric
        END) AS "Meter2_positive_reactive_energy_kvarh"
   FROM isolarcloud_historical_data h
     JOIN isolarcloud_devices d ON h.device_ps_key::text = d.device_ps_key::text
     JOIN isolarcloud_power_stations ps ON d.ps_id::text = ps.ps_id::text
  WHERE ps.ps_name::text = 'Charoen Pokphand Madiun'::text AND d.device_type = 7
  GROUP BY h."timestamp"
  ORDER BY h."timestamp";

-- Permissions

ALTER TABLE public.charoen_pokphand_madiun_meter_pivoted OWNER TO postgres;
GRANT ALL ON TABLE public.charoen_pokphand_madiun_meter_pivoted TO postgres;