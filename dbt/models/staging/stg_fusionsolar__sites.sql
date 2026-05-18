{{ config(materialized='table') }}

SELECT 
    plant_code,
    plant_name,
    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(plant_name, ' ', '_'), '(', ''), ')', ''), ' ', '_')) as plant_name_clean,
    grid_connection_date,
    latitude,
    longitude,
    capacity
FROM {{ source('raw', 'fusionsolar_plants') }}

