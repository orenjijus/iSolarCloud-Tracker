# Entity Relationship Diagram (ERD)

## Database Schema Design

This document provides a detailed Entity Relationship Diagram for the Solar Monitoring System Database.

## Core Entities

```mermaid
erDiagram
    ISOLARCLOUD_POWER_STATIONS ||--o{ ISOLARCLOUD_DEVICES : "has"
    ISOLARCLOUD_DEVICES ||--o{ ISOLARCLOUD_HISTORICAL_DATA : "generates"
    
    FUSIONSOLAR_PLANTS ||--o{ FUSIONSOLAR_DEVICES : "has"
    FUSIONSOLAR_DEVICES ||--o{ FUSIONSOLAR_HISTORICAL_DATA : "generates"
    
    ISOLARCLOUD_POWER_STATIONS {
        varchar ps_id PK
        varchar ps_name
        timestamp install_date
        float latitude
        float longitude
        int online_status
        text description
        int valid_flag
        int grid_connection_status
        int ps_fault_status
        varchar ps_location
        timestamp update_time_api
        varchar ps_current_time_zone
        timestamp grid_connection_time
        int connect_type
        int build_status
        int ps_type
    }
    
    ISOLARCLOUD_DEVICES {
        varchar device_ps_key PK
        varchar ps_id FK
        int device_type
        varchar type_name
        varchar device_sn
        int dev_status
        varchar factory_name
        varchar uuid
        timestamp grid_connection_date
        varchar device_name
        int dev_fault_status
        int rel_state
        varchar device_code
        varchar device_model_id
        varchar communication_dev_sn
        varchar device_model_code
        varchar chnnl_id
    }
    
    ISOLARCLOUD_HISTORICAL_DATA {
        varchar device_ps_key PK,FK
        timestamp timestamp PK
        jsonb measurement_data
    }
    
    FUSIONSOLAR_PLANTS {
        varchar plant_code PK
        varchar plant_name
        timestamp commissioning_date
        float capacity
        varchar address
        varchar plant_status
    }
    
    FUSIONSOLAR_DEVICES {
        varchar device_plant_key PK
        varchar plant_code FK
        int device_type
        varchar type_name
        varchar esn
        varchar model
        varchar status
        varchar device_name
    }
    
    FUSIONSOLAR_HISTORICAL_DATA {
        varchar device_plant_key PK,FK
        timestamp timestamp PK
        jsonb measurement_data
    }
```

## Derived Views

The system creates multiple SQL views for easier data analysis:

```mermaid
erDiagram
    ISOLARCLOUD_HISTORICAL_DATA ||--o{ SITE_DEVICE_VIEWS : "extracted to"
    SITE_DEVICE_VIEWS ||--o{ FULL_DAY_VIEWS : "expanded to"
    FULL_DAY_VIEWS ||--o{ PIVOTED_VIEWS : "transformed to"
    
    SITE_DEVICE_VIEWS {
        varchar device_ps_key
        timestamp timestamp
        float measurement_point_1
        float measurement_point_2
        float measurement_point_n
    }
    
    FULL_DAY_VIEWS {
        timestamp timestamp
        float measurement_point_1
        float measurement_point_2
        float measurement_point_n
    }
    
    PIVOTED_VIEWS {
        timestamp timestamp
        float yield_today_kWh
        float total_dc_power_W
        float string1_voltage_V
        float string1_current_A
    }
```

## Device Type Classification

Devices are classified into different types:

1. **Inverters (Type 1)**:
   - String-level measurements
   - Summary measurements (Yield, DC Power, Active Power, Reactive Power)

2. **Meters (Type 5)**:
   - Energy measurements
   - Power quality metrics

3. **Meteo Stations (Type 14)**:
   - Irradiance
   - Temperature
   - Other environmental metrics

## Measurement Data Structure

The `measurement_data` JSON column in historical data tables follows this structure:

```json
{
  "p1": 123.45,  // Yield Today (kWh)
  "p14": 5000,   // Total DC Power (W)
  "p24": 4800,   // Total Active Power (W)
  "p25": 300,    // Total Reactive Power (var)
  "p101": 600,   // String 1 Voltage (V)
  "p201": 8.5,   // String 1 Current (A)
  // Additional measurement points...
}
```

Different device types have different sets of measurement points, as defined in the `ISOLARCLOUD_SITE_MEASURING_POINTS` and similar configuration dictionaries.
