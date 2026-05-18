graph TD
    direction TB

    %% === 1. Raw Data (Python Load) ===
    subgraph step1 ["Step 1: Raw Tables (Loaded by Python)"]
        direction TB
        R_PERF_ISO((raw_isolarcloud_performance))
        R_PERF_FS((raw_fusionsolar_performance))
        R_META_ISO((raw_isolarcloud_metadata))
        R_META_FS((raw_fusionsolar_metadata))
    end

    %% === 2. Seeds (Manual CSVs - Engineering Data) ===
    subgraph step2 ["Step 2: Seeds (Manual CSVs)"]
        direction TB
        SEED_METRIC[seed_metric_mapper]
        SEED_STR[(seed_string_config)]
        SEED_SEN[(seed_sensor_config)]
        SEED_MET[(seed_meter_config)]
        SEED_SIM[(seed_simulation_targets)]
    end
    
    %% === 3. Staging (dbt Models - Parse & Unpivot) ===
    subgraph step3 ["Step 3: Staging (Views - Unpivot)"]
        direction TB
        S_PERF_ISO_UNPIVOT[stg_isolarcloud__perf_unpivoted]
        S_PERF_FS_UNPIVOT[stg_fusionsolar__perf_unpivoted]
        
        S_META_ISO_SITE[stg_isolarcloud__sites]
        S_META_ISO_DEV[stg_isolarcloud__devices]
        S_META_FS_SITE[stg_fusionsolar__sites]
        S_META_FS_DEV[stg_fusionsolar__devices]
    end

    %% === 4. Dimensions (dbt Models - Built from Seeds/Code/Staging) ===
    subgraph step4 ["Step 4: Dimensions (Tables)"]
        direction LR
        D_ASSET[dim_assets]
        D_DATE[dim_date_generated]
    end

    %% === 5. Intermediate (dbt Models - Map & Re-pivot) ===
    subgraph step5 ["Step 5: Intermediate (Views - Map & Re-pivot)"]
       direction TB
       I1[int_meters_unified]
       I2[int_sensors_unified]
       I3[int_inverters_unified_5min]
       I4[int_strings_unified_5min]
    end

    %% === 6. Marts (dbt Models - Final Fact Tables) ===
    subgraph step6 ["Step 6: Marts (Tables)"]
       direction TB
       M1[mart_site_performance_daily]
       M6[mart_inverter_performance_daily]
       M7[mart_string_performance_daily]
       
       M2[mart_inverter_performance_5min]
       M3[mart_string_performance_5min]
       M4[mart_meter_performance_5min]
       M5[mart_sensor_measurements_5min]
       
       M_SIM[mart_simulation_targets_daily]
    end
    
    %% === 7. BI Layer ===
    subgraph step7 ["Step 7: Business Intelligence"]
       BI[PowerBI_Model]
    end

    %% === Define Styles ===
    style step1 fill:#E0E0E0,stroke:#212121,stroke-width:2px,font-weight:bold
    style step2 fill:#CFD8DC,stroke:#424242,stroke-width:2px,font-weight:bold
    style step3 fill:#B0BEC5,stroke:#546E7A,stroke-width:2px,font-weight:bold
    style step4 fill:#90A4AE,stroke:#607D8B,stroke-width:2px,font-weight:bold
    style step5 fill:#78909C,stroke:#78909C,stroke-width:2px,font-weight:bold
    style step6 fill:#607D8B,stroke:#90A4AE,stroke-width:2px,font-weight:bold
    style step7 fill:#455A64,stroke:#B0BEC5,stroke-width:2px,font-weight:bold

    %% === Define Connections ===
    
    R_PERF_ISO --> S_PERF_ISO_UNPIVOT
    R_PERF_FS --> S_PERF_FS_UNPIVOT
    
    R_META_ISO --> S_META_ISO_SITE & S_META_ISO_DEV
    R_META_FS --> S_META_FS_SITE & S_META_FS_DEV
    
    S_META_ISO_SITE & S_META_FS_SITE --> D_ASSET
    S_META_ISO_DEV & S_META_FS_DEV --> D_ASSET
    SEED_STR & SEED_SEN & SEED_MET -- "Enrichment" --> D_ASSET
    
    SEED_SIM --> M_SIM

    S_PERF_ISO_UNPIVOT & S_PERF_FS_UNPIVOT & SEED_METRIC --> I1
    S_PERF_ISO_UNPIVOT & S_PERF_FS_UNPIVOT & SEED_METRIC --> I2
    S_PERF_ISO_UNPIVOT & S_PERF_FS_UNPIVOT & SEED_METRIC --> I3
    S_PERF_ISO_UNPIVOT & S_PERF_FS_UNPIVOT & SEED_METRIC --> I4
    
    I1 & I2 --> M1
    I3 & I2 --> M2
    I4 & I2 --> M3
    I1 --> M4
    I2 --> M5
    I3 & I2 --> M6
    I4 & I2 --> M7

    D_ASSET -- "FK: asset_id" --> M1 & M2 & M3 & M4 & M5 & M6 & M7 & M_SIM
    
    D_DATE -- "FK: date_key" --> M1 & M_SIM & M6 & M7
    D_DATE -- "FK: date(timestamp)" --> M2 & M3 & M4 & M5
    
    M1 & M2 & M3 & M4 & M5 & M6 & M7 & M_SIM & D_ASSET & D_DATE --> BI