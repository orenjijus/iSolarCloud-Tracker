graph TD
    %% Main Components
    subgraph MMSRDashboard[MMSRDashboard]
        subgraph FusionSolar[FusionSolar Integration]
            FS_Main[fusionsolar_data_harvester.py]
            FS_NoLimit[fusionsolar_data_harvester_no_limit.py]
            
            subgraph FS_Source[Source Files]
                FS_API[fusionsolar_api_client.py]
                FS_Config[fusionsolar_config.py]
                FS_DataProc[fusionsolar_data_processing.py]
                FS_DB[fusionsolar_db_operations.py]
            end
            
            subgraph FS_SQL[SQL Scripts]
                FS_Views[full_day_device_views.py]
                FS_ListViews[list_views.py]
            end
        end

        subgraph iSolarCloud[iSolarCloud Integration]
            ISC_Main[isolarcloud_data_harvester.py]
            ISC_Fetch[fetch_historical_device_data.py]
            
            subgraph ISC_Source[Source Files]
                ISC_API[isolar_api_client.py]
                ISC_Config[isolar_config.py]
                ISC_DataProc[isolar_data_processing.py]
                ISC_DB[isolar_db_operations.py]
            end
            
            subgraph ISC_SQL[SQL Scripts]
                ISC_CheckDB[check_db.py]
                ISC_Views1[create_daily_string_energy_views.py]
                ISC_Views2[create_inverter_pivoted_view.py]
                ISC_Views3[create_inverter_summary_views.py]
                ISC_Views4[create_ordered_energy_views.py]
                ISC_ListViews[list_views.py]
                ISC_SiteViews[site_device_views.py]
            end
        end
    end

    %% Data Flow
    FS_Main --> FS_API
    FS_Main --> FS_DataProc
    FS_Main --> FS_DB
    FS_DataProc --> FS_API
    FS_DataProc --> FS_DB
    
    ISC_Main --> ISC_API
    ISC_Main --> ISC_DataProc
    ISC_Main --> ISC_DB
    ISC_DataProc --> ISC_API
    ISC_DataProc --> ISC_DB
    
    ISC_Fetch --> ISC_DataProc
    FS_NoLimit --> FS_DataProc

    %% External Dependencies
    classDef external fill:#f9f,stroke:#333,stroke-width:2px;
    
    subgraph External[External Services]
        FS_API --> |API Calls| FusionSolarAPI[FusionSolar API]
        ISC_API --> |API Calls| iSolarCloudAPI[iSolarCloud API]
        FS_DB --> |Reads/Writes| PostgreSQL[(PostgreSQL)]
        ISC_DB --> |Reads/Writes| PostgreSQL
    end

    %% Styling
    classDef main fill:#cff,stroke:#333,stroke-width:2px;
    classDef source fill:#cfc,stroke:#333,stroke-width:2px;
    classDef sql fill:#fcc,stroke:#333,stroke-width:2px;
    
    class FS_Main,ISC_Main main;
    class FS_Source,ISC_Source source;
    class FS_SQL,ISC_SQL sql;