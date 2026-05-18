# Solar Monitoring System Database Documentation

## Database Schema

The Solar Monitoring System uses PostgreSQL for data storage. The database consists of the following core tables:

### iSolarCloud Tables

1. **isolarcloud_power_stations**
   - Primary key: `ps_id` (VARCHAR)
   - Contains metadata about solar power stations
   - Stores location, installation date, status information

2. **isolarcloud_devices**
   - Primary key: `device_ps_key` (VARCHAR)
   - Foreign key: `ps_id` references `isolarcloud_power_stations(ps_id)`
   - Contains information about devices (inverters, meters, meteo stations)
   - Tracks device type, serial number, model, and status

3. **isolarcloud_historical_data**
   - Composite primary key: (`device_ps_key`, `timestamp`)
   - Foreign key: `device_ps_key` references `isolarcloud_devices(device_ps_key)`
   - Stores time-series measurement data as JSON in the `measurement_data` column
   - Data is collected at 5-minute intervals

### FusionSolar Tables

1. **fusionsolar_plants**
   - Primary key: `plant_code` (VARCHAR)
   - Contains metadata about solar plants

2. **fusionsolar_devices**
   - Primary key: `device_plant_key` (VARCHAR)
   - Foreign key: `plant_code` references `fusionsolar_plants(plant_code)`
   - Contains information about devices

3. **fusionsolar_historical_data**
   - Composite primary key: (`device_plant_key`, `timestamp`)
   - Foreign key: `device_plant_key` references `fusionsolar_devices(device_plant_key)`
   - Stores time-series measurement data similar to iSolarCloud

## Database Views

The system creates various SQL views to make data analysis easier:

1. **Site-Device Views**
   - Named pattern: `{site_name}_{device_type}_data`
   - Example: `Garuda_Metalindo_IKP_meter_data`
   - Extracts specific measurement points from the JSON data column

2. **Full Day Views**
   - Named pattern: `{site}_{device_type}_full_day_data`
   - Contains complete time series for each day (00:00-23:55) with 5-minute intervals
   - Fills in device measurement data where available

3. **Pivoted Views**
   - Named pattern: `{site}_{device_type}_pivoted_full_day`
   - Measurements are presented as columns for easier analysis

## Connection Parameters

The database connection parameters are stored in the `.env` file:

- Host: `localhost`
- Port: `5433`
- Database: `MMSR`
- User: `postgres`
- Password: *(stored in .env file)*

## Entity Relationship Diagram

```
+-------------------------+        +------------------------+        +---------------------------+
| isolarcloud_power_      |        | isolarcloud_devices    |        | isolarcloud_historical_   |
| stations                |        |                        |        | data                      |
+-------------------------+        +------------------------+        +---------------------------+
| PK: ps_id               |<-------| FK: ps_id              |        | PK/FK: device_ps_key     |
| ps_name                 |        | PK: device_ps_key      |<-------| PK: timestamp            |
| install_date            |        | device_type            |        | measurement_data (JSON)   |
| latitude                |        | type_name              |        |                           |
| longitude               |        | device_sn              |        |                           |
| ...                     |        | ...                    |        |                           |
+-------------------------+        +------------------------+        +---------------------------+

+-------------------------+        +------------------------+        +---------------------------+
| fusionsolar_plants      |        | fusionsolar_devices    |        | fusionsolar_historical_   |
|                         |        |                        |        | data                      |
+-------------------------+        +------------------------+        +---------------------------+
| PK: plant_code          |<-------| FK: plant_code         |        | PK/FK: device_plant_key  |
| plant_name              |        | PK: device_plant_key   |<-------| PK: timestamp            |
| ...                     |        | device_type            |        | measurement_data (JSON)   |
|                         |        | ...                    |        |                           |
+-------------------------+        +------------------------+        +---------------------------+
```

## Database Management

Since the PostgreSQL MCP resource is available in read-only mode, all table creation and modifications should be done through the application code using SQLAlchemy.

Tables are created with `CREATE TABLE IF NOT EXISTS` statements in the database initialization functions found in:
- `isolarcloud/isolarcloud_harvester_src/isolar_db_operations.py`
- `fusionsolar/fusionsolar_harvester_src/fusionsolar_db_operations.py`
