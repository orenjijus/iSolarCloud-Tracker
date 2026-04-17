import logging
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from .isolar_config import DATABASE_URL, REQUEST_DELAY_SECONDS
from .isolar_api_client import _make_api_request

# Global database engine and session
engine = None
Session = None

def init_database():
    """Initializes the PostgreSQL database connection and creates a session factory."""
    global engine, Session
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        
        # Create tables if they don't exist
        with engine.connect() as conn:
            # Create power_stations table without PRIMARY KEY first (to handle existing tables)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.isolarcloud_power_stations (
                    ps_id VARCHAR(255),
                    ps_name VARCHAR(255),
                    install_date TIMESTAMP,
                    latitude FLOAT,
                    longitude FLOAT,
                    online_status INTEGER,
                    description TEXT,
                    valid_flag INTEGER,
                    grid_connection_status INTEGER,
                    ps_fault_status INTEGER,
                    ps_location VARCHAR(255),
                    update_time_api TIMESTAMP,
                    ps_current_time_zone VARCHAR(50),
                    grid_connection_time TIMESTAMP,
                    connect_type INTEGER,
                    build_status INTEGER,
                    ps_type INTEGER
                )
            """))
            
            # Ensure PRIMARY KEY constraint exists for power_stations
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM pg_constraint 
                        WHERE conrelid = 'raw.isolarcloud_power_stations'::regclass 
                        AND contype = 'p'
                    ) THEN
                        ALTER TABLE raw.isolarcloud_power_stations 
                        ADD CONSTRAINT isolarcloud_power_stations_pkey 
                        PRIMARY KEY (ps_id);
                    END IF;
                END $$;
            """))
            
            # Create devices table without PRIMARY KEY first (to handle existing tables)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.isolarcloud_devices (
                    device_ps_key VARCHAR(255),
                    ps_id VARCHAR(255),
                    device_type INTEGER,
                    type_name VARCHAR(255),
                    device_sn VARCHAR(255),
                    dev_status INTEGER,
                    factory_name VARCHAR(255),
                    uuid VARCHAR(255),
                    grid_connection_date TIMESTAMP,
                    device_name VARCHAR(255),
                    dev_fault_status INTEGER,
                    rel_state INTEGER,
                    device_code VARCHAR(255),
                    device_model_id VARCHAR(255),
                    communication_dev_sn VARCHAR(255),
                    device_model_code VARCHAR(255),
                    chnnl_id VARCHAR(255),
                    FOREIGN KEY (ps_id) REFERENCES raw.isolarcloud_power_stations(ps_id)
                )
            """))
            
            # Ensure PRIMARY KEY constraint exists for devices
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM pg_constraint 
                        WHERE conrelid = 'raw.isolarcloud_devices'::regclass 
                        AND contype = 'p'
                    ) THEN
                        ALTER TABLE raw.isolarcloud_devices 
                        ADD CONSTRAINT isolarcloud_devices_pkey 
                        PRIMARY KEY (device_ps_key);
                    END IF;
                END $$;
            """))

            # Create table without PRIMARY KEY first (to handle existing tables)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.isolarcloud_historical_data (
                    device_ps_key VARCHAR(255),
                    timestamp TIMESTAMP,
                    measurement_data JSONB,
                    FOREIGN KEY (device_ps_key) REFERENCES raw.isolarcloud_devices(device_ps_key)
                )
            """))
            
            # Ensure PRIMARY KEY constraint exists (needed for ON CONFLICT)
            # This handles both new tables and existing tables that were migrated from public schema
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM pg_constraint 
                        WHERE conrelid = 'raw.isolarcloud_historical_data'::regclass 
                        AND contype = 'p'
                    ) THEN
                        ALTER TABLE raw.isolarcloud_historical_data 
                        ADD CONSTRAINT isolarcloud_historical_data_pkey 
                        PRIMARY KEY (device_ps_key, timestamp);
                    END IF;
                END $$;
            """))
            conn.commit()
        
        logging.info("PostgreSQL database initialized successfully.")
        return True
    except Exception as e:
        logging.error(f"Failed to initialize PostgreSQL database: {e}")
        return False

def sync_power_stations():
    """Fetches all power stations and stores/updates them in PostgreSQL."""
    if not engine or not Session:
        logging.error("Database not initialized. Cannot sync power stations.")
        return

    print("Starting power station synchronization...")
    logging.info("Starting power station synchronization...")
    all_stations = []
    current_page = 1
    page_size = 20 

    while True:
        print(f"Fetching page {current_page} of power stations...")
        payload = {
            "curPage": current_page,
            "size": page_size
        }
        data = _make_api_request("/openapi/getPowerStationList", payload)
        print(f"Raw API data structure: {type(data)}")
        print(f"Data keys: {data.keys() if data else 'No data'}")
        time.sleep(REQUEST_DELAY_SECONDS)

        if not data:
            print(f"No data received from getPowerStationList page {current_page}. Ending sync.")
            logging.warning(f"No data received from getPowerStationList page {current_page}. Ending sync.")
            break

        # Check for result_data first, as that's the structure we're seeing in the API response
        if 'result_data' in data:
            print(f"Found result_data structure, keys: {data['result_data'].keys() if isinstance(data['result_data'], dict) else 'not a dict'}")
            # If pageList is inside result_data
            if isinstance(data['result_data'], dict) and 'pageList' in data['result_data']:
                stations_on_page = data['result_data'].get('pageList', [])
                print(f"Found {len(stations_on_page)} stations in result_data.pageList")
            else:
                print("No pageList found in result_data")
                stations_on_page = []
        else:        
            # Original structure as expected
            stations_on_page = data.get("pageList", [])
            print(f"Using original structure, found {len(stations_on_page)} stations in pageList")

        if not stations_on_page:
            print("No power stations found on current page.")
            logging.info("No more power stations found on current page.")
            break
        
        # Print limited info to avoid encoding issues
        if stations_on_page:
            first_station = stations_on_page[0]
            print(f"First station ID: {first_station.get('ps_id', 'Unknown')}, Name available: {'ps_name' in first_station}")
        else:
            print("No stations found in this page")
        all_stations.extend(stations_on_page)
        print(f"Fetched page {current_page} with {len(stations_on_page)} power stations.")
        logging.info(f"Fetched page {current_page} with {len(stations_on_page)} power stations.")

        if len(stations_on_page) < page_size or data.get("rowCount", 0) == len(all_stations):
            print("All power station pages fetched.")
            logging.info("All power station pages fetched.")
            break
        current_page += 1

    if not all_stations:
        print("No power stations to sync.")
        logging.info("No power stations to sync.")
        return

    print(f"Attempting to sync {len(all_stations)} power stations to database")
    try:
        session = Session()
        print("Database session created")
        for idx, station in enumerate(all_stations):
            print(f"Processing station {idx+1}/{len(all_stations)}: {station.get('ps_id', 'Unknown ID')}")
            # Avoid printing the station name to prevent encoding issues
            # Use UPSERT (INSERT ... ON CONFLICT DO UPDATE)
            session.execute(text("""
                INSERT INTO raw.isolarcloud_power_stations (
                    ps_id, ps_name, install_date, latitude, longitude, online_status,
                    description, valid_flag, grid_connection_status, ps_fault_status,
                    ps_location, update_time_api, ps_current_time_zone,
                    grid_connection_time, connect_type, build_status, ps_type
                ) VALUES (
                    :ps_id, :ps_name, :install_date, :latitude, :longitude, :online_status,
                    :description, :valid_flag, :grid_connection_status, :ps_fault_status,
                    :ps_location, :update_time_api, :ps_current_time_zone,
                    :grid_connection_time, :connect_type, :build_status, :ps_type
                ) ON CONFLICT (ps_id) DO UPDATE SET
                    ps_name = EXCLUDED.ps_name,
                    install_date = EXCLUDED.install_date,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    online_status = EXCLUDED.online_status,
                    description = EXCLUDED.description,
                    valid_flag = EXCLUDED.valid_flag,
                    grid_connection_status = EXCLUDED.grid_connection_status,
                    ps_fault_status = EXCLUDED.ps_fault_status,
                    ps_location = EXCLUDED.ps_location,
                    update_time_api = EXCLUDED.update_time_api,
                    ps_current_time_zone = EXCLUDED.ps_current_time_zone,
                    grid_connection_time = EXCLUDED.grid_connection_time,
                    connect_type = EXCLUDED.connect_type,
                    build_status = EXCLUDED.build_status,
                    ps_type = EXCLUDED.ps_type
            """), {
                "ps_id": station.get("ps_id"),
                "ps_name": station.get("ps_name"),
                "install_date": station.get("install_date"),
                "latitude": station.get("latitude"),
                "longitude": station.get("longitude"),
                "online_status": station.get("online_status"),
                "description": station.get("description"),
                "valid_flag": station.get("valid_flag"),
                "grid_connection_status": station.get("grid_connection_status"),
                "ps_fault_status": station.get("ps_fault_status"),
                "ps_location": station.get("ps_location"),
                "update_time_api": station.get("update_time"),
                "ps_current_time_zone": station.get("ps_current_time_zone"),
                "grid_connection_time": station.get("grid_connection_time"),
                "connect_type": station.get("connect_type"),
                "build_status": station.get("build_status"),
                "ps_type": station.get("ps_type")
            })
        
        session.commit()
        logging.info(f"Successfully synced {len(all_stations)} power stations to PostgreSQL.")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error during power station sync: {e}")
    finally:
        session.close()

def sync_all_devices():
    """Fetches all devices for all power stations and stores/updates them in PostgreSQL."""
    if not engine or not Session:
        logging.error("Database not initialized. Cannot sync devices.")
        return
    
    logging.info("Starting device synchronization for all power stations...")
    print("Starting device synchronization for all power stations...")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT ps_id FROM raw.isolarcloud_power_stations"))
            power_station_ids = [row[0] for row in result]
        
        if not power_station_ids:
            print("No power stations found in database. Please sync power stations first.")
            logging.warning("No power stations found in database. Please sync power stations first.")
            return
        
        print(f"Found {len(power_station_ids)} power stations. Syncing devices for each...")
        logging.info(f"Found {len(power_station_ids)} power stations. Syncing devices for each...")
        
        for idx, ps_id in enumerate(power_station_ids, 1):
            print(f"\n[{idx}/{len(power_station_ids)}] Syncing devices for power station: {ps_id}")
            logging.info(f"[{idx}/{len(power_station_ids)}] Syncing devices for power station: {ps_id}")
            try:
                sync_devices(ps_id)
                print(f"Successfully synced devices for power station: {ps_id}")
            except Exception as e:
                print(f"Error syncing devices for power station {ps_id}: {e}")
                logging.error(f"Error syncing devices for power station {ps_id}: {e}")
                # Continue with next power station even if one fails
                continue
        
        print(f"\nCompleted device synchronization for all {len(power_station_ids)} power stations.")
        logging.info(f"Completed device synchronization for all {len(power_station_ids)} power stations.")
        
    except Exception as e:
        logging.error(f"Error fetching power station IDs from database: {e}")
        print(f"Error fetching power station IDs from database: {e}")

def sync_devices(power_station_id):
    """Fetches all devices for a given power station and stores/updates them in PostgreSQL."""
    if not engine or not Session:
        logging.error("Database not initialized. Cannot sync devices.")
        return

    logging.info(f"Starting device synchronization for power station ID: {power_station_id}...")
    all_devices = []
    current_page = 1
    page_size = 50  

    while True:
        payload = {
            "ps_id": power_station_id,
            "curPage": current_page,
            "size": page_size,
        }
        data = _make_api_request("/openapi/getDeviceList", payload)
        time.sleep(REQUEST_DELAY_SECONDS)

        if not data:
            print(f"No data received from getDeviceList page {current_page} for ps_id {power_station_id}. Ending sync for this PS.")
            logging.warning(f"No data received from getDeviceList page {current_page} for ps_id {power_station_id}. Ending sync for this PS.")
            break
        
        # Check for result_data first, as that's the structure we're seeing in the API response
        if 'result_data' in data:
            print(f"Found result_data structure, keys: {data['result_data'].keys() if isinstance(data['result_data'], dict) else 'not a dict'}")
            # If pageList is inside result_data
            if isinstance(data['result_data'], dict) and 'pageList' in data['result_data']:
                devices_on_page = data['result_data'].get('pageList', [])
                print(f"Found {len(devices_on_page)} devices in result_data.pageList for PS {power_station_id}")
            else:
                print(f"No pageList found in result_data for PS {power_station_id}")
                devices_on_page = []
        else:        
            # Original structure as expected
            devices_on_page = data.get("pageList", [])
            print(f"Using original structure, found {len(devices_on_page)} devices in pageList for PS {power_station_id}")

        if not devices_on_page:
            print(f"No devices found for power station {power_station_id} on page {current_page}.")
            logging.info(f"No more devices found for power station {power_station_id} on page {current_page}.")
            break
        
        all_devices.extend(devices_on_page)
        logging.info(f"Fetched page {current_page} with {len(devices_on_page)} devices for power station {power_station_id}.")

        if len(devices_on_page) < page_size or data.get("rowCount", 0) == len(all_devices):
            logging.info(f"All device pages fetched for power station {power_station_id}.")
            break
        current_page += 1

    if not all_devices:
        print(f"No devices to sync for power station {power_station_id}.")
        logging.info(f"No devices to sync for power station {power_station_id}.")
        return

    print(f"Attempting to sync {len(all_devices)} devices for power station {power_station_id}")
    try:
        session = Session()
        print(f"Database session created for device sync")
        for idx, device in enumerate(all_devices):
            print(f"Processing device {idx+1}/{len(all_devices)}: {device.get('ps_key', 'Unknown key')}")
            # Skip device if no ps_key since that's our primary key
            if not device.get('ps_key'):
                print(f"Skipping device - missing ps_key")
                continue
            # Use UPSERT (INSERT ... ON CONFLICT DO UPDATE)
            session.execute(text("""
                INSERT INTO raw.isolarcloud_devices (
                    device_ps_key, ps_id, device_type, type_name, device_sn,
                    dev_status, factory_name, uuid, grid_connection_date,
                    device_name, dev_fault_status, rel_state, device_code,
                    device_model_id, communication_dev_sn, device_model_code, chnnl_id
                ) VALUES (
                    :device_ps_key, :ps_id, :device_type, :type_name, :device_sn,
                    :dev_status, :factory_name, :uuid, :grid_connection_date,
                    :device_name, :dev_fault_status, :rel_state, :device_code,
                    :device_model_id, :communication_dev_sn, :device_model_code, :chnnl_id
                ) ON CONFLICT (device_ps_key) DO UPDATE SET
                    ps_id = EXCLUDED.ps_id,
                    device_type = EXCLUDED.device_type,
                    type_name = EXCLUDED.type_name,
                    device_sn = EXCLUDED.device_sn,
                    dev_status = EXCLUDED.dev_status,
                    factory_name = EXCLUDED.factory_name,
                    uuid = EXCLUDED.uuid,
                    grid_connection_date = EXCLUDED.grid_connection_date,
                    device_name = EXCLUDED.device_name,
                    dev_fault_status = EXCLUDED.dev_fault_status,
                    rel_state = EXCLUDED.rel_state,
                    device_code = EXCLUDED.device_code,
                    device_model_id = EXCLUDED.device_model_id,
                    communication_dev_sn = EXCLUDED.communication_dev_sn,
                    device_model_code = EXCLUDED.device_model_code,
                    chnnl_id = EXCLUDED.chnnl_id
            """), {
                "device_ps_key": device.get("ps_key"),
                "ps_id": device.get("ps_id"),
                "device_type": device.get("device_type"),
                "type_name": device.get("type_name"),
                "device_sn": device.get("device_sn"),
                "dev_status": device.get("dev_status"),
                "factory_name": device.get("factory_name"),
                "uuid": device.get("uuid"),
                "grid_connection_date": device.get("grid_connection_date"),
                "device_name": device.get("device_name"),
                "dev_fault_status": device.get("dev_fault_status"),
                "rel_state": device.get("rel_state"),
                "device_code": device.get("device_code"),
                "device_model_id": device.get("device_model_id"),
                "communication_dev_sn": device.get("communication_dev_sn"),
                "device_model_code": device.get("device_model_code"),
                "chnnl_id": device.get("chnnl_id")
            })
        
        session.commit()
        logging.info(f"Successfully synced {len(all_devices)} devices for ps_id {power_station_id} to PostgreSQL.")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error during device sync for ps_id {power_station_id}: {e}")
    finally:
        session.close()
