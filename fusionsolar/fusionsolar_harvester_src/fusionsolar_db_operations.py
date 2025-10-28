import logging
import pprint
import time
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from .fusionsolar_config import DATABASE_URL, REQUEST_DELAY_SECONDS, MAX_PLANTS_PER_REQUEST
from .fusionsolar_api_client import _make_api_request

# Global database engine and session
# Initialize them immediately to avoid scope issues
try:
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    logging.info("Database engine initialized at module import")
except Exception as e:
    engine = None
    Session = None
    logging.error(f"Failed to initialize database at module import: {e}")

def init_database():
    """Initializes the PostgreSQL database connection and creates a session factory."""
    global engine, Session
    
    # If already initialized, just return
    if engine is not None and Session is not None:
        return True
        
    try:
        # Reinitialize if not already done
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        
        # Create tables if they don't exist
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fusionsolar_plants (
                    plant_code VARCHAR(255) PRIMARY KEY,
                    plant_name VARCHAR(255),
                    grid_connection_date TIMESTAMP WITH TIME ZONE,
                    latitude FLOAT,
                    longitude FLOAT,
                    capacity FLOAT,
                    status INTEGER,
                    last_update TIMESTAMP WITH TIME ZONE
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fusionsolar_devices (
                    dev_id VARCHAR(255) PRIMARY KEY,
                    plant_code VARCHAR(255),
                    dev_type_id INTEGER,
                    dev_name VARCHAR(255),
                    esn_code VARCHAR(255),
                    software_version VARCHAR(255),
                    inv_type VARCHAR(255),
                    model VARCHAR(255),
                    manufacturer VARCHAR(255),
                    status INTEGER,
                    last_update TIMESTAMP WITH TIME ZONE,
                    FOREIGN KEY (plant_code) REFERENCES fusionsolar_plants(plant_code)
                )
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fusionsolar_historical_data (
                    dev_id VARCHAR(255),
                    collect_time TIMESTAMP WITH TIME ZONE,
                    measurement_data JSONB,
                    PRIMARY KEY (dev_id, collect_time),
                    FOREIGN KEY (dev_id) REFERENCES fusionsolar_devices(dev_id)
                )
            """))
            conn.commit()
        
        logging.info("PostgreSQL database initialized successfully.")
        return True
    except Exception as e:
        logging.error(f"Failed to initialize PostgreSQL database: {e}")
        return False

def sync_plants():
    """Fetches all plants and stores/updates them in PostgreSQL."""
    if not engine or not Session:
        logging.error("Database not initialized. Cannot sync plants.")
        return

    logging.info("Starting plant synchronization...")
    all_plants = []
    current_page = 1
    page_size = MAX_PLANTS_PER_REQUEST

    while True:
        logging.info(f"Fetching page {current_page} of plants...")
        payload = {
            "pageNo": current_page,
            "pageSize": page_size
        }
        data = _make_api_request("/thirdData/stations", payload)
        time.sleep(REQUEST_DELAY_SECONDS)

        if not data:
            logging.warning(f"No data received from stations API page {current_page}. Ending sync.")
            break

        # Extract plant list from the response
        if 'data' in data and 'list' in data['data']:
            plants_on_page = data['data']['list']
            if not plants_on_page:
                logging.info("No more plants found on current page.")
                break
            
            all_plants.extend(plants_on_page)
            logging.info(f"Fetched page {current_page} with {len(plants_on_page)} plants.")
            
            # Check if we've reached the last page
            if len(plants_on_page) < page_size or current_page >= data['data']['pageCount']:
                logging.info("All plant pages fetched.")
                break
            
            current_page += 1
        else:
            logging.warning("Unexpected response format from stations API. Ending sync.")
            break

    if not all_plants:
        logging.info("No plants to sync.")
        return

    logging.info(f"Syncing {len(all_plants)} plants to database")
    try:
        session = Session()
        for idx, plant in enumerate(all_plants):
            print(f"Processing plant {idx+1}/{len(all_plants)}: {plant.get('plantCode', 'Unknown Code')}")
            # Convert grid connection date to a proper datetime if present
            grid_connection_date = None
            if 'gridConnectionDate' in plant and plant['gridConnectionDate']:
                try:
                    grid_connection_date = plant['gridConnectionDate']
                except Exception as e:
                    logging.error(f"Error parsing grid connection date: {e}")
            
            # Extract additional plant details if available
            latitude = plant.get('latitude')
            longitude = plant.get('longitude')
            capacity = plant.get('capacity')
            status = plant.get('status')
            
            logging.info(f"Plant details: {plant.get('plantName')}, Latitude: {latitude}, Longitude: {longitude}, Capacity: {capacity}, Status: {status}")
            
            # Use UPSERT (INSERT ... ON CONFLICT DO UPDATE)
            session.execute(text("""
                INSERT INTO fusionsolar_plants (
                    plant_code, plant_name, grid_connection_date, latitude, longitude, capacity, status, last_update
                ) VALUES (
                    :plant_code, :plant_name, :grid_connection_date, :latitude, :longitude, :capacity, :status, NOW()
                ) ON CONFLICT (plant_code) DO UPDATE SET
                    plant_name = EXCLUDED.plant_name,
                    grid_connection_date = EXCLUDED.grid_connection_date,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    capacity = EXCLUDED.capacity,
                    status = EXCLUDED.status,
                    last_update = NOW()
            """), {
                "plant_code": plant.get("plantCode"),
                "plant_name": plant.get("plantName"),
                "grid_connection_date": grid_connection_date,
                "latitude": latitude,
                "longitude": longitude,
                "capacity": capacity,
                "status": status
            })
        
        session.commit()
        logging.info(f"Successfully synced {len(all_plants)} plants.")
        return True
    except SQLAlchemyError as e:
        if session:
            session.rollback()
        logging.error(f"Database error during plant sync: {e}")
        return False
    finally:
        if session:
            session.close()

def sync_devices(plant_code=None):
    """Fetches devices for specified plant(s) and stores/updates them in PostgreSQL."""
    if not engine or not Session:
        logging.error("Database not initialized. Cannot sync devices.")
        return False

    try:
        session = Session()
        
        # If no plant_code is specified, fetch all plant codes from the database
        if not plant_code:
            logging.info("No plant code specified. Fetching all plant codes from database.")
            plant_codes = []
            result = session.execute(text("SELECT plant_code FROM fusionsolar_plants"))
            for row in result:
                plant_codes.append(row[0])
            
            if not plant_codes:
                logging.warning("No plants found in database. Cannot sync devices.")
                return False
            
            logging.info(f"Found {len(plant_codes)} plants in database.")
        else:
            # If a specific plant_code is provided, use that
            plant_codes = [plant_code]
            logging.info(f"Syncing devices for plant code: {plant_code}")
        
        # FusionSolar API allows up to 100 plants per request for getDevList
        # Process plant codes in batches
        all_devices = []
        for i in range(0, len(plant_codes), MAX_PLANTS_PER_REQUEST):
            batch_plant_codes = plant_codes[i:i + MAX_PLANTS_PER_REQUEST]
            station_codes = ",".join(batch_plant_codes)
            
            logging.info(f"Fetching devices for {len(batch_plant_codes)} plants...")
            # According to FusionSolar API docs, this param should be stationCodes
            payload = {
                "stationCodes": station_codes
            }
            
            # Log the request details
            logging.info(f"Requesting devices for station code(s): {station_codes}")
            logging.info(f"Request payload: {payload}")
            
            data = _make_api_request("/thirdData/getDevList", payload)
            time.sleep(REQUEST_DELAY_SECONDS)
            
            if not data:
                logging.warning(f"No data received for plants {station_codes}. Skipping.")
                continue
            
            # Log the full response for debugging
            logging.debug(f"API response for getDevList: {data}")
            
            # Handle different possible response formats
            if 'data' in data and 'list' in data['data']:
                devices_in_batch = data['data']['list']
                logging.info(f"Fetched {len(devices_in_batch)} devices for batch.")
                all_devices.extend(devices_in_batch)
            elif 'data' in data and isinstance(data['data'], list):
                # Handle case where 'data' itself is a list
                devices_in_batch = data['data']
                logging.info(f"Fetched {len(devices_in_batch)} devices for batch (direct list format).")
                all_devices.extend(devices_in_batch)
            elif 'result_data' in data and isinstance(data['result_data'], list):
                # Handle case where data is in 'result_data' field
                devices_in_batch = data['result_data']
                logging.info(f"Fetched {len(devices_in_batch)} devices for batch (result_data format).")
                all_devices.extend(devices_in_batch)
            elif 'result_data' in data and 'list' in data['result_data']:
                # Handle case where devices are in result_data.list
                devices_in_batch = data['result_data']['list']
                logging.info(f"Fetched {len(devices_in_batch)} devices for batch (result_data.list format).")
                all_devices.extend(devices_in_batch)
            else:
                # Log the actual keys in the response to help debug
                logging.warning(f"Unexpected response format from getDevList API for plants {station_codes}.")
                logging.warning(f"Response keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
                if 'data' in data:
                    logging.warning(f"Data type: {type(data['data'])}")
                    if isinstance(data['data'], dict):
                        logging.warning(f"Data keys: {data['data'].keys()}")
                if 'failCode' in data:
                    logging.warning(f"Failed with code: {data.get('failCode')}, message: {data.get('message', 'No message')}")
                    
                # Print entire response in a safer manner for debugging
                try:
                    pretty_data = pprint.pformat(data)
                    logging.debug(f"Full response data: {pretty_data}")
                except Exception as e:
                    logging.error(f"Error pretty-printing response: {e}")
        
        if not all_devices:
            logging.info("No devices to sync.")
            return False
        
        logging.info(f"Syncing {len(all_devices)} devices to database")
        
        # Process all devices and insert/update them in the database
        for device in all_devices:
            session.execute(text("""
                INSERT INTO fusionsolar_devices (
                    dev_id, plant_code, dev_type_id, dev_name, esn_code, 
                    software_version, inv_type, last_update
                ) VALUES (
                    :dev_id, :plant_code, :dev_type_id, :dev_name, :esn_code, 
                    :software_version, :inv_type, NOW()
                ) ON CONFLICT (dev_id) DO UPDATE SET
                    plant_code = EXCLUDED.plant_code,
                    dev_type_id = EXCLUDED.dev_type_id,
                    dev_name = EXCLUDED.dev_name,
                    esn_code = EXCLUDED.esn_code,
                    software_version = EXCLUDED.software_version,
                    inv_type = EXCLUDED.inv_type,
                    last_update = NOW()
            """), {
                "dev_id": device.get("devId", device.get("esnCode")),  # Use devId if available, fall back to esnCode
                "plant_code": device.get("stationCode"),
                "dev_type_id": device.get("devTypeId"),
                "dev_name": device.get("devName"),
                "esn_code": device.get("esnCode"),
                "software_version": device.get("softwareVersion"),
                "inv_type": device.get("invType")
            })
        
        session.commit()
        logging.info(f"Successfully synced {len(all_devices)} devices.")
        
        # Verify with a database query how many devices were actually stored
        try:
            # Count all devices by plant code to provide a summary
            counts_by_plant = {}
            for plant_code in plant_codes:
                count_result = session.execute(text(
                    "SELECT COUNT(*) FROM fusionsolar_devices WHERE plant_code = :plant_code"
                ), {"plant_code": plant_code})
                count = count_result.scalar()
                counts_by_plant[plant_code] = count
            
            # Print a detailed summary
            logging.info("\n==== DEVICE SYNC SUMMARY ====")
            logging.info(f"Total devices processed: {len(all_devices)}")
            for plant_code, count in counts_by_plant.items():
                logging.info(f"Plant {plant_code}: {count} devices")
            logging.info("==========================\n")
            
            # For terminal output as well
            print("\n==== DEVICE SYNC SUMMARY ====")
            print(f"Total devices processed: {len(all_devices)}")
            for plant_code, count in counts_by_plant.items():
                print(f"Plant {plant_code}: {count} devices")
            print("==========================\n")
            
        except Exception as e:
            logging.error(f"Error during verification query: {e}")
        
        return True
    except SQLAlchemyError as e:
        if session:
            session.rollback()
        logging.error(f"Database error during device sync: {e}")
        return False
    finally:
        if session:
            session.close()
