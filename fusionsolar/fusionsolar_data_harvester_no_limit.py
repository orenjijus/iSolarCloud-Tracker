import logging
import argparse
import json
import os
import time
from datetime import datetime, timezone, timedelta
from functools import wraps

# Import modules from the harvester package
from fusionsolar_harvester_src.fusionsolar_config import FUSIONSOLAR_USERNAME, FUSIONSOLAR_PASSWORD, DEVICE_TYPES, MAX_DEVICES_PER_REQUEST, MAX_DAYS_PER_REQUEST, REQUEST_DELAY_SECONDS
from fusionsolar_harvester_src.fusionsolar_api_client import login_fusionsolar, _make_api_request
from fusionsolar_harvester_src.fusionsolar_db_operations import init_database, sync_plants, sync_devices
from fusionsolar_harvester_src.fusionsolar_data_processing import fetch_historical_data, fetch_yesterday_data
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker

# Create logs directory if it doesn't exist
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# Configure ETL logging
log_filename = f"{log_dir}/fusionsolar_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
etl_logger = logging.getLogger('fusionsolar_etl_logger')
etl_logger.setLevel(logging.INFO)

# Create file handler
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to logger
etl_logger.addHandler(file_handler)
etl_logger.addHandler(console_handler)

# Set root logger to use the same handlers (for backward compatibility)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, console_handler]
)

etl_logger.info("=== Starting ETL process for FusionSolar data harvester (no limit) ===")

# Timer decorator for measuring execution time
def timer_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        etl_logger.info(f"Starting {func.__name__}...")
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            etl_logger.info(f"{func.__name__} completed successfully in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            etl_logger.error(f"{func.__name__} failed after {execution_time:.2f} seconds with error: {str(e)}")
            raise
    return wrapper

# Create a patched version of the rate limiter that always allows API calls
from fusionsolar_harvester_src.fusionsolar_rate_limiter import FusionSolarRateLimiter

# Patch the rate limiter to always allow API calls
original_can_make_api_call = FusionSolarRateLimiter.can_make_api_call
original_wait_for_api_call = FusionSolarRateLimiter.wait_for_api_call

def patched_can_make_api_call(self, device_type):
    """Always return True to bypass rate limiting"""
    # Still record the call for tracking purposes
    logging.info(f"BYPASSING RATE LIMIT CHECK for {device_type}")
    return True

def patched_wait_for_api_call(self, device_type, max_wait_time=None):
    """Skip waiting and return True immediately"""
    logging.info(f"BYPASSING RATE LIMIT WAIT for {device_type}")
    return True

# Apply the patches
FusionSolarRateLimiter.can_make_api_call = patched_can_make_api_call
FusionSolarRateLimiter.wait_for_api_call = patched_wait_for_api_call

# Initialize database engine and Session
engine = None
Session = None

def ensure_db_initialized():
    """Ensure the database is initialized for this module."""
    global engine, Session
    if engine is None:
        try:
            # Initialize the database
            if not init_database():
                logging.error("Failed to initialize database")
                return False
                
            # Import the engine and Session directly from the module
            from fusionsolar_harvester_src.fusionsolar_db_operations import engine as db_engine, Session as db_Session
            
            # Use the already initialized engine and Session
            engine = db_engine
            Session = db_Session
            
            if engine is None:
                logging.error("Engine not available from db_operations module")
                return False
                
            logging.info("Database engine initialized in flexible data processing module")
            return True
        except Exception as e:
            logging.error(f"Failed to initialize database in flexible module: {e}")
            return False
    return True

def flexible_fetch_and_store_device_data(devices_to_fetch, start_time_dt, end_time_dt):
    """Flexible version that fetches historical device data and handles various API response formats."""
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot store device data.")
        return 0
    
    if not devices_to_fetch:
        logging.info("No devices provided to fetch_and_store_device_data.")
        return 0

    # Group devices by type
    grouped_by_type = {}
    for device in devices_to_fetch:
        device_type_id = device.get('dev_type_id')
        
        if device_type_id is None:
            logging.warning(f"Device {device.get('dev_id', 'Unknown')} has no device type ID")
            continue
            
        # Find the device type name for this ID
        device_type_name = None
        for name, type_id in DEVICE_TYPES.items():
            if type_id == device_type_id:
                device_type_name = name
                break
                
        if not device_type_name:
            logging.warning(f"Unknown device type ID {device_type_id} for device {device.get('dev_id', 'Unknown')}")
            continue
            
        if device_type_name not in grouped_by_type:
            grouped_by_type[device_type_name] = []
        
        grouped_by_type[device_type_name].append(device)
    
    total_data_points_ingested = 0
    
    # Convert timestamps to milliseconds for the FusionSolar API
    start_timestamp_ms = int(start_time_dt.timestamp() * 1000)
    end_timestamp_ms = int(end_time_dt.timestamp() * 1000)
    
    # Process each device type separately
    for device_type_name, devices_of_type in grouped_by_type.items():
        device_type_id = DEVICE_TYPES.get(device_type_name)
        
        # Process devices in batches as per API limitations
        for i in range(0, len(devices_of_type), MAX_DEVICES_PER_REQUEST):
            device_batch = devices_of_type[i:i + MAX_DEVICES_PER_REQUEST]
            
            # Get device IDs for the API call
            dev_ids = []
            for device in device_batch:
                dev_id = device.get('dev_id')
                if dev_id:
                    dev_ids.append(dev_id)
            
            if not dev_ids:
                logging.warning("No valid device IDs in batch")
                continue
            
            # Create API payload with additional parameters
            plant_code = None
            # Extract plant code from first device if available
            if device_batch and hasattr(device_batch[0], 'get') and device_batch[0].get('plant_code'):
                plant_code = device_batch[0].get('plant_code')
                
            # Use sns parameter based on successful API test results
            payload = {
                "sns": ",".join(str(dev_id) for dev_id in dev_ids),
                "devTypeId": device_type_id,
                "startTime": start_timestamp_ms,
                "endTime": end_timestamp_ms
            }
            
            # Print payload clearly to terminal for monitoring data flow
            print("\n" + "="*50)
            print("API REQUEST PAYLOAD:")
            print("Endpoint: /thirdData/getDevHistoryKpi")
            for key, value in payload.items():
                # Truncate long device ID lists for readability
                if key == "sns" and len(str(value)) > 100:
                    print(f"{key}: {str(value)[:50]}...{str(value)[-10:]} ({len(value.split(','))} devices)")
                else:
                    print(f"{key}: {value}")
            print("="*50)
            
            logging.info("Using 'sns' parameter instead of 'devIds' based on API testing")
            
            # Make API call
            logging.info(f"Fetching historical data for {len(dev_ids)} devices of type {device_type_name}")
            api_response = _make_api_request("/thirdData/getDevHistoryKpi", payload)
            
            # Print response summary to terminal
            print("RESPONSE SUMMARY:")
            if api_response:
                if isinstance(api_response, dict):
                    print(f"Response type: Dictionary with {len(api_response)} keys")
                    print(f"Keys: {list(api_response.keys())}")
                    if 'data' in api_response and api_response['data']:
                        if isinstance(api_response['data'], dict):
                            print(f"Data keys: {list(api_response['data'].keys())}")
                        elif isinstance(api_response['data'], list):
                            print(f"Data: List with {len(api_response['data'])} items")
                elif isinstance(api_response, list):
                    print(f"Response type: List with {len(api_response)} items")
                else:
                    print(f"Response type: {type(api_response).__name__}")
            else:
                print("No response or empty response received")
            print("="*50 + "\n")
            
            # Wait between API calls to avoid overloading
            time.sleep(REQUEST_DELAY_SECONDS)
            
            if not api_response:
                logging.warning("Failed to fetch historical data")
                continue
            
            # FLEXIBLE RESPONSE HANDLING - Try different response formats
            data_points = None
            
            # Check for standard format: data.list
            if 'data' in api_response and isinstance(api_response['data'], dict) and 'list' in api_response['data']:
                data_points = api_response['data']['list']
                logging.info("Using standard response format: data.list")
            
            # Check for alternate format: just 'data' as a list
            elif 'data' in api_response and isinstance(api_response['data'], list):
                data_points = api_response['data']
                logging.info("Using alternate response format: data as list")
            
            # Check for alternate format: array at the root level
            elif isinstance(api_response, list):
                data_points = api_response
                logging.info("Using alternate response format: root as list")
            
            # Check for results field
            elif 'results' in api_response:
                if isinstance(api_response['results'], list):
                    data_points = api_response['results']
                    logging.info("Using alternate response format: results as list")
                elif isinstance(api_response['results'], dict) and 'list' in api_response['results']:
                    data_points = api_response['results']['list']
                    logging.info("Using alternate response format: results.list")
            
            # If we still didn't find data points, log the response structure
            if data_points is None:
                logging.warning("Could not find data points in API response")
                logging.warning(f"API response keys: {list(api_response.keys() if isinstance(api_response, dict) else [])}")
                if 'data' in api_response and isinstance(api_response['data'], dict):
                    logging.warning(f"Data keys: {list(api_response['data'].keys())}")
                continue
            
            if not data_points:
                logging.info(f"No data points returned for the query period")
                continue
            
            logging.info(f"Received {len(data_points)} data points from API")
            
            # Debug the structure of the first data point
            if data_points and len(data_points) > 0:
                sample_point = data_points[0]
                sample_point_keys = list(sample_point.keys()) if isinstance(sample_point, dict) else 'not a dict'
                logging.info(f"Sample data point structure: {sample_point_keys}")
                
                # Print sample data point to terminal
                print("SAMPLE DATA POINT STRUCTURE:")
                print(f"Total data points: {len(data_points)}")
                if isinstance(sample_point, dict):
                    print(f"Sample point keys: {sample_point_keys}")
                    # Print a few sample values
                    print("SAMPLE VALUES:")
                    for key in list(sample_point.keys())[:5]:  # Show first 5 keys
                        print(f"{key}: {sample_point[key]}")
                print("="*50) 
            # Convert data points to database format
            db_records = []
            timestamp_field = 'collectTime'  # Default field name
            
            # Check if we need to use a different timestamp field
            if data_points and isinstance(data_points[0], dict):
                sample_keys = data_points[0].keys()
                if 'collectTime' not in sample_keys:
                    # Try to find alternative timestamp fields
                    timestamp_candidates = ['timestamp', 'time', 'collectionTime', 'dateTime']
                    for candidate in timestamp_candidates:
                        if candidate in sample_keys:
                            timestamp_field = candidate
                            logging.info(f"Using alternate timestamp field: {timestamp_field}")
                            break
                    
                    # If still not found, try case-insensitive search
                    if timestamp_field == 'collectTime':
                        for key in sample_keys:
                            if key.lower() in ['collecttime', 'timestamp', 'datetime']:
                                timestamp_field = key
                                logging.info(f"Using case-insensitive match for timestamp: {timestamp_field}")
                                break
            
            for data_point in data_points:
                if not isinstance(data_point, dict):
                    logging.warning("Data point is not a dictionary, skipping")
                    continue
                    
                if timestamp_field not in data_point:
                    logging.warning(f"Data point missing {timestamp_field}, skipping")
                    continue
                
                # Handle different timestamp formats (milliseconds or seconds)
                collect_time_value = data_point.get(timestamp_field)
                
                # If string, parse it
                if isinstance(collect_time_value, str):
                    try:
                        collect_time = datetime.fromisoformat(collect_time_value.replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            # Try common formats
                            collect_time = datetime.strptime(collect_time_value, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            logging.warning(f"Could not parse timestamp: {collect_time_value}")
                            continue
                else:
                    # Assume numeric timestamp (either seconds or milliseconds)
                    try:
                        # If the timestamp is in milliseconds (typical for FusionSolar)
                        if collect_time_value > 1e10:
                            collect_time = datetime.fromtimestamp(collect_time_value / 1000.0, tz=timezone.utc)
                        else:
                            collect_time = datetime.fromtimestamp(collect_time_value, tz=timezone.utc)
                    except (ValueError, TypeError, OverflowError):
                        logging.warning(f"Invalid timestamp value: {collect_time_value}")
                        continue
                
                # Get SN (serial number) from the data point - this matches our device IDs in the database
                sn = data_point.get('sn')
                if not sn:
                    logging.warning("Data point missing SN, skipping")
                    continue
                    
                # Use the SN as the device ID since that's what's in our database
                dev_id = sn
                
                logging.debug(f"Using SN {sn} as device ID for database insert")
                
                # Handle dataItemMap structure from our test results
                if 'dataItemMap' in data_point and isinstance(data_point['dataItemMap'], dict):
                    measurement_data = data_point['dataItemMap']
                    logging.debug(f"Using dataItemMap format with {len(measurement_data)} measurements")
                else:
                    # Create a JSON object with all the measurements (for older format)
                    measurement_data = {}
                    for key, value in data_point.items():
                        if key not in [timestamp_field, 'devId', 'sn']:  # Skip these fields
                            measurement_data[key] = value
                
                db_records.append({
                    "dev_id": str(dev_id),  # Ensure it's a string
                    "collect_time": collect_time.isoformat(),
                    "measurement_data": json.dumps(measurement_data)
                })
            
            # Insert records into the database
            if db_records:
                try:
                    session = Session()
                    for record in db_records:
                        # Build the column list and values for the INSERT
                        columns = list(record.keys())
                        values = [record[col] for col in columns]
                        
                        # Build the ON CONFLICT DO UPDATE clause
                        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in ['dev_id', 'collect_time']])
                        
                        # Construct and execute the UPSERT query
                        query = text(f"""
                            INSERT INTO fusionsolar_historical_data ({', '.join(columns)})
                            VALUES ({', '.join([':' + col for col in columns])})
                            ON CONFLICT (dev_id, collect_time) DO UPDATE SET
                            {update_clause}
                        """)
                        
                        session.execute(query, record)
                    
                    session.commit()
                    total_data_points_ingested += len(db_records)
                    logging.info(f"Successfully upserted {len(db_records)} data points.")
                except Exception as e:
                    session.rollback()
                    logging.error(f"Database error during data upsert: {e}")
                finally:
                    session.close()
            else:
                logging.info("No data to insert into database for this API data batch.")
    
    return total_data_points_ingested

def flexible_fetch_historical_data(start_date_str, end_date_str, plant_codes_str=None, device_types_str=None, device_ids_str=None):
    """Flexible version that fetches historical data with improved response handling."""
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot fetch historical data.")
        return 0
    
    try:
        # Parse date strings
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        
        if start_date > end_date:
            logging.error("Start date cannot be after end date.")
            return 0
        
        # Build SQL filter conditions
        conditions = []
        query_params = {}
        
        if plant_codes_str:
            plant_codes = [code.strip() for code in plant_codes_str.split(',')]
            if len(plant_codes) == 1:
                conditions.append("d.plant_code = :plant_code")
                query_params['plant_code'] = plant_codes[0]
            else:
                conditions.append("d.plant_code IN :plant_codes")
                query_params['plant_codes'] = tuple(plant_codes)
        
        if device_types_str:
            device_type_names = [dtype.strip() for dtype in device_types_str.split(',')]
            device_type_ids = []
            for name in device_type_names:
                if name in DEVICE_TYPES:
                    device_type_ids.append(DEVICE_TYPES[name])
            
            if len(device_type_ids) == 1:
                conditions.append("d.dev_type_id = :device_type_id")
                query_params['device_type_id'] = device_type_ids[0]
            elif len(device_type_ids) > 1:
                conditions.append("d.dev_type_id IN :device_type_ids")
                query_params['device_type_ids'] = tuple(device_type_ids)
                
        # Add filter for specific device IDs if provided
        if device_ids_str:
            device_ids = [id.strip() for id in device_ids_str.split(',')]
            if len(device_ids) == 1:
                conditions.append("d.dev_id = :device_id")
                query_params['device_id'] = device_ids[0]
            else:
                conditions.append("d.dev_id IN :device_ids")
                query_params['device_ids'] = tuple(device_ids)
        
        # Build the SQL WHERE clause
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Fetch devices from database
        session = Session()
        query = f"""
            SELECT d.dev_id, d.plant_code, d.dev_type_id, d.dev_name 
            FROM fusionsolar_devices d
            WHERE {where_clause}
        """
        
        result = session.execute(text(query), query_params)
        devices = [dict(row._mapping) for row in result]
        session.close()
        
        if not devices:
            logging.warning("No devices found matching the filter criteria.")
            return 0
        
        logging.info(f"Found {len(devices)} devices matching the filter criteria.")
        
        # Process data in batches of MAX_DAYS_PER_REQUEST days
        total_points_ingested = 0
        current_date = start_date
        
        while current_date < end_date:
            batch_end_date = min(current_date + timedelta(days=MAX_DAYS_PER_REQUEST), end_date)
            
            current_date_str = current_date.strftime('%Y-%m-%d')
            batch_end_date_str = batch_end_date.strftime('%Y-%m-%d')
            
            logging.info(f"Processing data from {current_date_str} to {batch_end_date_str}")
            
            # Fetch and store data using our flexible function
            points_ingested = flexible_fetch_and_store_device_data(devices, current_date, batch_end_date)
            total_points_ingested += points_ingested
            
            logging.info(f"Processed {points_ingested} data points for {current_date_str} to {batch_end_date_str}")
            
            # Move to the next batch
            current_date = batch_end_date + timedelta(seconds=1)
        
        return total_points_ingested
    except Exception as e:
        logging.error(f"Error fetching historical data: {e}")
        return 0

# Apply timer decorator to key functions
timed_flexible_fetch_historical_data = timer_decorator(flexible_fetch_historical_data)
timed_flexible_fetch_and_store_device_data = timer_decorator(flexible_fetch_and_store_device_data)
timed_sync_plants = timer_decorator(sync_plants)
timed_sync_devices = timer_decorator(sync_devices)

def main():
    # Start timing for the entire ETL process
    total_start_time = time.time()
    etl_status = "SUCCESS"  # Assume success unless an error occurs
    action_performed = False
    
    etl_logger.info("Starting FusionSolar Data Harvester (NO RATE LIMIT VERSION)")
    etl_logger.warning("WARNING: This version bypasses rate limits. Use for testing only!")
    
    # Initialize PostgreSQL database
    etl_logger.info("Initializing database connection")
    try:
        if not init_database():
            etl_logger.error("Database initialization failed")
            etl_logger.error("Exiting script due to database initialization failure.")
            etl_status = "FAILED"
            return
        etl_logger.info("Database initialization successful")
    except Exception as e:
        etl_logger.error(f"Database initialization error: {str(e)}")
        etl_status = "FAILED"
        return

    # Set up command line argument parser
    parser = argparse.ArgumentParser(description="FusionSolar Data Harvester (No Rate Limit)")
    parser.add_argument("--sync-plants", action="store_true", help="Synchronize all plants.")
    parser.add_argument("--sync-devices", type=str, metavar="PLANT_CODE", help="Synchronize devices for a specific plant code. Use 'all' to sync devices for all known plants.")
    
    parser.add_argument("--fetch-historical", nargs=2, metavar=("YYYY-MM-DD_START", "YYYY-MM-DD_END"), 
                        help="Fetch historical minute data for a date range.")
    parser.add_argument("--plant-codes", type=str, help="Comma-separated list of plant codes to filter for --fetch-historical.")
    parser.add_argument("--device-types", type=str, help="Comma-separated list of device type names (inverter, meter, & meteo_station) to filter for --fetch-historical.")
    parser.add_argument("--device-ids", type=str, help="Comma-separated list of specific device IDs to filter (e.g., '1001,1002,1003').")

    parser.add_argument("--fetch-yesterday", action="store_true", help="Fetch all of yesterday's data for all devices.")
    parser.add_argument("--create-views", action="store_true", help="Create SQL views for site-device data.")
    
    args = parser.parse_args()
    print(f"Arguments parsed: {vars(args)}")

    # Check if any action was specified
    if not any(vars(args).values()): 
        print("No arguments were provided")
        parser.print_help()
        logging.info("No action specified. Exiting.")
        return

    # Only attempt login if we're going to make API calls
    if args.sync_plants or args.sync_devices or args.fetch_historical or args.fetch_yesterday:
        # Login to FusionSolar API
        print("Attempting to login to FusionSolar API")
        if not login_fusionsolar():
            print("FusionSolar API login failed")
            logging.error("Exiting script due to FusionSolar API login failure.")
            return
        print("FusionSolar API login successful")
    
    try:
        # Process the requested actions
        if args.sync_plants:
            action_performed = True
            etl_logger.info("Action: Synchronizing plants")
            try:
                timed_sync_plants()
                etl_logger.info("Plants synchronization completed successfully")
            except Exception as e:
                etl_logger.error(f"Plant synchronization failed: {str(e)}")
                etl_status = "FAILED"

        if args.sync_devices:
            action_performed = True
            if args.sync_devices.lower() == 'all':
                etl_logger.info("Action: Synchronizing devices for all plants")
                try:
                    timed_sync_devices()
                    etl_logger.info("Devices synchronization for all plants completed successfully")
                except Exception as e:
                    etl_logger.error(f"Devices synchronization for all plants failed: {str(e)}")
                    etl_status = "FAILED"
            else:
                etl_logger.info(f"Action: Synchronizing devices for plant code: {args.sync_devices}")
                try:
                    timed_sync_devices(args.sync_devices)
                    etl_logger.info(f"Devices synchronization for plant {args.sync_devices} completed successfully")
                except Exception as e:
                    etl_logger.error(f"Devices synchronization for plant {args.sync_devices} failed: {str(e)}")
                    etl_status = "FAILED"

        if args.fetch_historical:
            action_performed = True
            start_date, end_date = args.fetch_historical
            etl_logger.info(f"Action: Fetching historical data from {start_date} to {end_date}")
            
            # Use our timed patched version instead of the original
            try:
                points_ingested = timed_flexible_fetch_historical_data(start_date, end_date, args.plant_codes, args.device_types, args.device_ids)
                etl_logger.info(f"Total data points ingested: {points_ingested}")
                if points_ingested == 0:
                    etl_logger.warning("No data points were ingested during fetch historical operation")
            except Exception as e:
                etl_logger.error(f"Historical data fetch failed: {str(e)}")
                etl_status = "FAILED"
            
            # Check if data was inserted into the database
            if ensure_db_initialized():
                try:
                    session = Session()
                    
                    # Query to check if data was inserted
                    query = text("""
                        SELECT COUNT(*) FROM fusionsolar_historical_data
                        WHERE collect_time BETWEEN :start_date AND :end_date
                    """)
                    
                    result = session.execute(query, {
                        'start_date': start_date + ' 00:00:00',
                        'end_date': end_date + ' 23:59:59'
                    }).scalar()
                    
                    etl_logger.info(f"Found {result} records in database for the specified date range")
                    print(f"Found {result} records in database for the specified date range")
                    
                    # If we have records, show sample data
                    if result > 0:
                        sample_query = text("""
                            SELECT dev_id, collect_time, measurement_data 
                            FROM fusionsolar_historical_data
                            WHERE collect_time BETWEEN :start_date AND :end_date
                            LIMIT 1
                        """)
                        
                        sample = session.execute(sample_query, {
                            'start_date': start_date + ' 00:00:00',
                            'end_date': end_date + ' 23:59:59'
                        }).fetchone()
                        
                        if sample:
                            logging.info(f"Sample record - DevID: {sample[0]}, Time: {sample[1]}, Data: {sample[2][:100]}...")
                except Exception as e:
                    logging.error(f"Error checking database for inserted data: {e}")
                finally:
                    session.close()

        if args.fetch_yesterday:
            action_performed = True
            etl_logger.info("Action: Fetching yesterday's data for all devices")
            try:
                timed_fetch_yesterday_data = timer_decorator(fetch_yesterday_data)
                timed_fetch_yesterday_data()
                etl_logger.info("Yesterday's data fetch completed successfully")
            except Exception as e:
                etl_logger.error(f"Yesterday's data fetch failed: {str(e)}")
                etl_status = "FAILED"

        if args.create_views:
            action_performed = True
            etl_logger.info("Action: Creating SQL views for site-device data")
            try:
                # Import here to avoid circular imports
                from site_device_views import create_device_views
                timed_create_device_views = timer_decorator(create_device_views)
                timed_create_device_views()
                etl_logger.info("SQL views created successfully")
            except Exception as e:
                etl_logger.error(f"SQL views creation failed: {str(e)}")
                etl_status = "FAILED"
                
        # If no action was performed but arguments were parsed successfully
        if not action_performed:
            etl_logger.warning("No ETL actions were performed despite parsing arguments successfully")
            
    except Exception as e:
        etl_status = "FAILED"
        etl_logger.error(f"Unexpected error during execution: {str(e)}")
    finally:
        # Log total execution time
        total_end_time = time.time()
        total_execution_time = total_end_time - total_start_time
        etl_logger.info(f"=== ETL process completed with status: {etl_status} ===")
        etl_logger.info(f"Total execution time: {total_execution_time:.2f} seconds")
        
        # Write a simple status file that can be easily parsed by monitoring tools
        with open(f"{log_dir}/fusionsolar_last_etl_status.txt", "w") as status_file:
            status_file.write(f"timestamp: {datetime.now().isoformat()}\n")
            status_file.write(f"status: {etl_status}\n")
            status_file.write(f"execution_time: {total_execution_time:.2f}\n")
            
            # Add additional status fields that match the iSolarCloud ETL log format
            if args.fetch_historical:
                start_date, end_date = args.fetch_historical
                status_file.write(f"start_date: {start_date}\n")
                status_file.write(f"end_date: {end_date}\n")
                
                # For device type, get from args if available
                device_type = args.device_types if args.device_types else "ALL"
                status_file.write(f"device_type: {device_type}\n")
                
                # For power station/plant, get from args if available
                plant_codes = args.plant_codes if args.plant_codes else "ALL"
                status_file.write(f"power_station: {plant_codes}\n")
            elif args.fetch_yesterday:
                # For yesterday data
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                status_file.write(f"start_date: {yesterday}\n")
                status_file.write(f"end_date: {yesterday}\n")
                status_file.write(f"device_type: ALL\n")
                status_file.write(f"power_station: ALL\n")
            # Write command line arguments
            for arg, value in vars(args).items():
                if value is not None:
                    status_file.write(f"{arg}: {value}\n")
                    
        etl_logger.info("FusionSolar Data Harvester (No Rate Limit) completed")

if __name__ == "__main__":
    main()
