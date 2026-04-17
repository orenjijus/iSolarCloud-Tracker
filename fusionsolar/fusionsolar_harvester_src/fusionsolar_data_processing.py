import logging
print("CASCADE_DEBUG: fusionsolar_data_processing.py WAS LOADED AND IS RUNNING", flush=True) # Unmissable print
import time
import json
import math
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from .fusionsolar_config import (
    MAX_DEVICES_PER_REQUEST, MAX_DAYS_PER_REQUEST, REQUEST_DELAY_SECONDS, 
    DEVICE_TYPES, DEVICE_TYPE_MEASURING_POINTS, DATABASE_URL
)
from .fusionsolar_api_client import _make_api_request
# We'll reinitialize these locally to avoid scope issues
from .fusionsolar_db_operations import init_database
from .fusionsolar_rate_limiter import FusionSolarRateLimiter

# Patch the rate limiter to always allow API calls (bypass rate limiting like no_limit version)
# IMPORTANT: Patch BEFORE creating any instance (sama seperti no_limit version)
original_can_make_api_call = FusionSolarRateLimiter.can_make_api_call
original_wait_for_api_call = FusionSolarRateLimiter.wait_for_api_call

def patched_can_make_api_call(self, device_type):
    """Always return True to bypass rate limiting"""
    logging.info(f"BYPASSING RATE LIMIT CHECK for {device_type}")
    return True

def patched_wait_for_api_call(self, device_type, max_wait_time=None):
    """Skip waiting and return True immediately"""
    logging.info(f"BYPASSING RATE LIMIT WAIT for {device_type}")
    return True

# Apply the patches (sama seperti no_limit version)
FusionSolarRateLimiter.can_make_api_call = patched_can_make_api_call
FusionSolarRateLimiter.wait_for_api_call = patched_wait_for_api_call

# Initialize the rate limiter AFTER patching (untuk progress tracking saja, tidak untuk blocking)
rate_limiter = FusionSolarRateLimiter(test_mode=True)

# Initialize our own engine and Session to ensure they're available in this module
engine = None
Session = None

def ensure_db_initialized():
    """Ensure the database is initialized for this module."""
    global engine, Session
    if engine is None:
        try:
            # Initialize directly in this module
            engine = create_engine(DATABASE_URL)
            Session = sessionmaker(bind=engine)
            logging.info("Database engine initialized in data_processing module")
            return True
        except Exception as e:
            logging.error(f"Failed to initialize database in data_processing module: {e}")
            return False
    return True

def _map_device_type_name_for_points(device):
    """Helper to determine the standardized device type name for point lookup."""
    device_type_id = device.get('dev_type_id')
    
    if device_type_id is None:
        logging.warning(f"Device {device.get('dev_id', 'Unknown')} has no device type ID")
        return 'unknown'
    
    # Map device type ID to our standard names
    for name, type_id in DEVICE_TYPES.items():
        if type_id == device_type_id:
            return name
    
    logging.warning(f"Unknown device type ID {device_type_id} for device {device.get('dev_id', 'Unknown')}")
    return 'unknown'

def fetch_and_store_device_data(devices_to_fetch, start_time_dt, end_time_dt):
    """Fetches historical device data and stores it in PostgreSQL."""
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot store device data.")
        return 0
    
    if not devices_to_fetch:
        logging.info("No devices provided to fetch_and_store_device_data.")
        return 0

    # Group devices by type
    grouped_by_type = {}
    for device in devices_to_fetch:
        device_type_name = _map_device_type_name_for_points(device)
        if device_type_name == 'unknown':
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
        if not device_type_id:
            logging.warning(f"No device type ID mapping for {device_type_name}")
            continue
        
        # Process devices in batches as per API limitations
        for i in range(0, len(devices_of_type), MAX_DEVICES_PER_REQUEST):
            device_batch = devices_of_type[i:i + MAX_DEVICES_PER_REQUEST]
            
            # Get device identifiers for the API call (endpoint baru membutuhkan devDn).
            dev_ids = []
            dev_dns = []
            for device in device_batch:
                dev_id = device.get('dev_id')
                if dev_id is None:
                    continue
                s = str(dev_id).strip()
                if not s or s.lower() == 'none':
                    logging.debug(f"Skipping device with invalid dev_id: {repr(dev_id)}")
                    continue
                dev_ids.append(s)
                dev_dn = device.get('dev_dn')
                if dev_dn is not None:
                    dn = str(dev_dn).strip()
                    if dn and dn.lower() != 'none':
                        dev_dns.append(dn)
            
            if not dev_ids:
                logging.warning("No valid device IDs in batch")
                continue
            if not dev_dns:
                logging.warning("No valid devDn in batch. Skipping because /device/history requires devDn.")
                continue
            
            # /rest/openapi/pvms/nbi/v1/device/history: one devDn per request (docs: max one device / 24h of 5-min data).
            if len(dev_dns) != 1:
                logging.error("Expected exactly one devDn per device/history call; check MAX_DEVICES_PER_REQUEST")
                continue
            payload = {
                "devDn": dev_dns[0],
                "devTypeId": device_type_id,
                "startTime": start_timestamp_ms,
                "endTime": end_timestamp_ms
            }
            logging.info("Using single devDn for device/history (API: one device per query)")
            
            # Print payload clearly to terminal for monitoring data flow
            print("\n" + "="*50)
            print("API REQUEST PAYLOAD:")
            print("Endpoint: /rest/openapi/pvms/nbi/v1/device/history")
            for key, value in payload.items():
                # Truncate long device ID lists for readability
                if key in ("sns", "devIds", "devDn") and len(str(value)) > 100:
                    print(f"{key}: {str(value)[:50]}...{str(value)[-10:]} ({len(value.split(','))} devices)")
                else:
                    print(f"{key}: {value}")
            print("="*50)
            
            # Make API call
            logging.info(f"Fetching historical data for {len(dev_ids)} devices of type {device_type_name}")
            api_response = _make_api_request("/rest/openapi/pvms/nbi/v1/device/history", payload)
            
            # Print response summary to terminal (sama seperti no_limit version)
            print("RESPONSE SUMMARY:")
            if api_response:
                if isinstance(api_response, dict):
                    print(f"  success: {api_response.get('success')}")
                    print(f"  message: {api_response.get('message', 'N/A')}")
                    print(f"  failCode: {api_response.get('failCode', 'N/A')}")
                    print(f"Response type: Dictionary with {len(api_response)} keys")
                    print(f"Keys: {list(api_response.keys())}")
                    if 'data' in api_response:
                        d = api_response['data']
                        if d is None:
                            print("  data: None")
                        elif isinstance(d, dict):
                            print(f"  data: dict with keys: {list(d.keys())}")
                            if 'list' in d:
                                print(f"  data.list: {len(d['list'])} items")
                            else:
                                for k, v in list(d.items())[:3]:
                                    print(f"  data[{k!r}]: {type(v).__name__} {len(v) if isinstance(v, (list, dict)) else repr(v)[:60]}")
                        elif isinstance(d, list):
                            print(f"  data: list with {len(d)} items")
                            if len(d) == 0:
                                print("  (empty list - API has no historical data for this device/period)")
                        else:
                            print(f"  data: {type(d).__name__}")
                    else:
                        print("  (no 'data' key)")
                elif isinstance(api_response, list):
                    print(f"Response type: List with {len(api_response)} items")
                else:
                    print(f"Response type: {type(api_response).__name__}")
            else:
                print("No response or empty response received")
            print("="*50 + "\n")
            
            if not api_response:
                logging.warning("Failed to fetch historical data")
                continue
            
            # TAMBAHKAN LOGGING UNTUK DEBUG - Lihat response format yang sebenarnya
            logging.info(f"API Response type: {type(api_response)}")
            if isinstance(api_response, dict):
                logging.info(f"API Response keys: {list(api_response.keys())}")
                if 'data' in api_response:
                    if isinstance(api_response['data'], dict):
                        logging.info(f"API Response data keys: {list(api_response['data'].keys())}")
                        # Log sample of data dict content
                        if api_response['data']:
                            logging.info(f"API Response data dict sample (first 3 keys): {list(api_response['data'].keys())[:3]}")
                    elif isinstance(api_response['data'], list):
                        logging.info(f"API Response data is a list with {len(api_response['data'])} items")
                else:
                    logging.warning(f"API Response does NOT have 'data' key. Available keys: {list(api_response.keys())}")
                if 'failCode' in api_response:
                    logging.info(f"API Response has failCode: {api_response.get('failCode')}, message: {api_response.get('message', 'No message')}")
            elif isinstance(api_response, list):
                logging.info(f"API Response is a list with {len(api_response)} items")
            
            # FLEXIBLE RESPONSE HANDLING - Handle berbagai format (sama seperti no_limit version)
            data_points = None
            
            # Check for standard format: data.list
            if 'data' in api_response and api_response['data'] is not None:
                if isinstance(api_response['data'], dict) and 'list' in api_response['data']:
                    data_points = api_response['data']['list']
                    logging.info("Using standard response format: data.list")
                # Check for alternate format: just 'data' as a list
                elif isinstance(api_response['data'], list):
                    data_points = api_response['data']
                    logging.info("Using alternate response format: data as list")
                # Check if data is empty dict or None
                elif not api_response['data']:
                    logging.warning("API response has 'data' key but it's empty or None")
            # Handle case where data key exists but is None
            elif 'data' in api_response and api_response['data'] is None:
                logging.warning("API response has 'data' key but value is None")
            
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
            
            # If we still didn't find data points, log the response structure dengan detail
            if data_points is None:
                logging.error("=" * 60)
                logging.error("COULD NOT FIND DATA POINTS IN API RESPONSE")
                logging.error("=" * 60)
                logging.error(f"API response type: {type(api_response)}")
                if isinstance(api_response, dict):
                    logging.error(f"API response keys: {list(api_response.keys())}")
                    # Log full response structure for debugging
                    for key in api_response.keys():
                        value = api_response[key]
                        if isinstance(value, dict):
                            logging.error(f"  Key '{key}' is dict with keys: {list(value.keys())}")
                        elif isinstance(value, list):
                            logging.error(f"  Key '{key}' is list with {len(value)} items")
                        else:
                            logging.error(f"  Key '{key}' is {type(value).__name__}: {str(value)[:100]}")
                    
                    if 'data' in api_response:
                        if isinstance(api_response['data'], dict):
                            logging.error(f"Data dict keys: {list(api_response['data'].keys())}")
                            # Log sample of nested structure
                            for key in list(api_response['data'].keys())[:5]:
                                val = api_response['data'][key]
                                if isinstance(val, (dict, list)):
                                    logging.error(f"  data['{key}'] is {type(val).__name__} with {len(val) if isinstance(val, list) else len(val.keys())} items")
                                else:
                                    logging.error(f"  data['{key}'] = {str(val)[:50]}")
                        elif isinstance(api_response['data'], list):
                            logging.error(f"Data is a list with {len(api_response['data'])} items")
                            if len(api_response['data']) > 0:
                                logging.error(f"First item type: {type(api_response['data'][0])}")
                                if isinstance(api_response['data'][0], dict):
                                    logging.error(f"First item keys: {list(api_response['data'][0].keys())}")
                    else:
                        logging.error("Response does NOT have 'data' key!")
                    
                    if 'failCode' in api_response:
                        logging.error(f"failCode: {api_response.get('failCode')}, message: {api_response.get('message')}")
                elif isinstance(api_response, list):
                    logging.error(f"Response is a list with {len(api_response)} items")
                    if len(api_response) > 0:
                        logging.error(f"First item type: {type(api_response[0])}")
                        if isinstance(api_response[0], dict):
                            logging.error(f"First item keys: {list(api_response[0].keys())}")
                logging.error("=" * 60)
                continue
            
            if not data_points:
                logging.info(f"No data points returned for the query period")
                continue
            
            logging.info(f"Received {len(data_points)} data points from API")
            
            # Debug the structure of the first data point (sama seperti no_limit version)
            if data_points and len(data_points) > 0:
                sample_point = data_points[0]
                sample_point_keys = list(sample_point.keys()) if isinstance(sample_point, dict) else 'not a dict'
                logging.info(f"Sample data point structure: {sample_point_keys}")
                
                # Print sample data point to terminal (sama seperti no_limit version)
                print("SAMPLE DATA POINT STRUCTURE:")
                print(f"Total data points: {len(data_points)}")
                if isinstance(sample_point, dict):
                    print(f"Sample point keys: {sample_point_keys}")
                    # Print a few sample values
                    print("SAMPLE VALUES:")
                    for key in list(sample_point.keys())[:5]:  # Show first 5 keys
                        print(f"{key}: {sample_point[key]}")
                print("="*50)
            
            # Convert data points to database format with flexible parsing
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
                # Battery API returns sn: null; use devId (or dev_id) from response, or first valid dev_id from batch
                sn = data_point.get('sn') or data_point.get('devId') or data_point.get('dev_id')
                dev_id_str = str(sn).strip() if sn else ""
                # API may return devDn "NE=…" only; never store that as dev_id (breaks dbt numeric unpivot joins)
                if dev_id_str.startswith('NE='):
                    dev_id_str = ""
                if not dev_id_str or dev_id_str.lower() in ('none', 'null'):
                    # Fallback: use first valid dev_id from batch (e.g. battery with sn=null in API; batch may have 2 rows, one invalid)
                    for d in device_batch:
                        bid = d.get('dev_id')
                        if bid is not None:
                            bs = str(bid).strip()
                            if bs and bs.lower() not in ('none', 'null'):
                                dev_id_str = bs
                                break
                    if not dev_id_str or dev_id_str.lower() in ('none', 'null'):
                        logging.warning("Data point has invalid dev_id, skipping")
                        continue
                
                # Find device details for grid_connection_date check (match dev_id; API may return devId, DB may have int or str)
                current_device_details = next((d for d in device_batch if str(d.get('dev_id')) == dev_id_str), None)
                
                # Skip data before plant grid_connection_date (except battery: GCD is plant-level, battery may be added later)
                if current_device_details and current_device_details.get('dev_type_id') != 39:
                    grid_connection_date_dt = current_device_details.get('grid_connection_date')
                    if grid_connection_date_dt and collect_time < grid_connection_date_dt:
                        continue  # Skip this data point if before grid connection
                
                # OpenAPI responses use dataItems; older paths used dataItemMap
                if 'dataItemMap' in data_point and isinstance(data_point['dataItemMap'], dict):
                    measurement_data = data_point['dataItemMap']
                    logging.debug(f"Using dataItemMap format with {len(measurement_data)} measurements")
                elif 'dataItems' in data_point and isinstance(data_point['dataItems'], dict):
                    measurement_data = data_point['dataItems']
                    logging.debug(f"Using dataItems format with {len(measurement_data)} measurements")
                else:
                    # Legacy flat format — exclude identifiers so dbt unpivot does not cast devDn to numeric
                    skip_keys = {
                        timestamp_field, 'devId', 'sn', 'dev_id', 'devDn',
                        'dataItems', 'dataItemMap',
                    }
                    measurement_data = {}
                    for key, value in data_point.items():
                        if key not in skip_keys:
                            measurement_data[key] = value
                
                db_records.append({
                    "dev_id": dev_id_str,
                    "collect_time": collect_time.isoformat(),
                    "measurement_data": json.dumps(measurement_data)
                })
            
            # Insert records into the database - AFTER collecting all records (sama seperti no_limit version)
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
                            INSERT INTO raw.fusionsolar_historical_data ({', '.join(columns)})
                            VALUES ({', '.join([':' + col for col in columns])})
                            ON CONFLICT (dev_id, collect_time) DO UPDATE SET
                            {update_clause}
                        """)
                        
                        session.execute(query, record)
                    
                    session.commit()
                    total_data_points_ingested += len(db_records)
                    logging.info(f"Successfully upserted {len(db_records)} data points.")
                    print(f"DB: Upserted {len(db_records)} rows into raw.fusionsolar_historical_data", flush=True)
                    # Verify write and show which DB we connected to (so you can match your SQL client)
                    try:
                        sample_dev_id = db_records[0]["dev_id"]
                        r = session.execute(text("SELECT COUNT(*) FROM raw.fusionsolar_historical_data WHERE dev_id = :dev_id"), {"dev_id": sample_dev_id})
                        cnt = r.scalar()
                        url = session.get_bind().url
                        print(f"DB Verify: dev_id {sample_dev_id} has {cnt} rows in raw.fusionsolar_historical_data", flush=True)
                        print(f"DB connection: host={url.host} port={url.port} database={url.database} (pastikan SQL client Anda connect ke sini)", flush=True)
                    except Exception as ve:
                        print(f"DB Verify warning: {ve}", flush=True)
                except SQLAlchemyError as e:
                    session.rollback()
                    logging.error(f"Database error during data upsert: {e}")
                    print(f"DB ERROR: {e}", flush=True)
                finally:
                    session.close()
            else:
                logging.info("No data to insert into database for this API data batch.")
                print("DB: No records to insert for this API batch (db_records empty).", flush=True)
            
            # Wait between API calls to avoid overloading (setelah response handling, sama seperti no_limit version)
            time.sleep(REQUEST_DELAY_SECONDS)
    
    return total_data_points_ingested

def update_device_counts_in_rate_limiter():
    """Updates the device counts in the rate limiter based on database."""
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot update device counts.")
        return
    
    try:
        session = Session()
        
        # Count devices by type
        query = """
            SELECT dev_type_id, COUNT(*) as count
            FROM raw.fusionsolar_devices
            GROUP BY dev_type_id
        """
        
        result = session.execute(text(query))
        
        # Map device type IDs to names and build counts dictionary
        device_counts = {}
        
        for row in result:
            dev_type_id = row[0]
            count = row[1]
            
            # Find the device type name for this ID
            device_type_name = None
            for name, type_id in DEVICE_TYPES.items():
                if type_id == dev_type_id:
                    device_type_name = name
                    break
            
            if device_type_name:
                device_counts[device_type_name] = count
        
        # Update the rate limiter
        rate_limiter.update_device_counts(device_counts)
        
        logging.info(f"Updated device counts in rate limiter: {device_counts}")
        
    except Exception as e:
        logging.error(f"Error updating device counts: {e}")
    finally:
        if 'session' in locals():
            session.close()

def fetch_historical_data(start_date_str, end_date_str, plant_codes_str=None, device_types_str=None):
    print("CASCADE_DEBUG: ENTERING fetch_historical_data FUNCTION", flush=True)
    """Fetches historical data for a given date range, optionally filtered by plant codes and device types."""
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot fetch historical data.")
        return 0
    
    try:
        # Parse date strings - interpret as local time (UTC+7) and convert to UTC
        # When user inputs "2025-11-23", they mean that date in local time (UTC+7)
        # So we need to convert: 2025-11-23 00:00:00 UTC+7 -> 2025-11-22 17:00:00 UTC
        local_tz = timezone(timedelta(hours=7))  # UTC+7 timezone
        start_date_local = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=local_tz)
        end_date_local = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=local_tz)
        # Convert to UTC for API calls
        start_date = start_date_local.astimezone(timezone.utc)
        end_date = end_date_local.astimezone(timezone.utc)
        
        if start_date > end_date:
            logging.error("Start date cannot be after end date.")
            return 0
        
        # Create a progress key for this fetch operation
        progress_key = rate_limiter.get_progress_tracker_key(
            start_date_str, 
            end_date_str, 
            {
                "plant_codes": plant_codes_str,
                "device_types": device_types_str
            }
        )
        
        # Check if we have an existing progress record
        progress = rate_limiter.get_fetch_progress(progress_key)
        if progress:
            # Resume from where we left off
            resume_date_str = progress.get('current_date')
            if resume_date_str:
                try:
                    # Parse resume date the same way as start_date (local time UTC+7 -> UTC)
                    local_tz = timezone(timedelta(hours=7))  # UTC+7 timezone
                    current_date = datetime.strptime(resume_date_str, '%Y-%m-%d').replace(tzinfo=local_tz).astimezone(timezone.utc)
                    if current_date < end_date:
                        logging.info(f"Resuming fetch from {resume_date_str} to {end_date_str}")
                    else:
                        logging.info(f"Previous fetch operation completed. Starting fresh.")
                        current_date = start_date
                except ValueError:
                    logging.warning(f"Invalid resume date: {resume_date_str}. Starting from the beginning.")
                    current_date = start_date
            else:
                current_date = start_date
        else:
            # Start from the beginning
            current_date = start_date
        
        # Build SQL filter conditions
        conditions = []
        query_params = {}
        
        if plant_codes_str:
            plant_codes = [code.strip() for code in plant_codes_str.split(',')]
            conditions.append("d.plant_code IN :plant_codes")
            query_params['plant_codes'] = tuple(plant_codes)
        
        if device_types_str:
            device_type_names = [dtype.strip() for dtype in device_types_str.split(',')]
            device_type_ids = []
            for name in device_type_names:
                if name in DEVICE_TYPES:
                    device_type_ids.append(DEVICE_TYPES[name])
            
            if device_type_ids:
                conditions.append("d.dev_type_id IN :device_type_ids")
                query_params['device_type_ids'] = tuple(device_type_ids)
        
        # Build the SQL WHERE clause
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Fetch devices from database
        session = Session()
        query = f"""
            SELECT d.dev_id, d.plant_code, d.dev_type_id, d.dev_name, d.dev_dn, p.grid_connection_date
            FROM raw.fusionsolar_devices d
            JOIN raw.fusionsolar_plants p ON d.plant_code = p.plant_code
            WHERE {where_clause}
        """
        
        result = session.execute(text(query), query_params)
        devices = [dict(row._mapping) for row in result]
        
        if not devices:
            logging.warning("No devices found matching the filter criteria.")
            return 0
        
        logging.info(f"Found {len(devices)} devices matching the filter criteria.")
        print(f"CASCADE_DEBUG: Inside fetch_historical_data, after finding {len(devices)} devices.", flush=True) # Unmissable print
        
        print("CASCADE_DEBUG: Inside fetch_historical_data, RIGHT BEFORE GRID_CONNECTION_DATE PARSING BLOCK.", flush=True) # Unmissable print
        # Ensure grid_connection_date is timezone-aware datetime object (UTC) and add logging
        logging.info(f"Processing {len(devices)} devices fetched from DB. Parsing and verifying grid_connection_dates...")
        parsed_gcd_count = 0
        failed_parse_gcd_count = 0
        none_gcd_from_db_count = 0 # Count of dates that were None/empty directly from DB

        for dev in devices:
            gcd_value = dev.get('grid_connection_date') # This is the raw value from the database query result
            original_gcd_type = type(gcd_value)
            # Log the raw value before any processing for this device
            logging.debug(f"Device ID: {dev.get('dev_id', 'N/A')}, Plant: {dev.get('plant_code', 'N/A')}, Raw DB grid_connection_date: '{gcd_value}', Type: {original_gcd_type}")

            parsed_successfully = False
            if gcd_value is None or (isinstance(gcd_value, str) and not gcd_value.strip()):
                logging.debug(f"  -> GCD for Dev ID {dev.get('dev_id', 'N/A')} was None or empty string from DB. Setting to None.")
                dev['grid_connection_date'] = None
                none_gcd_from_db_count +=1
                # No parsing attempt needed, already None or equivalent
            elif isinstance(gcd_value, str):
                gcd_str = gcd_value.strip()
                try:
                    # Attempt to parse ISO format (handles ' ' or 'T' separator and timezone offsets)
                    # Example: "2023-01-22 10:29:42.000 +0700"
                    dt_obj = datetime.fromisoformat(gcd_str.replace(" ", "T"))
                    dev['grid_connection_date'] = dt_obj.astimezone(timezone.utc)
                    logging.debug(f"  -> Parsed as ISO: {dev['grid_connection_date']} (UTC)")
                    parsed_successfully = True
                except ValueError as e_iso:
                    logging.debug(f"  -> Failed ISO parsing for '{gcd_str}': {e_iso}. Trying common strptime formats...")
                    # Try common formats, ensure to make them timezone-aware (assume UTC if naive)
                    common_formats = ['%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S']
                    for fmt in common_formats:
                        try:
                            dt_obj = datetime.strptime(gcd_str, fmt)
                            if dt_obj.tzinfo is None:
                                dt_obj = dt_obj.replace(tzinfo=timezone.utc) # Assume UTC if naive
                            else:
                                dt_obj = dt_obj.astimezone(timezone.utc) # Convert to UTC if timezone-aware
                            dev['grid_connection_date'] = dt_obj
                            logging.debug(f"  -> Parsed with strptime '{fmt}': {dev['grid_connection_date']} (UTC)")
                            parsed_successfully = True
                            break # Parsed successfully
                        except ValueError:
                            continue # Try next format
                    if not parsed_successfully:
                        logging.warning(f"  -> Could not parse grid_connection_date string '{gcd_str}' for device {dev.get('dev_id', 'N/A')} with any strptime method. Setting to None.")
                        dev['grid_connection_date'] = None
            
            elif isinstance(gcd_value, (datetime, date)): # If it's already a datetime/date object
                 logging.debug(f"  -> GCD for Dev ID {dev.get('dev_id', 'N/A')} is already a datetime/date object: {gcd_value}.")
                 dt_obj_temp = gcd_value
                 if isinstance(dt_obj_temp, date) and not isinstance(dt_obj_temp, datetime): # if it's a date, convert to datetime at midnight
                    dt_obj_temp = datetime.combine(dt_obj_temp, datetime.min.time())
                    logging.debug(f"    Converted date to datetime: {dt_obj_temp}")

                 if dt_obj_temp.tzinfo is None:
                    dev['grid_connection_date'] = dt_obj_temp.replace(tzinfo=timezone.utc) # Assume UTC if naive
                    logging.debug(f"    Made naive datetime UTC: {dev['grid_connection_date']}")
                 else:
                    dev['grid_connection_date'] = dt_obj_temp.astimezone(timezone.utc)
                    logging.debug(f"    Converted existing timezone to UTC: {dev['grid_connection_date']}")
                 parsed_successfully = True
            
            else: # Other unexpected type
                logging.warning(f"  -> Unexpected type for grid_connection_date '{gcd_value}' (Type: {original_gcd_type}) for device {dev.get('dev_id', 'N/A')}. Setting to None.")
                dev['grid_connection_date'] = None

            if parsed_successfully:
                parsed_gcd_count += 1
            elif dev['grid_connection_date'] is None and not (gcd_value is None or (isinstance(gcd_value, str) and not gcd_value.strip())):
                # This means it was not None/empty from DB, but parsing failed and resulted in None
                failed_parse_gcd_count += 1
        
        logging.info(f"Finished parsing Grid Connection Dates. Total devices processed: {len(devices)}.")
        logging.info(f"  GCDs initially None/Empty from DB: {none_gcd_from_db_count}")
        logging.info(f"  Successfully parsed/converted to datetime (UTC): {parsed_gcd_count}")
        logging.info(f"  Failed to parse non-empty values (now None): {failed_parse_gcd_count}")

        logging.debug("Device list before main batch loop (sample of first 5 and any with recently parsed None GCD):")
        temp_none_log_count = 0
        for i_log, dev_log in enumerate(devices):
            is_none_after_parse = dev_log.get('grid_connection_date') is None
            if i_log < 5 or (is_none_after_parse and temp_none_log_count < 5) :
                logging.debug(f"  Dev ID: {dev_log.get('dev_id', 'N/A')}, Plant: {dev_log.get('plant_code', 'N/A')}, Parsed GCD: {dev_log.get('grid_connection_date')}, Type: {type(dev_log.get('grid_connection_date'))}")
                if is_none_after_parse:
                    temp_none_log_count +=1
        
        # Process data in batches of MAX_DAYS_PER_REQUEST days
        total_points_ingested = 0
        
        logging.info(f"Starting main batch processing loop. Total devices to consider: {len(devices)}.")
        while current_date < end_date:
            batch_end_date = min(current_date + timedelta(days=MAX_DAYS_PER_REQUEST), end_date)
            
            current_date_str = current_date.strftime('%Y-%m-%d')
            batch_end_date_str = batch_end_date.strftime('%Y-%m-%d')
            
            logging.info(f"Processing data from {current_date_str} to {batch_end_date_str}")
            
            # Update progress tracker
            rate_limiter.update_fetch_progress(progress_key, current_date_str)
            
            # Fetch and store data
            # Filter devices for the current batch based on grid_connection_date.
            # Battery (dev_type_id 39) is always included (plant GCD may be after battery data).
            logging.debug(f"Current batch window for filtering: {current_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {batch_end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"CASCADE_DEBUG: Inside fetch_historical_data, RIGHT BEFORE CREATING 'devices_for_batch'. Current batch window: {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}.", flush=True)
            devices_for_batch = [
                dev for dev in devices
                if dev.get('dev_type_id') == 39  # battery: always eligible
                or dev.get('grid_connection_date') is None
                or dev['grid_connection_date'] <= batch_end_date
            ]
            logging.info(f"Master device list count: {len(devices)}. Devices eligible for this batch (ending {batch_end_date.strftime('%Y-%m-%d')}): {len(devices_for_batch)}.")
            if len(devices_for_batch) > 0 and len(devices_for_batch) < 10: # Log details if filtering happened and list is small
                 for dev_in_batch_log in devices_for_batch:
                     logging.debug(f"  -> Eligible Dev ID: {dev_in_batch_log.get('dev_id', 'N/A')}, Plant: {dev_in_batch_log.get('plant_code', 'N/A')}, GCD: {dev_in_batch_log.get('grid_connection_date')}")
            if not devices_for_batch:
                logging.info(f"No devices eligible for data fetching in batch from {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')} "
                             f"based on their grid connection dates relative to the batch end date.")
                points_ingested = 0
            else:
                # logging.info(f"Querying API for {len(devices_for_batch)} device(s) for batch {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}")
                points_ingested = fetch_and_store_device_data(devices_for_batch, current_date, batch_end_date)
                total_points_ingested += points_ingested
                
                logging.info(f"Processed {points_ingested} data points for {current_date_str} to {batch_end_date_str}")
            
            # Move to the next batch - Always increment current_date regardless of whether devices_for_batch is empty
            current_date = batch_end_date + timedelta(seconds=1)
        
        # Mark progress as complete by clearing it
        rate_limiter.clear_fetch_progress(progress_key)
        
        logging.info(f"Total data points ingested: {total_points_ingested}")
        return total_points_ingested
    
    except Exception as e:
        logging.error(f"Error fetching historical data: {e}")
        return 0
    finally:
        if 'session' in locals():
            session.close()

def fetch_yesterday_data():
    """Fetches yesterday's data for all devices."""
    # Use local time (UTC+7) to determine yesterday's date
    local_tz = timezone(timedelta(hours=7))  # UTC+7 timezone
    yesterday = datetime.now(local_tz) - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    
    logging.info(f"Fetching data for yesterday ({yesterday_str} local time)")
    return fetch_historical_data(yesterday_str, yesterday_str)
