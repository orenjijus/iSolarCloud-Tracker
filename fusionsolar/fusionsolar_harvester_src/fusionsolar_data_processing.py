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

# Initialize the rate limiter with test mode enabled for faster testing
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

    # Update device counts in rate limiter
    update_device_counts_in_rate_limiter()

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
        
        # Calculate API call quota for this device type
        daily_limit = rate_limiter.calculate_daily_limit(device_type_name)
        calls_made = rate_limiter.get_calls_today(device_type_name)
        calls_remaining = rate_limiter.get_remaining_calls(device_type_name)
        
        logging.info(f"API Rate Limit - {device_type_name}: Limit={daily_limit}, Used={calls_made}, Remaining={calls_remaining}")
        
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
            
            # Check if we can make the API call within rate limits
            if not rate_limiter.can_make_api_call(device_type_name):
                wait_time = min(3600, rate_limiter._get_seconds_until_tomorrow())
                logging.warning(f"Rate limit reached for {device_type_name}. Waiting up to {wait_time} seconds...")
                
                # Wait until we can make the call or max wait time is reached
                if not rate_limiter.wait_for_api_call(device_type_name, max_wait_time=wait_time):
                    logging.error(f"Exceeded maximum wait time for {device_type_name}. Skipping batch.")
                    continue
                
                logging.info(f"Resuming API calls for {device_type_name}")
            
            # Create API payload
            payload = {
                "devIds": ",".join(dev_ids),
                "devTypeId": device_type_id,
                "startTime": start_timestamp_ms,
                "endTime": end_timestamp_ms
            }
            
            # Make API call
            logging.info(f"Fetching historical data for {len(dev_ids)} devices of type {device_type_name}")
            api_response = _make_api_request("/thirdData/getDevHistoryKpi", payload)
            
            # Record the API call
            rate_limiter.record_api_call(device_type_name)
            
            # Wait between API calls to avoid overloading
            time.sleep(REQUEST_DELAY_SECONDS)
            
            if not api_response:
                logging.warning("Failed to fetch historical data")
                continue
            
            if 'data' in api_response and 'list' in api_response['data']:
                data_points = api_response['data']['list']
                
                if not data_points:
                    logging.info(f"No data points returned for the query period")
                    continue
                
                logging.info(f"Received {len(data_points)} data points from API")
                
                # Convert data points to database format
                db_records = []
                for data_point in data_points:
                    if 'collectTime' not in data_point:
                        logging.warning("Data point missing collectTime")
                        continue
                    
                    # FusionSolar uses millisecond timestamps
                    collect_time_ms = data_point.pop('collectTime')
                    collect_time = datetime.fromtimestamp(collect_time_ms / 1000.0, tz=timezone.utc)
                    
                    # For each device ID in the API call's batch, create a record if applicable
                    # dev_ids is the list of device ID strings used in the API payload for the current batch
                    for dev_id_str in dev_ids:
                        # Find the full device dictionary from device_batch to get its grid_connection_date
                        # device_batch is a list of dictionaries like: [{'dev_id': '123', ..., 'grid_connection_date': datetime_obj}, ...]
                        current_device_details = next((d for d in device_batch if d.get('dev_id') == dev_id_str), None)

                        if not current_device_details:
                            logging.warning(f"Device details not found in current API batch for dev_id: {dev_id_str}. Skipping record creation for this data point.")
                            continue

                        grid_connection_date_dt = current_device_details.get('grid_connection_date')
                        # Ensure grid_connection_date is timezone-aware datetime object (UTC)
                        if grid_connection_date_dt and collect_time < grid_connection_date_dt:
                            # logging.debug(f"Skipping data for device {dev_id_str} at {collect_time} as it's before grid connection {grid_connection_date_dt}")
                            continue  # Skip this data point for this specific device
                        
                        # Create a JSON object with all the measurements (collectTime was already popped from data_point)
                        measurement_data_for_db = {k: v for k, v in data_point.items()}
                        
                        db_records.append({
                            "dev_id": dev_id_str,
                            "collect_time": collect_time.isoformat(), # collect_time is the UTC datetime object for the data_point
                            "measurement_data": json.dumps(measurement_data_for_db)
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
                    except SQLAlchemyError as e:
                        session.rollback()
                        logging.error(f"Database error during data upsert: {e}")
                    finally:
                        session.close()
                else:
                    logging.info("No data to insert into database for this API data batch.")
            else:
                logging.warning("Unexpected response format from historical data API")
    
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
            FROM fusionsolar_devices
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
        # Parse date strings
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        
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
                    current_date = datetime.strptime(resume_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
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
            SELECT d.dev_id, d.plant_code, d.dev_type_id, d.dev_name, p.grid_connection_date
            FROM fusionsolar_devices d
            JOIN fusionsolar_plants p ON d.plant_code = p.plant_code
            WHERE {where_clause}
        """
        
        result = session.execute(text(query), query_params)
        devices = [dict(row._mapping) for row in result]
        
        if not devices:
            logging.warning("No devices found matching the filter criteria.")
            return 0
        
        logging.info(f"Found {len(devices)} devices matching the filter criteria.")
        print(f"CASCADE_DEBUG: Inside fetch_historical_data, after finding {len(devices)} devices. About to call update_device_counts_in_rate_limiter.", flush=True) # Unmissable print
        
        # Update device counts in rate limiter
        update_device_counts_in_rate_limiter()
        
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
            # Filter devices for the current batch based on grid_connection_date
            # A device is eligible if its grid_connection_date is on or before the end of the current batch window,
            # or if its grid_connection_date is not set (None).
            logging.debug(f"Current batch window for filtering: {current_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {batch_end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"CASCADE_DEBUG: Inside fetch_historical_data, RIGHT BEFORE CREATING 'devices_for_batch'. Current batch window: {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}.", flush=True)
            devices_for_batch = [
                dev for dev in devices
                if dev.get('grid_connection_date') is None or dev['grid_connection_date'] <= batch_end_date
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
            
            # Move to the next batch
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
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    
    logging.info(f"Fetching data for yesterday ({yesterday_str})")
    return fetch_historical_data(yesterday_str, yesterday_str)
