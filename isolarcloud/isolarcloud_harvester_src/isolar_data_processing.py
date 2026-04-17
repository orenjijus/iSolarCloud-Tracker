import logging
import time
import json
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .isolar_config import (MAX_PS_KEYS_PER_REQUEST, MAX_POINTS_PER_REQUEST, REQUEST_DELAY_SECONDS,
                         DEVICE_TYPE_MEASURING_POINTS, get_measuring_points_for_device_type, DAYS_PER_HISTORICAL_BATCH,
                         PARALLEL_PROCESSING_ENABLED, PARALLEL_MAX_WORKERS)
from .isolar_api_client import _make_api_request
from .isolar_db_operations import init_database

# Initialize our own engine and Session to ensure they're available in this module
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
            from .isolar_db_operations import engine as db_engine, Session as db_Session
            
            # Use the already initialized engine and Session
            engine = db_engine
            Session = db_Session
            
            if engine is None:
                logging.error("Engine not available from db_operations module")
                return False
                
            logging.info("Database engine initialized in data processing module")
            return True
        except Exception as e:
            logging.error(f"Failed to initialize database in data processing module: {e}")
            return False
    return True


def _map_device_type_name_for_points(device):
    """Helper to determine the standardized device type name for point lookup."""
    device_type_name_for_points = 'unknown'
    if 'type_name' in device:
        type_name_lower = device['type_name'].lower()
        # Prioritize specific keywords for mapping
        if "inverter" in type_name_lower or "逆变器" in type_name_lower: # Chinese for inverter
            device_type_name_for_points = "inverter"
        elif "meteo_station" in type_name_lower or "meteo" in type_name_lower or "气象站" in type_name_lower: # Chinese for weather station
            device_type_name_for_points = "meteo_station"
        elif "meter" in type_name_lower or "电表" in type_name_lower: # Chinese for meter
            device_type_name_for_points = "meter"
        else: # Fallback if no keywords match, try to use type_name directly if it's in DEVICE_TYPE_MEASURING_POINTS
            if type_name_lower in DEVICE_TYPE_MEASURING_POINTS:
                 device_type_name_for_points = type_name_lower
            else:
                logging.debug(f"type_name '{device['type_name']}' not directly in DEVICE_TYPE_MEASURING_POINTS, trying API type code.")

    if device_type_name_for_points == 'unknown' and 'device_type' in device:
        api_type_code = device.get('device_type')
        for name, config_data in DEVICE_TYPE_MEASURING_POINTS.items():
            if config_data.get('device_type') == api_type_code:
                device_type_name_for_points = name
                break
    return device_type_name_for_points

def fetch_and_store_minute_data(devices_to_fetch, start_time_dt, end_time_dt, minute_interval=5):
    """Fetches minute-level data and stores it in PostgreSQL."""
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot store minute data.")
        return 0
    
    if not devices_to_fetch:
        logging.info("No devices provided to fetch_and_store_minute_data.")
        return 0

    grouped_by_ps_and_type = {}
    for device in devices_to_fetch:
        ps_id = device.get('ps_id') 
        device_type_name = _map_device_type_name_for_points(device)
        
        if ps_id not in grouped_by_ps_and_type:
            grouped_by_ps_and_type[ps_id] = {}
        if device_type_name not in grouped_by_ps_and_type[ps_id]:
            grouped_by_ps_and_type[ps_id][device_type_name] = []
        # Store the full device object temporarily if needed, or just ps_key
        grouped_by_ps_and_type[ps_id][device_type_name].append(device.get('device_ps_key'))

    total_data_points_ingested = 0

    start_time_api_format = start_time_dt.strftime('%Y%m%d%H%M%S')
    end_time_api_format = end_time_dt.strftime('%Y%m%d%H%M%S')

    for ps_id, types_in_ps in grouped_by_ps_and_type.items():
        for device_type_name, ps_key_list_for_type in types_in_ps.items():
            if device_type_name == 'unknown':
                logging.warning(f"Skipping devices with unknown type for ps_id {ps_id}: {ps_key_list_for_type}")
                continue

            measuring_points_for_type = get_measuring_points_for_device_type(device_type_name)
            if not measuring_points_for_type:
                logging.warning(f"Skipping {device_type_name} for ps_id {ps_id} as no measuring points defined.")
                continue
            
            # The /openapi/getDevicePointMinuteDataList endpoint uses ps_key_list and points, 
            # not device_type for filtering, so api_device_type_code is not used here.

            # Batch ps_keys and measuring_points according to API limits
            for i in range(0, len(ps_key_list_for_type), MAX_PS_KEYS_PER_REQUEST):
                batched_ps_keys = ps_key_list_for_type[i:i + MAX_PS_KEYS_PER_REQUEST]
                
                for j in range(0, len(measuring_points_for_type), MAX_POINTS_PER_REQUEST):
                    batched_points_str_list = measuring_points_for_type[j:j + MAX_POINTS_PER_REQUEST]
                    
                    payload = {
                        "ps_key_list": batched_ps_keys,
                        "points": ",".join(batched_points_str_list), # API expects a comma-separated string
                        "start_time_stamp": start_time_api_format,
                        "end_time_stamp": end_time_api_format,
                        "minute_interval": minute_interval,
                    }
                    
                    logging.info(f"Fetching minute data with payload: {payload}")
                    api_response_parsed = _make_api_request("/openapi/getDevicePointMinuteDataList", payload)
                    time.sleep(REQUEST_DELAY_SECONDS)

                    if api_response_parsed and api_response_parsed.get("result_code") == "1":
                        result_data = api_response_parsed.get("result_data", {})
                        data_to_insert = []
                        for device_api_ps_key, point_data_records in result_data.items():
                            if not isinstance(point_data_records, list):
                                logging.warning(f"Expected a list of records for ps_key {device_api_ps_key}, got {type(point_data_records)}. Skipping.")
                                continue
                            
                            for point_data_item in point_data_records:
                                timestamp_api_str = point_data_item.get("time_stamp")
                                if not timestamp_api_str or not device_api_ps_key: # device_api_ps_key is from the outer loop
                                    logging.warning(f"Missing time_stamp or ps_key in record for {device_api_ps_key}: {point_data_item}")
                                    continue

                                try:
                                    # API timestamp is YYYYMMDDHHMMSS
                                    naive_dt = datetime.strptime(timestamp_api_str, '%Y%m%d%H%M%S')
                                    # TODO: Confirm timezone of API's time_stamp. Assuming it's local to powerhouse.
                                    # For now, store as naive datetime converted to ISO string.
                                    # Proper UTC conversion would require knowing the powerhouse's timezone.
                                    # Example: local_tz.localize(naive_dt).astimezone(timezone.utc).isoformat()
                                    converted_utc_timestamp = naive_dt.isoformat() 

                                except ValueError as ve:
                                    logging.error(f"Error parsing time_stamp '{timestamp_api_str}' for ps_key {device_api_ps_key}: {ve}. Skipping record.")
                                    continue
                                
                                # Extract measurement data (all keys except time_stamp) into a separate dict for JSONB column
                                measurement_data = {}
                                for key, value in point_data_item.items():
                                    if key.lower() != "time_stamp": # Exclude the original time_stamp
                                        measurement_data[key] = value
                                
                                # Only use the required columns in row_data, with measurement_data as JSONB
                                # Convert measurement_data dictionary to JSON string for proper PostgreSQL JSONB storage
                                row_data = {
                                    "device_ps_key": device_api_ps_key, # Use the key from the API response
                                    "timestamp": converted_utc_timestamp,
                                    "measurement_data": json.dumps(measurement_data)  # Serialize to JSON string
                                }
                                
                                data_to_insert.append(row_data)
                        
                        if data_to_insert:
                            try:
                                session = Session()
                                for row in data_to_insert:
                                    # Build the column list and values for the INSERT
                                    columns = list(row.keys())
                                    values = [row[col] for col in columns]
                                    
                                    # Build the ON CONFLICT DO UPDATE clause
                                    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in ['device_ps_key', 'timestamp']])
                                    
                                    # Construct and execute the UPSERT query
                                    query = text(f"""
                                        INSERT INTO raw.isolarcloud_historical_data ({', '.join(columns)})
                                        VALUES ({', '.join([':' + col for col in columns])})
                                        ON CONFLICT (device_ps_key, timestamp) DO UPDATE SET
                                        {update_clause}
                                    """)
                                    
                                    session.execute(query, row)
                                
                                session.commit()
                                total_data_points_ingested += len(data_to_insert)
                                logging.info(f"Successfully upserted {len(data_to_insert)} data points.")
                            except SQLAlchemyError as e:
                                session.rollback()
                                logging.error(f"Database error during data upsert: {e}")
                            finally:
                                session.close()
                        else:
                            logging.info("No data to insert into database for this API data batch.")
                            
                    elif api_response_parsed is None: # Error already logged by _make_api_request
                        pass 
                    else: # This covers api_response_parsed.get("result_code") != "1"
                        logging.warning(f"API request failed or returned unexpected data: {api_response_parsed}")

    return total_data_points_ingested

def fetch_historical_data_for_batch(devices_batch, day_dt_start, day_dt_end, minute_interval, parallel=None, max_workers=None):
    """
    Processes a batch of devices for a given day, fetching data in 3-hour intervals.
    
    Args:
        devices_batch: List of devices to process
        day_dt_start: Start datetime for the day
        day_dt_end: End datetime for the day
        minute_interval: Interval in minutes between data points
        parallel: Whether to use parallel processing (default: from config)
        max_workers: Number of parallel workers (default: from config)
    """
    logging.info(f"Processing day-batch: {day_dt_start.strftime('%Y-%m-%d')} for {len(devices_batch)} devices.")
    
    # Use config defaults if not specified
    if parallel is None:
        parallel = PARALLEL_PROCESSING_ENABLED
    if max_workers is None:
        max_workers = PARALLEL_MAX_WORKERS
    
    # Build list of 3-hour intervals
    intervals = []
    current_interval_start = day_dt_start

    while current_interval_start < day_dt_end:
        # Calculate end of the current 3-hour interval (e.g., start 00:00:00 -> end 02:59:59)
        current_interval_end = min(current_interval_start + timedelta(hours=3), day_dt_end)
        
        # Ensure the interval is valid, especially for the last partial hour
        if current_interval_end < current_interval_start:
            # This might happen if day_dt_end was exactly on an hour boundary before subtraction, adjust to process the last second.
             current_interval_end = current_interval_start 

        intervals.append((current_interval_start, current_interval_end))

        # Move to the start of the next 3-hour interval
        current_interval_start += timedelta(hours=3)
    
    total_points_ingested_for_day_batch = 0
    
    if parallel and len(intervals) > 1:
        # Parallel processing: process all intervals concurrently
        logging.info(f"Processing {len(intervals)} intervals in parallel with {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all interval tasks
            future_to_interval = {
                executor.submit(
                    fetch_and_store_minute_data,
                    devices_batch,
                    interval_start,
                    interval_end,
                    minute_interval
                ): (interval_start, interval_end)
                for interval_start, interval_end in intervals
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_interval):
                interval_start, interval_end = future_to_interval[future]
                try:
                    points_ingested_for_interval = future.result()
                    if points_ingested_for_interval:
                        total_points_ingested_for_day_batch += points_ingested_for_interval
                    logging.info(f"Completed 3-hour interval: {interval_start.strftime('%Y-%m-%d %H:%M:%S')} to {interval_end.strftime('%Y-%m-%d %H:%M:%S')} - {points_ingested_for_interval} points")
                except Exception as e:
                    logging.error(f"Error processing interval {interval_start.strftime('%Y-%m-%d %H:%M:%S')} to {interval_end.strftime('%Y-%m-%d %H:%M:%S')}: {str(e)}")
                    # Continue processing other intervals even if one fails
    else:
        # Sequential processing: original behavior
        logging.info(f"Processing {len(intervals)} intervals sequentially")
        for interval_start, interval_end in intervals:
            logging.info(f"Fetching data for 3-hour interval: {interval_start.strftime('%Y-%m-%d %H:%M:%S')} to {interval_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
            points_ingested_for_interval = fetch_and_store_minute_data(
                devices_batch,
                interval_start,
                interval_end,
                minute_interval
            )
            if points_ingested_for_interval:
                total_points_ingested_for_day_batch += points_ingested_for_interval
        
    logging.info(f"Total data points ingested for day-batch ({day_dt_start.strftime('%Y-%m-%d')}): {total_points_ingested_for_day_batch}")
    return total_points_ingested_for_day_batch

def fetch_historical_data(start_date_str, end_date_str, ps_ids_str=None, device_types_str=None, parallel_override=None, max_workers_override=None):
    """Fetches historical data for a given date range, optionally filtered by power station IDs and device types.

    parallel_override: True = always use parallel; False = always sequential; None = use parallel only if range <= 2 days (default).
    max_workers_override: If set, use this many workers; otherwise use PARALLEL_MAX_WORKERS from config.
    """
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot fetch historical data.")
        return
    
    print(f"Starting historical data fetch for {start_date_str} to {end_date_str}")
    print(f"Database engine: {engine}")
    # Continue with existing function...

    try:
        start_time_dt = datetime.strptime(start_date_str, '%Y-%m-%d').replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
        end_time_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    except ValueError:
        logging.error("Invalid date format. Please use YYYY-MM-DD.")
        return

    if end_time_dt <= start_time_dt:
        logging.error("End date must be after start date.")
        return
    
    logging.info(f"Preparing to fetch historical data from {start_time_dt.strftime('%Y-%m-%d')} to {end_time_dt.strftime('%Y-%m-%d')}")

    # Long date range (>2 days): use sequential to stay under API rate limit 2000/h. Daily (1 day) keeps parallel.
    # Can override via parallel_override=True (e.g. from CLI --force-parallel) and max_workers_override (e.g. --workers 8).
    range_days = (end_time_dt - start_time_dt).days
    if parallel_override is True:
        parallel_arg = True
        logging.info("Parallel processing forced (e.g. --force-parallel). Workers: %s", max_workers_override or PARALLEL_MAX_WORKERS)
    elif parallel_override is False:
        parallel_arg = False
        logging.info("Parallel processing disabled (forced sequential).")
    else:
        parallel_arg = None if range_days <= 2 else False
        if parallel_arg is False:
            logging.info("Date range > 2 days: using sequential processing to respect API rate limit (2000/h). Use --force-parallel to override.")
    max_workers_arg = max_workers_override  # None = use config in fetch_historical_data_for_batch

    try:
        session = Session()
        # Build the base query to include install_date from power stations table
        base_query_str = """
            SELECT d.ps_id, d.device_ps_key, d.device_type, d.type_name, ps.install_date 
            FROM raw.isolarcloud_devices d
            JOIN raw.isolarcloud_power_stations ps ON d.ps_id = ps.ps_id
        """
        params = {}
        where_clauses = []

        if ps_ids_str:
            ps_id_list = [pid.strip() for pid in ps_ids_str.split(',') if pid.strip()]
            if ps_id_list:
                where_clauses.append("d.ps_id = ANY(:ps_ids)")
                params['ps_ids'] = ps_id_list
        
        if where_clauses:
            query_str = base_query_str + " WHERE " + " AND ".join(where_clauses)
        else:
            query_str = base_query_str
        
        query = text(query_str)
        
        result = session.execute(query, params)
        # Print column names to debug the query result structure
        columns = result.keys()
        print(f"Query returned columns: {columns}")
        
        # Convert each row to a dictionary with proper column names
        devices_to_process = []
        for row in result:
            device_dict = {}
            for idx, column in enumerate(columns):
                device_dict[column] = row[idx]
            devices_to_process.append(device_dict)
            
        # Print the first device for debugging with safe encoding handling
        if devices_to_process:
            # Print only safe fields to avoid encoding issues
            first_device = devices_to_process[0]
            print(f"First device - ID: {first_device.get('device_ps_key')}, Type ID: {first_device.get('device_type')}")
            print(f"Found {len(devices_to_process)} devices to process")
        else:
            print("No devices found")

        # Parse install_date for each device
        logging.info(f"Parsing install_date for {len(devices_to_process)} devices...")
        parsed_install_date_count = 0
        failed_install_date_parse_count = 0
        none_install_date_count = 0

        for device in devices_to_process:
            install_date_raw = device.get('install_date')
            device['parsed_install_date'] = None # Initialize
            if install_date_raw:
                try:
                    # Assuming install_date is a date string like 'YYYY-MM-DD'
                    # If it's a datetime string, adjust format e.g., '%Y-%m-%d %H:%M:%S'
                    if isinstance(install_date_raw, datetime):
                        parsed_date = install_date_raw # Already a datetime object
                    else:
                        parsed_date = datetime.strptime(str(install_date_raw).split(' ')[0], '%Y-%m-%d')
                    
                    device['parsed_install_date'] = parsed_date.replace(tzinfo=timezone.utc) # Make timezone-aware UTC
                    parsed_install_date_count += 1
                    logging.debug(f"Device PS Key: {device.get('device_ps_key')}, Raw install_date: {install_date_raw}, Parsed: {device['parsed_install_date']}")
                except ValueError as e:
                    logging.warning(f"Device PS Key: {device.get('device_ps_key')}, Failed to parse install_date '{install_date_raw}': {e}. Device will be included in all batches.")
                    failed_install_date_parse_count += 1
            else:
                logging.debug(f"Device PS Key: {device.get('device_ps_key')} has no install_date. Device will be included in all batches.")
                none_install_date_count += 1
        logging.info(f"Finished parsing install_dates. Parsed: {parsed_install_date_count}, Failed: {failed_install_date_parse_count}, None from DB: {none_install_date_count}")

        if not devices_to_process:
            logging.warning("No devices found in database matching ps_id criteria (or no ps_ids specified and no devices exist).")
            return

        if device_types_str:
            logging.info(f"Filtering for device types: {device_types_str}")
            filter_types_input = [dt.strip().lower() for dt in device_types_str.split(',')]
            
            filtered_devices_for_type = []
            for device in devices_to_process:
                device_type_name = _map_device_type_name_for_points(device)
                if device_type_name in filter_types_input:
                    filtered_devices_for_type.append(device)
            
            devices_to_process = filtered_devices_for_type
            logging.info(f"Filtered to {len(devices_to_process)} devices matching specified types.")
        
        # Process inverter summary when "inverter" is in filter (normal daily + backfill)
        process_inverter_summary = 'inverter' in filter_types_input if device_types_str else True

        # Process in batches of days
        current_date = start_time_dt # Already UTC from start_time_dt
        total_points_ingested = 0

        # Create a modified version of _map_device_type_name_for_points that returns 'inverter_summary' for inverters
        def _map_device_type_name_for_summary_points(device):
            # Directly implement device type name mapping without recursive calls
            device_type_name = 'unknown'
            if 'type_name' in device:
                type_name_lower = device['type_name'].lower()
                # Prioritize specific keywords for mapping
                if "inverter" in type_name_lower or "逆变器" in type_name_lower:
                    return "inverter_summary"  # Return inverter_summary directly
                elif "meteo_station" in type_name_lower or "meteo" in type_name_lower or "气象站" in type_name_lower:
                    device_type_name = "meteo_station"
                elif "meter" in type_name_lower or "电表" in type_name_lower:
                    device_type_name = "meter"
                else: # Fallback if no keywords match, try to use type_name directly if it's in DEVICE_TYPE_MEASURING_POINTS
                    if type_name_lower in DEVICE_TYPE_MEASURING_POINTS:
                         device_type_name = type_name_lower
            
            if device_type_name == 'unknown' and 'device_type' in device:
                api_type_code = device.get('device_type')
                for name, config_data in DEVICE_TYPE_MEASURING_POINTS.items():
                    if config_data.get('device_type') == api_type_code:
                        device_type_name = name
                        break
                        
            # Convert inverter to inverter_summary if needed
            if device_type_name == 'inverter':
                return 'inverter_summary'
            return device_type_name
            
        while current_date <= end_time_dt:
            batch_end_date = min(current_date + timedelta(days=DAYS_PER_HISTORICAL_BATCH), end_time_dt)
            # Ensure batch_end_date is also UTC and correctly set to end of day
            batch_end_date = batch_end_date.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)
            
            logging.info(f"Processing batch from {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}")
            
            # Filter devices for the current batch based on their install_date
            devices_for_current_batch = [
                dev for dev in devices_to_process
                if dev.get('parsed_install_date') is None or dev['parsed_install_date'] <= batch_end_date
            ]
            logging.info(f"Batch {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}: Total devices considered: {len(devices_to_process)}, Eligible by install_date: {len(devices_for_current_batch)}")

            points_ingested = 0
            if devices_for_current_batch:
                points_ingested = fetch_historical_data_for_batch(
                    devices_for_current_batch,
                    current_date,
                    batch_end_date,
                    5,  # minute_interval
                    parallel=parallel_arg,
                    max_workers=max_workers_arg,
                )
            else:
                logging.info("No devices eligible for this batch based on install_date.")
            
            total_points_ingested += points_ingested
            
            # Process inverter summary points if needed
            if process_inverter_summary:
                # Find inverter devices
                inverter_devices = []
                for device in devices_to_process:
                    if _map_device_type_name_for_points(device) == 'inverter':
                        inverter_devices.append(device)
                
                if inverter_devices:
                    # Filter inverter_devices for the current batch based on their install_date
                    inverter_devices_for_batch = [
                        dev for dev in inverter_devices
                        if dev.get('parsed_install_date') is None or dev['parsed_install_date'] <= batch_end_date
                    ]
                    logging.info(f"Processing inverter summary data for {len(inverter_devices_for_batch)} eligible inverters (out of {len(inverter_devices)} total inverters for this type filter)")
                    
                    summary_points_ingested = 0
                    if inverter_devices_for_batch:
                        # Process the summary points without modifying global functions
                        # Use a custom processing function that uses the summary mapping function
                        summary_points_ingested = _fetch_historical_data_for_batch_with_custom_mapping(
                            inverter_devices_for_batch,
                            current_date,
                            batch_end_date,
                            5,  # minute_interval
                            _map_device_type_name_for_summary_points,
                            parallel=parallel_arg,
                            max_workers=max_workers_arg,
                        )
                    else:
                        logging.info("No inverter devices eligible for summary data in this batch based on install_date.")
                    
                    total_points_ingested += summary_points_ingested
                    logging.info(f"Ingested {summary_points_ingested} inverter summary data points")
            
            current_date = batch_end_date + timedelta(days=1)
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc) # Ensure next current_date is also UTC

        logging.info(f"Historical data fetch complete. Total points ingested: {total_points_ingested}")
    except SQLAlchemyError as e:
        logging.error(f"Database error during historical data fetch: {e}")
    finally:
        session.close()

def _fetch_historical_data_for_batch_with_custom_mapping(devices_batch, day_dt_start, day_dt_end, minute_interval, custom_mapping_function, parallel=None, max_workers=None):
    """Processes a batch of devices using a custom mapping function instead of the global one.
    
    This avoids the need to temporarily modify global functions and prevents recursion errors.
    
    Args:
        devices_batch: List of devices to process
        day_dt_start: Start datetime for the day
        day_dt_end: End datetime for the day
        minute_interval: Interval in minutes between data points
        custom_mapping_function: Custom mapping function to use
        parallel: Whether to use parallel processing (default: from config)
        max_workers: Number of parallel workers (default: from config)
    """
    logging.info(f"Processing day-batch with custom mapping: {day_dt_start.strftime('%Y-%m-%d')} for {len(devices_batch)} devices.")
    
    # Use config defaults if not specified
    if parallel is None:
        parallel = PARALLEL_PROCESSING_ENABLED
    if max_workers is None:
        max_workers = PARALLEL_MAX_WORKERS
    
    # Build list of 3-hour intervals
    intervals = []
    current_interval_start = day_dt_start

    while current_interval_start < day_dt_end:
        # Calculate end of the current 3-hour interval
        current_interval_end = min(current_interval_start + timedelta(hours=3) - timedelta(seconds=1), day_dt_end)
        
        # Ensure the interval is valid
        if current_interval_end < current_interval_start:
            current_interval_end = current_interval_start 

        intervals.append((current_interval_start, current_interval_end))
        current_interval_start = current_interval_end + timedelta(seconds=1)
    
    total_points_ingested_for_day_batch = 0

    # Create a specialized function for this request that uses the custom mapping
    def specialized_fetch_and_store_minute_data(devices_to_fetch, start_time_dt, end_time_dt, minute_interval=5):
            """Modified version of fetch_and_store_minute_data that uses the custom mapping function"""
            if not ensure_db_initialized():
                logging.error("Database engine not initialized. Cannot store minute data.")
                return 0
            
            if not devices_to_fetch:
                logging.info("No devices provided to specialized_fetch.")
                return 0

            grouped_by_ps_and_type = {}
            for device in devices_to_fetch:
                ps_id = device.get('ps_id') 
                # Use the custom mapping function here
                device_type_name = custom_mapping_function(device)
                
                if ps_id not in grouped_by_ps_and_type:
                    grouped_by_ps_and_type[ps_id] = {}
                if device_type_name not in grouped_by_ps_and_type[ps_id]:
                    grouped_by_ps_and_type[ps_id][device_type_name] = []
                grouped_by_ps_and_type[ps_id][device_type_name].append(device.get('device_ps_key'))
            
            # The rest is identical to the original function
            total_data_points_ingested = 0

            start_time_api_format = start_time_dt.strftime('%Y%m%d%H%M%S')
            end_time_api_format = end_time_dt.strftime('%Y%m%d%H%M%S')

            for ps_id, types_in_ps in grouped_by_ps_and_type.items():
                for device_type_name, ps_key_list_for_type in types_in_ps.items():
                    if device_type_name == 'unknown':
                        logging.warning(f"Skipping devices with unknown type for ps_id {ps_id}: {ps_key_list_for_type}")
                        continue

                    measuring_points_for_type = get_measuring_points_for_device_type(device_type_name)
                    if not measuring_points_for_type:
                        logging.warning(f"Skipping {device_type_name} for ps_id {ps_id} as no measuring points defined.")
                        continue
                    
                    # Process in batches according to API limits
                    for i in range(0, len(ps_key_list_for_type), MAX_PS_KEYS_PER_REQUEST):
                        batched_ps_keys = ps_key_list_for_type[i:i + MAX_PS_KEYS_PER_REQUEST]
                        
                        for j in range(0, len(measuring_points_for_type), MAX_POINTS_PER_REQUEST):
                            batched_points = measuring_points_for_type[j:j + MAX_POINTS_PER_REQUEST]
                            
                            payload = {
                                "ps_key_list": batched_ps_keys,
                                "points": ",".join(batched_points),
                                "start_time_stamp": start_time_api_format,
                                "end_time_stamp": end_time_api_format,
                                "minute_interval": minute_interval,
                            }
                            
                            logging.info(f"Fetching minute data with payload: {payload}")
                            api_response_parsed = _make_api_request("/openapi/getDevicePointMinuteDataList", payload)
                            time.sleep(REQUEST_DELAY_SECONDS)

                            if api_response_parsed and api_response_parsed.get("result_code") == "1":
                                result_data = api_response_parsed.get("result_data", {})
                                data_to_insert = []
                                for device_api_ps_key, point_data_records in result_data.items():
                                    if not isinstance(point_data_records, list):
                                        logging.warning(f"Expected a list of records for ps_key {device_api_ps_key}, got {type(point_data_records)}. Skipping.")
                                        continue
                                    
                                    for point_data_item in point_data_records:
                                        timestamp_api_str = point_data_item.get("time_stamp")
                                        if not timestamp_api_str or not device_api_ps_key:
                                            logging.warning(f"Missing time_stamp or ps_key in record for {device_api_ps_key}: {point_data_item}")
                                            continue

                                        try:
                                            naive_dt = datetime.strptime(timestamp_api_str, '%Y%m%d%H%M%S')
                                            converted_utc_timestamp = naive_dt.isoformat()
                                        except ValueError as ve:
                                            logging.error(f"Error parsing time_stamp '{timestamp_api_str}' for ps_key {device_api_ps_key}: {ve}. Skipping record.")
                                            continue
                                        
                                        # Process measurement data
                                        measurement_data = {}
                                        for key, value in point_data_item.items():
                                            if key.lower() != "time_stamp":
                                                measurement_data[key] = value
                                        
                                        row_data = {
                                            "device_ps_key": device_api_ps_key,
                                            "timestamp": converted_utc_timestamp,
                                            "measurement_data": json.dumps(measurement_data)
                                        }
                                        
                                        data_to_insert.append(row_data)
                                
                                # Store the data in the database
                                if data_to_insert:
                                    try:
                                        session = Session()
                                        for row in data_to_insert:
                                            columns = list(row.keys())
    
                                            query = text("""
                                                INSERT INTO raw.isolarcloud_historical_data (
                                                    device_ps_key,
                                                    timestamp,
                                                    measurement_data
                                                )
                                                VALUES (
                                                    :device_ps_key,
                                                    :timestamp,
                                                    CAST(:measurement_data AS jsonb)
                                                )
                                                ON CONFLICT (device_ps_key, timestamp) DO UPDATE
                                                SET
                                                    measurement_data = COALESCE(raw.isolarcloud_historical_data.measurement_data, '{}'::jsonb)
                                                                    || EXCLUDED.measurement_data;
                                            """)
                                            session.execute(query, row)
                                        
                                        session.commit()
                                        total_data_points_ingested += len(data_to_insert)
                                        logging.info(f"Successfully upserted {len(data_to_insert)} data points.")
                                    except SQLAlchemyError as e:
                                        session.rollback()
                                        logging.error(f"Database error during data upsert: {e}")
                                    finally:
                                        session.close()
                                else:
                                    logging.info("No data to insert into database for this API data batch.")
                                    
                            elif api_response_parsed is None:
                                pass
                            else:
                                logging.warning(f"API request failed or returned unexpected data: {api_response_parsed}")

            return total_data_points_ingested
        
    if parallel and len(intervals) > 1:
            # Parallel processing: process all intervals concurrently
            logging.info(f"Processing {len(intervals)} intervals in parallel with {max_workers} workers (custom mapping)")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all interval tasks
                future_to_interval = {
                    executor.submit(
                        specialized_fetch_and_store_minute_data,
                        devices_batch,
                        interval_start,
                        interval_end,
                        minute_interval
                    ): (interval_start, interval_end)
                    for interval_start, interval_end in intervals
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_interval):
                    interval_start, interval_end = future_to_interval[future]
                    try:
                        points_ingested_for_interval = future.result()
                        total_points_ingested_for_day_batch += points_ingested_for_interval
                        logging.info(f"Completed 3-hour interval (custom mapping): {interval_start.strftime('%Y-%m-%d %H:%M:%S')} to {interval_end.strftime('%Y-%m-%d %H:%M:%S')} - {points_ingested_for_interval} points")
                    except Exception as e:
                        logging.error(f"Error processing interval {interval_start.strftime('%Y-%m-%d %H:%M:%S')} to {interval_end.strftime('%Y-%m-%d %H:%M:%S')}: {str(e)}")
                        # Continue processing other intervals even if one fails
    else:
        # Sequential processing: original behavior
        logging.info(f"Processing {len(intervals)} intervals sequentially (custom mapping)")
        for interval_start, interval_end in intervals:
            logging.info(f"Fetching data with custom mapping for interval: {interval_start.strftime('%Y-%m-%d %H:%M:%S')} to {interval_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
        points_ingested_for_interval = specialized_fetch_and_store_minute_data(
            devices_batch,
                interval_start,
                interval_end,
            minute_interval
        )
        total_points_ingested_for_day_batch += points_ingested_for_interval

    return total_points_ingested_for_day_batch


def fetch_yesterday_data_for_all_devices():
    """Fetches yesterday's data for all devices."""
    if not ensure_db_initialized():
        logging.error("Database engine not initialized. Cannot fetch yesterday's data.")
        return

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    
    logging.info(f"Fetching yesterday's data: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    fetch_historical_data(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )

