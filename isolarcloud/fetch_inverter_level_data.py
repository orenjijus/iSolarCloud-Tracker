import logging
import sys
import json
import argparse
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

# Configure logging first
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Log to console
    ]
)

# Function that will always return 'inverter_summary' regardless of device
def inverter_summary_mapper(_device):
    """Always returns 'inverter_summary' to force using those specific points."""
    return 'inverter_summary'

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Fetch inverter summary data points from iSolarCloud API.')
    
    # Date range parameters
    date_group = parser.add_argument_group('Date Range Parameters')
    date_group.add_argument('--start-date', type=str, required=True,
                        help='Start date for fetching data (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    date_group.add_argument('--end-date', type=str, required=True,
                        help='End date for fetching data (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    
    # Filtering parameters
    filter_group = parser.add_argument_group('Filtering Parameters')
    filter_group.add_argument('--ps-id', type=str, default=None,
                        help='Power station ID to filter by (comma-separated for multiple, or leave empty for all)')
    filter_group.add_argument('--device-id', type=str, default=None,
                        help='Specific device ID to fetch data for (leave empty for all devices)')
    
    # Advanced options
    advanced_group = parser.add_argument_group('Advanced Options')
    advanced_group.add_argument('--minute-interval', type=int, default=5,
                        help='Interval in minutes between data points (default: 5)')
    advanced_group.add_argument('--batch-days', type=int, default=3,
                        help='Number of days to process in each batch (default: 3)')
    advanced_group.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    advanced_group.add_argument('--dry-run', action='store_true',
                        help='Show which data would be fetched without actually fetching it')
    
    return parser.parse_args()

def main():
    """Main function to ensure proper execution order"""
    print("Starting script for fetching ONLY inverter-level summary data")
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set up logging based on debug flag
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Debug logging enabled")
    
    # Extract parameters from command-line arguments
    start_date = args.start_date
    end_date = args.end_date
    ps_id = args.ps_id
    specific_device_id = args.device_id
    minute_interval = args.minute_interval
    batch_days = args.batch_days
    dry_run = args.dry_run
    
    print(f"Parameters:")
    print(f"  Start Date: {start_date}")
    print(f"  End Date: {end_date}")
    print(f"  Power Station ID: {ps_id if ps_id else 'All power stations'}")
    print(f"  Device ID: {specific_device_id if specific_device_id else 'All devices'}")
    print(f"  Minute Interval: {minute_interval}")
    print(f"  Batch Days: {batch_days}")
    print(f"  Dry Run: {'Yes' if dry_run else 'No'}")
    
    if dry_run:
        print("DRY RUN MODE: Will show what would be fetched without actually fetching data")
        print("Example command to run this for real:")
        print(f"python fetch_inverter_level_data.py --start-date \"{start_date}\" --end-date \"{end_date}\"" + 
              (f" --ps-id \"{ps_id}\"" if ps_id else "") + 
              (f" --device-id \"{specific_device_id}\"" if specific_device_id else "") + 
              (f" --minute-interval {minute_interval}" if minute_interval != 5 else "") + 
              (f" --batch-days {batch_days}" if batch_days != 3 else ""))
        return 0
    
    # Step 1: Initialize the database
    from isolarcloud_harvester_src.isolar_db_operations import init_database
    print("Initializing database...")
    if not init_database():
        print("Failed to initialize database")
        return 1
    print("Database initialized successfully")
    
    # Step 2: Import the global engine from isolar_db_operations
    from isolarcloud_harvester_src.isolar_db_operations import engine
    
    # Step 3: Log in to the API
    from isolarcloud_harvester_src.isolar_api_client import login_isolarcloud
    print("Logging in to iSolarCloud...")
    if not login_isolarcloud():
        print("Failed to log in to iSolarCloud API")
        return 1
    print("Login successful")
    
    # Import other required modules
    import isolarcloud_harvester_src.isolar_data_processing as data_processing
    
    # Now run the function to fetch inverter summary data
    fetch_only_inverter_summary_data(start_date, end_date, ps_id, specific_device_id, engine, 
                                    data_processing, data_processing.fetch_historical_data_for_batch,
                                    batch_days, minute_interval)
    
    print("Inverter summary data fetch completed")
    return 0

def custom_fetch_and_store_minute_data(devices_to_fetch, start_time_dt, end_time_dt, minute_interval=5):
    """Custom version of fetch_and_store_minute_data that properly merges measurement data instead of replacing it."""
    import time
    import json
    from sqlalchemy.exc import SQLAlchemyError
    from isolarcloud_harvester_src.isolar_api_client import _make_api_request
    from isolarcloud_harvester_src.isolar_config import MAX_PS_KEYS_PER_REQUEST, MAX_POINTS_PER_REQUEST, REQUEST_DELAY_SECONDS
    from isolarcloud_harvester_src.isolar_config import get_measuring_points_for_device_type
    from isolarcloud_harvester_src.isolar_db_operations import Session
    
    if not devices_to_fetch:
        logging.info("No devices provided to custom_fetch_and_store_minute_data.")
        return 0
    
    # Ensure all devices use the inverter_summary device type
    for device in devices_to_fetch:
        device['device_type_name'] = 'inverter_summary'
    
    grouped_by_ps_and_type = {}
    for device in devices_to_fetch:
        ps_id = device.get('ps_id')
        if not ps_id:
            continue
            
        if ps_id not in grouped_by_ps_and_type:
            grouped_by_ps_and_type[ps_id] = {}
            
        # Always use 'inverter_summary' as the device type
        device_type = 'inverter_summary'
        if device_type not in grouped_by_ps_and_type[ps_id]:
            grouped_by_ps_and_type[ps_id][device_type] = []
            
        device_ps_key = device.get('device_ps_key')
        if device_ps_key and device_ps_key not in grouped_by_ps_and_type[ps_id][device_type]:
            grouped_by_ps_and_type[ps_id][device_type].append(device_ps_key)

    total_data_points_ingested = 0

    start_time_api_format = start_time_dt.strftime('%Y%m%d%H%M%S')
    end_time_api_format = end_time_dt.strftime('%Y%m%d%H%M%S')

    for ps_id, types_in_ps in grouped_by_ps_and_type.items():
        for device_type_name, ps_key_list_for_type in types_in_ps.items():
            # These are the key inverter summary points we want to fetch
            measuring_points = ["p1", "p14", "p24", "p25"]  # Yield Today, Total DC Power, Total Active Power, Total Reactive Power
            
            # Batch ps_keys according to API limits
            for i in range(0, len(ps_key_list_for_type), MAX_PS_KEYS_PER_REQUEST):
                batched_ps_keys = ps_key_list_for_type[i:i + MAX_PS_KEYS_PER_REQUEST]
                
                payload = {
                    "ps_key_list": batched_ps_keys,
                    "points": ",".join(measuring_points),
                    "start_time_stamp": start_time_api_format,
                    "end_time_stamp": end_time_api_format,
                    "minute_interval": minute_interval,
                }
                
                logging.info(f"Fetching minute data with payload: {payload}")
                api_response_parsed = _make_api_request("/openapi/getDevicePointMinuteDataList", payload)
                time.sleep(REQUEST_DELAY_SECONDS)

                if api_response_parsed and api_response_parsed.get("result_code") == "1":
                    result_data = api_response_parsed.get("result_data", {})
                    
                    if result_data:
                        session = Session()
                        try:
                            for device_api_ps_key, point_data_records in result_data.items():
                                if not isinstance(point_data_records, list):
                                    continue
                                
                                for point_data_item in point_data_records:
                                    timestamp_api_str = point_data_item.get("time_stamp")
                                    if not timestamp_api_str or not device_api_ps_key:
                                        continue

                                    try:
                                        # API timestamp is YYYYMMDDHHMMSS
                                        naive_dt = datetime.strptime(timestamp_api_str, '%Y%m%d%H%M%S')
                                        converted_utc_timestamp = naive_dt.isoformat() 
                                    except ValueError as ve:
                                        logging.error(f"Error parsing time_stamp '{timestamp_api_str}'")
                                        continue
                                    
                                    # Extract measurement data (all keys except time_stamp)
                                    new_measurement_data = {}
                                    for key, value in point_data_item.items():
                                        if key.lower() != "time_stamp":
                                            new_measurement_data[key] = value
                                    
                                    # First, check if a record already exists for this device and timestamp
                                    check_query = text("""
                                        SELECT measurement_data FROM isolarcloud_historical_data 
                                        WHERE device_ps_key = :device_ps_key AND timestamp = :timestamp
                                    """)
                                    
                                    # Get the device info for this ps_key and ensure it uses the correct device type
                                    device_info = next((d for d in devices_to_fetch if d['device_ps_key'] == device_api_ps_key), None)
                                    if not device_info:
                                        logging.warning(f"No device info found for ps_key {device_api_ps_key}")
                                        continue
                                        
                                    # Create a copy of device info and ensure it uses the correct device type mapping
                                    device_info = dict(device_info)  # Create a copy to avoid modifying the original
                                    # Use the inverter_summary_mapper to ensure consistent device type naming
                                    device_info['device_type_name'] = inverter_summary_mapper(device_info)
                                    
                                    result = session.execute(check_query, {
                                        "device_ps_key": device_api_ps_key,
                                        "timestamp": converted_utc_timestamp
                                    })
                                    
                                    existing_record = result.fetchone()
                                    merged_data = {}
                                    
                                    if existing_record and existing_record[0]:
                                        # Record exists, merge the data
                                        try:
                                            # Handle different types of existing_record[0] data
                                            if isinstance(existing_record[0], dict):
                                                # It's already a dictionary
                                                existing_data = existing_record[0]
                                            elif isinstance(existing_record[0], str):
                                                # It's a JSON string
                                                existing_data = json.loads(existing_record[0])
                                            else:
                                                # Try to convert to string first
                                                existing_data = json.loads(str(existing_record[0]))
                                                
                                            merged_data.update(existing_data)  # Start with existing data
                                            merged_data.update(new_measurement_data)  # Add new data, overwriting if keys exist
                                        except (json.JSONDecodeError, TypeError) as e:
                                            logging.error(f"Error processing existing data for {device_api_ps_key}: {e}")
                                            merged_data = new_measurement_data
                                    else:
                                        # No existing record, just use the new data
                                        merged_data = new_measurement_data
                                    
                                    # Now upsert the record with the merged data
                                    upsert_query = text("""
                                        INSERT INTO isolarcloud_historical_data 
                                            (device_ps_key, timestamp, measurement_data)
                                        VALUES 
                                            (:device_ps_key, :timestamp, :measurement_data)
                                        ON CONFLICT (device_ps_key, timestamp) DO UPDATE SET
                                            measurement_data = :measurement_data
                                    """)
                                    
                                    try:
                                        # Ensure we have a properly serialized JSON string
                                        json_data = json.dumps(merged_data) if isinstance(merged_data, dict) else merged_data
                                        
                                        session.execute(upsert_query, {
                                            "device_ps_key": device_api_ps_key,
                                            "timestamp": converted_utc_timestamp,
                                            "measurement_data": json_data
                                        })
                                    except Exception as e:
                                        logging.error(f"Error during database upsert: {e}")
                                        continue
                                    
                                    total_data_points_ingested += 1
                            
                            session.commit()
                            logging.info(f"Successfully merged {total_data_points_ingested} data points.")
                        except SQLAlchemyError as e:
                            session.rollback()
                            logging.error(f"Database error during data merge: {e}")
                        finally:
                            session.close()
                else:
                    logging.warning(f"API request failed or returned unexpected data")
    
    return total_data_points_ingested

def fetch_only_inverter_summary_data(start_date_str, end_date_str, ps_ids_str, specific_device_id,
                                    engine, data_processing, fetch_historical_data_for_batch,
                                    batch_days=3, minute_interval=5):
    """Fetches only the inverter summary data points for the given date range."""
    print(f"Fetching ONLY inverter summary data points from {start_date_str} to {end_date_str}")
    session = None
    
    try:
        # Parse start date
        try:
            # Try date-time format first
            start_time_dt = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Fall back to date-only format
            start_time_dt = datetime.strptime(start_date_str, '%Y-%m-%d').replace(hour=0, minute=0, second=0)
        
        # Parse end date (separately since formats might be different)
        try:
            # Try date-time format first
            end_time_dt = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Fall back to date-only format
            end_time_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            
        logging.info(f"Parsed date range: {start_time_dt} to {end_time_dt}")
    except ValueError as e:
        logging.error(f"Invalid date format: {e}. Please use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS.")
        return

    if end_time_dt <= start_time_dt:
        logging.error("End date must be after start date.")
        return
    
    # Verify engine is available
    if not engine:
        logging.error("Database engine not initialized. Cannot fetch data.")
        return
    
    # Create a session using the engine
    SessionMaker = sessionmaker(bind=engine)
    
    try:
        session = SessionMaker()
        # Build the base query to get inverter devices
        base_query = "SELECT ps_id, device_ps_key, device_type, type_name FROM isolarcloud_devices WHERE device_type = 1"
        params = {}
        
        # Add power station filter if specified
        if ps_ids_str:
            ps_id_list = [pid.strip() for pid in ps_ids_str.split(',')]
            if ps_id_list:
                base_query += " AND ps_id = ANY(:ps_ids)"
                params['ps_ids'] = ps_id_list
        
        # Add specific device filter if specified
        if specific_device_id:
            base_query += " AND device_ps_key = :device_ps_key"
            params['device_ps_key'] = specific_device_id
            print(f"Filtering for specific device: {specific_device_id}")
        
        query = text(base_query)
        
        result = session.execute(query, params)
        
        # Convert each row to a dictionary with proper column names
        inverter_devices = []
        for row in result:
            device_dict = {}
            for idx, column in enumerate(result.keys()):
                device_dict[column] = row[idx]
            inverter_devices.append(device_dict)
        
        if not inverter_devices:
            logging.warning("No inverter devices found in database.")
            return
        
        print(f"Found {len(inverter_devices)} inverter devices to process")
        
        # Process in batches of days
        current_date = start_time_dt
        total_points_ingested = 0

        while current_date <= end_time_dt:
            batch_end_date = min(current_date + timedelta(days=batch_days), end_time_dt)
            batch_end_date = batch_end_date.replace(hour=23, minute=59, second=59)
            
            print(f"Processing batch from {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}")
            
            # Instead of using fetch_historical_data_for_batch, use our custom function that merges data
            hourly_intervals = []
            hourly_start = current_date
            
            while hourly_start < batch_end_date:
                hourly_end = min(hourly_start + timedelta(hours=1) - timedelta(seconds=1), batch_end_date)
                hourly_intervals.append((hourly_start, hourly_end))
                hourly_start += timedelta(hours=1)
            
            for interval_start, interval_end in hourly_intervals:
                points_ingested = custom_fetch_and_store_minute_data(
                    inverter_devices, 
                    interval_start, 
                    interval_end, 
                    minute_interval
                )
                total_points_ingested += points_ingested
            
            print(f"Merged {total_points_ingested} inverter summary data points for this batch")
            
            current_date = batch_end_date + timedelta(days=1)
            current_date = current_date.replace(hour=0, minute=0, second=0)
        
        print(f"Inverter summary data fetch complete. Total points merged: {total_points_ingested}")
    except SQLAlchemyError as e:
        logging.error(f"Database error during inverter summary data fetch: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if session is not None:
            session.close()

# Execute main function if script is run directly
if __name__ == "__main__":
    # Show usage example if no arguments are provided
    if len(sys.argv) == 1:
        print("Example usage:")
        print("  python fetch_inverter_level_data.py --start-date \"2025-04-01\" --end-date \"2025-04-30\"")
        print("  python fetch_inverter_level_data.py --start-date \"2025-04-01\" --end-date \"2025-04-30\" --ps-id \"1445767\" --device-id \"1445767_1_1_1\"")
        print("  python fetch_inverter_level_data.py --start-date \"2025-04-01\" --end-date \"2025-04-30\" --dry-run")
        print("\nUse --help for more options")
        sys.exit(1)
        
    sys.exit(main())
