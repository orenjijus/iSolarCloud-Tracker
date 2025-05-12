import logging
import time
from datetime import datetime, timedelta, timezone

from .config import (MAX_PS_KEYS_PER_REQUEST, MAX_POINTS_PER_REQUEST, REQUEST_DELAY_SECONDS, 
                         DEVICE_TYPE_MEASURING_POINTS, get_measuring_points_for_device_type, DAYS_PER_HISTORICAL_BATCH)
from .api_client import _make_api_request


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
            if config_data.get('api_device_type_code') == api_type_code:
                device_type_name_for_points = name
                break
    return device_type_name_for_points

def fetch_and_store_minute_data(supabase_client, devices_to_fetch, start_time_dt, end_time_dt, minute_interval=5):
    """Fetches minute-level data and stores it in Supabase."""
    if not supabase_client:
        logging.error("Supabase client not initialized in data_processing. Cannot store minute data.")
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
                        supabase_data_to_insert = []
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
                                
                                row_data = {
                                    "device_ps_key": device_api_ps_key, # Use the key from the API response
                                    "timestamp": converted_utc_timestamp,
                                }
                                for key, value in point_data_item.items():
                                    if key.lower() != "time_stamp": # Exclude the original time_stamp
                                        row_data[key] = value
                                
                                supabase_data_to_insert.append(row_data)
                        
                        if supabase_data_to_insert:
                            try:
                                logging.info(f"Attempting to upsert {len(supabase_data_to_insert)} records to isolarcloud_historical_data.")
                                response = supabase_client.table("isolarcloud_historical_data") \
                                                          .upsert(supabase_data_to_insert, on_conflict='device_ps_key,timestamp') \
                                                          .execute()
                                
                                upserted_count = 0
                                if hasattr(response, 'data') and response.data is not None:
                                    upserted_count = len(response.data)
                                elif hasattr(response, 'count') and response.count is not None and (not hasattr(response, 'data') or response.data is None):
                                    upserted_count = response.count
                                
                                if upserted_count > 0:
                                    logging.info(f"Successfully upserted {upserted_count} data points.")
                                    total_data_points_ingested += upserted_count
                                else:
                                    # This case can occur if all records in the batch resulted in updates due to on_conflict, 
                                    # and the client version/response doesn't detail updated rows in 'data' or 'count'.
                                    # Or, if no new records were inserted and no existing records were updated.
                                    logging.info(f"Upsert call made. Response indicates {upserted_count} records were directly counted as upserted (inserted/updated). Review response if details are needed: {response}")


                                if hasattr(response, 'error') and response.error:
                                    logging.error(f"Supabase upsert error: {response.error}")

                            except Exception as db_e:
                                logging.error(f"Exception during Supabase upsert: {db_e}")
                                import traceback
                                logging.error(traceback.format_exc())
                        else:
                            logging.info("No data to insert into Supabase for this API data batch.")
                            
                    elif api_response_parsed is None: # Error already logged by _make_api_request
                        pass 
                    else: # This covers api_response_parsed.get("result_code") != "1"
                        logging.warning(f"API request failed or returned unexpected data: {api_response_parsed}")

    return total_data_points_ingested





def fetch_historical_data_for_batch(devices_batch, day_dt_start, day_dt_end, minute_interval, supabase_client):
    """Processes a batch of devices for a given day, fetching data in 1-hour intervals."""
    logging.info(f"Processing day-batch: {day_dt_start.strftime('%Y-%m-%d')} for {len(devices_batch)} devices.")
    
    current_interval_start = day_dt_start
    total_points_ingested_for_day_batch = 0

    # day_dt_end is the end of the day (e.g., 23:59:59)
    while current_interval_start < day_dt_end:
        # Calculate end of the current 1-hour interval (e.g., start 00:00:00 -> end 00:59:59)
        current_interval_end = min(current_interval_start + timedelta(hours=1) - timedelta(seconds=1), day_dt_end)
        
        # Ensure the interval is valid, especially for the last partial hour
        if current_interval_end < current_interval_start:
            # This might happen if day_dt_end was exactly on an hour boundary before subtraction, adjust to process the last second.
             current_interval_end = current_interval_start 

        logging.info(f"Fetching data for 1-hour interval: {current_interval_start.strftime('%Y-%m-%d %H:%M:%S')} to {current_interval_end.strftime('%Y-%m-%d %H:%M:%S')}")

        points_ingested_for_interval = fetch_and_store_minute_data(
            supabase_client,      # First arg for fetch_and_store_minute_data
            devices_batch,        # Second arg
            current_interval_start, # Third arg
            current_interval_end,   # Fourth arg
            minute_interval         # Fifth arg
        )
        if points_ingested_for_interval: # fetch_and_store_minute_data returns an int or 0
             total_points_ingested_for_day_batch += points_ingested_for_interval
        
        # Move to the start of the next 1-hour interval
        current_interval_start += timedelta(hours=1)
        
    logging.info(f"Total data points ingested for day-batch ({day_dt_start.strftime('%Y-%m-%d')}): {total_points_ingested_for_day_batch}")
    return total_points_ingested_for_day_batch


def fetch_historical_data(supabase_client, start_date_str, end_date_str, ps_ids_str=None, device_types_str=None):
    """Fetches historical data for a given date range, optionally filtered by power station IDs and device types."""
    if not supabase_client:
        logging.error("Supabase client not initialized. Cannot fetch historical data.")
        return

    try:
        start_time_dt = datetime.strptime(start_date_str, '%Y-%m-%d').replace(hour=0, minute=0, second=0)
        end_time_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    except ValueError:
        logging.error("Invalid date format. Please use YYYY-MM-DD.")
        return

    if end_time_dt <= start_time_dt:
        logging.error("End date must be after start date.")
        return
    
    logging.info(f"Preparing to fetch historical data from {start_time_dt.strftime('%Y-%m-%d')} to {end_time_dt.strftime('%Y-%m-%d')}")

    device_query = supabase_client.table("isolarcloud_devices").select("ps_id, device_ps_key, device_type, type_name")
    if ps_ids_str:
        ps_id_list = [pid.strip() for pid in ps_ids_str.split(',')]
        if ps_id_list:
            device_query = device_query.in_("ps_id", ps_id_list)
    
    try:
        device_response = device_query.execute()
        if not device_response.data:
            logging.warning("No devices found in Supabase matching ps_id criteria (or no ps_ids specified and no devices exist).")
            return
        
        devices_to_process = device_response.data

        if device_types_str:
            logging.info(f"Filtering for device types: {device_types_str}")
            filter_types_input = [dt.strip().lower() for dt in device_types_str.split(',')]
            
            filtered_devices_for_type = []
            for dev in devices_to_process:
                # Use the same mapping logic as in fetch_and_store_minute_data
                mapped_type = _map_device_type_name_for_points(dev)
                if mapped_type in filter_types_input or dev.get('type_name', '').lower() in filter_types_input:
                    filtered_devices_for_type.append(dev)
            devices_to_process = filtered_devices_for_type
            logging.info(f"Found {len(devices_to_process)} devices after type filtering.")

        if not devices_to_process:
            logging.warning("No devices found after applying all filters. Nothing to fetch.")
            return

        current_batch_start_dt = start_time_dt
        total_ingested_all_batches = 0

        while current_batch_start_dt <= end_time_dt:
            current_batch_end_dt = current_batch_start_dt + timedelta(days=DAYS_PER_HISTORICAL_BATCH - 1)
            current_batch_end_dt = current_batch_end_dt.replace(hour=23, minute=59, second=59)
            
            if current_batch_end_dt > end_time_dt:
                current_batch_end_dt = end_time_dt
            
            logging.info(f"Fetching batch: {current_batch_start_dt.strftime('%Y-%m-%d')} to {current_batch_end_dt.strftime('%Y-%m-%d')} for {len(devices_to_process)} devices.")
            
            batch_ingested = fetch_historical_data_for_batch(devices_to_process, current_batch_start_dt, current_batch_end_dt, 5, supabase_client)
            logging.info(f"Batch from {current_batch_start_dt.strftime('%Y-%m-%d')} to {current_batch_end_dt.strftime('%Y-%m-%d')} complete. Ingested {batch_ingested} data points.")
            total_ingested_all_batches += batch_ingested
            
            current_batch_start_dt += timedelta(days=DAYS_PER_HISTORICAL_BATCH)
            if current_batch_start_dt > end_time_dt: # More precise check for loop termination
                break
        
        logging.info(f"Historical data fetch fully complete. Total ingested over all batches: {total_ingested_all_batches} data points.")

    except Exception as e:
        logging.error(f"Error during historical data fetching process: {e}")
        import traceback
        logging.error(traceback.format_exc())

def fetch_yesterday_data_for_all_devices(supabase_client):
    """Fetches all of yesterday's data for all devices stored in Supabase."""
    if not supabase_client:
        logging.error("Supabase client not initialized. Cannot fetch yesterday data.")
        return

    logging.info("Starting to fetch yesterday's data for all devices...")
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    start_date_str = yesterday.strftime('%Y-%m-%d')
    end_date_str = start_date_str # Fetch for a single day

    # No ps_id or device_type filters, so they will be None (fetch all)
    fetch_historical_data(supabase_client, start_date_str, end_date_str, None, None)
    logging.info("Finished fetching yesterday's data for all devices.")

