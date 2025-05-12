import logging
import time
from supabase import create_client, Client

from .config import SUPABASE_URL, SUPABASE_ANON_KEY, REQUEST_DELAY_SECONDS
from .api_client import _make_api_request

# Global Supabase client, to be initialized by the main script
supabase_client: Client = None

def init_supabase_client():
    """Initializes the Supabase client and assigns it to the global variable."""
    global supabase_client
    if SUPABASE_URL and SUPABASE_ANON_KEY:
        try:
            supabase_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
            logging.info("Supabase client initialized successfully.")
            return supabase_client
        except Exception as e:
            logging.error(f"Failed to initialize Supabase client: {e}")
            supabase_client = None
            return None
    else:
        logging.error("Supabase URL or Anon Key not found. Supabase client cannot be initialized.")
        supabase_client = None
        return None

def sync_power_stations():
    """Fetches all power stations and stores/updates them in Supabase."""
    if not supabase_client:
        logging.error("Supabase client not initialized. Cannot sync power stations.")
        return

    logging.info("Starting power station synchronization...")
    all_stations = []
    current_page = 1
    page_size = 20 

    while True:
        payload = {
            "curPage": current_page,
            "size": page_size
        }
        # Use _make_api_request from api_client module
        data = _make_api_request("/openapi/getPowerStationList", payload)
        time.sleep(REQUEST_DELAY_SECONDS) # Ensure delay after every API call

        if not data:
            logging.warning(f"No data received from getPowerStationList page {current_page}. Ending sync.")
            break

        stations_on_page = data.get("pageList", [])
        if not stations_on_page:
            logging.info("No more power stations found on current page.")
            break
        
        all_stations.extend(stations_on_page)
        logging.info(f"Fetched page {current_page} with {len(stations_on_page)} power stations.")

        if len(stations_on_page) < page_size or data.get("rowCount", 0) == len(all_stations):
            logging.info("All power station pages fetched.")
            break
        current_page += 1
        # Delay is now after _make_api_request

    if not all_stations:
        logging.info("No power stations to sync.")
        return

    supabase_stations_data = []
    for station in all_stations:
        supabase_stations_data.append({
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
            "ps_type": station.get("ps_type"),
        })
    
    try:
        response = supabase_client.table("isolarcloud_power_stations").upsert(supabase_stations_data, on_conflict="ps_id").execute()
        logging.info(f"Successfully synced {len(supabase_stations_data)} power stations to Supabase.")
        if hasattr(response, 'error') and response.error:
            logging.error(f"Error syncing power stations to Supabase: {response.error}")
    except Exception as e:
        logging.error(f"Exception during Supabase upsert for power stations. Type: {type(e)}, Exception: {e}")
        
        # Attempt to get detailed error message from Supabase client exception
        error_message = None
        error_details_dict = None

        if hasattr(e, 'message'): # Common attribute for Supabase/GoTrue errors
            if isinstance(e.message, str):
                error_message = e.message
            elif isinstance(e.message, dict) and 'message' in e.message: # Sometimes it's a dict
                 error_message = e.message['message']
        
        if hasattr(e, 'json'): # For PostgrestAPIError
            try:
                error_details_dict = e.json()
                if not error_message and 'message' in error_details_dict:
                    error_message = error_details_dict['message']
            except Exception as json_e:
                logging.warning(f"Could not parse e.json(): {json_e}")
        
        if hasattr(e, 'args') and e.args: # Fallback for more generic errors
            if not error_message:
                 error_message = str(e.args[0])
            if not error_details_dict and isinstance(e.args[0], dict):
                error_details_dict = e.args[0]
            elif not error_details_dict and isinstance(e.args[0], str):
                try:
                    # Sometimes the first arg is a JSON string
                    import json
                    error_details_dict = json.loads(e.args[0])
                    if not error_message and 'message' in error_details_dict:
                         error_message = error_details_dict['message']
                except:
                    pass # Not a JSON string

        if error_message:
            logging.error(f"Supabase error message: {error_message}")
        if error_details_dict:
            logging.error(f"Supabase error details dict: {error_details_dict}")
        
        # Log standard attributes if present
        if hasattr(e, 'status_code'): 
            logging.error(f"Supabase error status_code: {e.status_code}")
        if hasattr(e, 'code'):
            logging.error(f"Supabase error code: {e.code}")
        
        # For HTTPX based errors that might be wrapped
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Underlying HTTP response status code: {e.response.status_code}")
            try:
                response_json = e.response.json()
                logging.error(f"Underlying HTTP response JSON: {response_json}")
            except ValueError:
                logging.error(f"Underlying HTTP response text: {e.response.text}")
        
        logging.error(f"Full exception args: {e.args}")

def sync_devices(power_station_id):
    """Fetches all devices for a given power station and stores/updates them in Supabase."""
    if not supabase_client:
        logging.error("Supabase client not initialized. Cannot sync devices.")
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
        time.sleep(REQUEST_DELAY_SECONDS) # Ensure delay after every API call

        if not data:
            logging.warning(f"No data received from getDeviceList page {current_page} for ps_id {power_station_id}. Ending sync for this PS.")
            break
        
        devices_on_page = data.get("pageList", [])
        if not devices_on_page:
            logging.info(f"No more devices found for power station {power_station_id} on page {current_page}.")
            break
        
        all_devices.extend(devices_on_page)
        logging.info(f"Fetched page {current_page} with {len(devices_on_page)} devices for power station {power_station_id}.")

        if len(devices_on_page) < page_size or data.get("rowCount", 0) == len(all_devices):
            logging.info(f"All device pages fetched for power station {power_station_id}.")
            break
        current_page += 1
        # Delay is now after _make_api_request

    if not all_devices:
        logging.info(f"No devices to sync for power station {power_station_id}.")
        return

    supabase_devices_data = []
    for device in all_devices:
        supabase_devices_data.append({
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

    try:
        response = supabase_client.table("isolarcloud_devices").upsert(supabase_devices_data, on_conflict="device_ps_key").execute()
        logging.info(f"Successfully synced {len(supabase_devices_data)} devices for ps_id {power_station_id} to Supabase.")
        if hasattr(response, 'error') and response.error:
            logging.error(f"Error syncing devices to Supabase for ps_id {power_station_id}: {response.error}")
    except Exception as e:
        logging.error(f"Exception during Supabase upsert for devices (ps_id {power_station_id}): {e}")
