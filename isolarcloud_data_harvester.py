import logging
import argparse
from datetime import datetime, timezone

# Imports from the new modules (will be isolarcloud_harvester_src.module_name)
from isolarcloud_harvester_src.config import ISOLARCLOUD_APP_KEY, ISOLARCLOUD_SECRET_KEY, ISOLARCLOUD_USERNAME, ISOLARCLOUD_PASSWORD
from isolarcloud_harvester_src.api_client import login_isolarcloud
from isolarcloud_harvester_src.db_operations import init_supabase_client, sync_power_stations, sync_devices
from isolarcloud_harvester_src.data_processing import fetch_historical_data, fetch_yesterday_data_for_all_devices

# Logging Configuration - should be configured once
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Initialize Supabase client first, as other operations might depend on it or config
    # The client is stored globally in db_operations_module upon initialization.
    client = init_supabase_client()
    if not client:
        logging.error("Exiting script due to Supabase client initialization failure.")
        return

    # Attempt to log in to iSolarCloud
    # The token is stored globally in api_client_module.
    if not login_isolarcloud():
        logging.error("Exiting script due to iSolarCloud login failure.")
        return

    parser = argparse.ArgumentParser(description="iSolarCloud Data Harvester")
    parser.add_argument("--sync-powerstations", action="store_true", help="Synchronize all power stations.")
    parser.add_argument("--sync-devices", type=str, metavar="PS_ID", help="Synchronize devices for a specific power station ID. Use 'all' to sync devices for all known power stations.")
    
    parser.add_argument("--fetch-historical", nargs=2, metavar=("YYYY-MM-DD_START", "YYYY-MM-DD_END"), 
                        help="Fetch historical minute data for a date range.")
    parser.add_argument("--ps-ids", type=str, help="Comma-separated list of power station IDs to filter for --fetch-historical.")
    parser.add_argument("--device-types", type=str, help="Comma-separated list of device type names (e.g., inverter, meter) to filter for --fetch-historical.")

    parser.add_argument("--fetch-yesterday", action="store_true", help="Fetch all of yesterday's data for all devices.")
    
    args = parser.parse_args()

    if not any(vars(args).values()): # Check if any argument was passed
        parser.print_help()
        logging.info("No action specified. Exiting.")
        return

    if args.sync_powerstations:
        logging.info("Action: Synchronizing power stations.")
        sync_power_stations() # Uses global supabase_client and token

    if args.sync_devices:
        if args.sync_devices.lower() == 'all':
            logging.info("Action: Synchronizing devices for all power stations.")
            if not client: 
                logging.error("Supabase client not available for fetching all power station IDs.")
                return
            try:
                response = client.table("isolarcloud_power_stations").select("ps_id").execute()
                if response.data:
                    for station in response.data:
                        sync_devices(station['ps_id']) # Uses global supabase_client and token
                else:
                    logging.info("No power stations found in database to sync devices for.")
            except Exception as e:
                logging.error(f"Error fetching power station IDs from Supabase: {e}")
        else:
            logging.info(f"Action: Synchronizing devices for power station ID: {args.sync_devices}.")
            sync_devices(args.sync_devices)

    if args.fetch_historical:
        start_date, end_date = args.fetch_historical
        logging.info(f"Action: Fetching historical data from {start_date} to {end_date}.")
        fetch_historical_data(client, start_date, end_date, args.ps_ids, args.device_types)

    if args.fetch_yesterday:
        logging.info("Action: Fetching yesterday's data for all devices.")
        fetch_yesterday_data_for_all_devices(client)

    logging.info("Script finished.")

if __name__ == "__main__":
    main()
