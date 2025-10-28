import logging
import argparse
import traceback
from datetime import datetime, timezone
from sqlalchemy import text

# Imports from the isolarcloud_harvester_src modules
from isolarcloud_harvester_src.isolar_config import ISOLARCLOUD_APP_KEY, ISOLARCLOUD_SECRET_KEY, ISOLARCLOUD_USERNAME, ISOLARCLOUD_PASSWORD
from isolarcloud_harvester_src.isolar_api_client import login_isolarcloud
from isolarcloud_harvester_src.isolar_db_operations import init_database, sync_power_stations, sync_devices, engine
from isolarcloud_harvester_src.isolar_data_processing import fetch_historical_data, fetch_yesterday_data_for_all_devices
# Logging Configuration - should be configured once
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more verbose output
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("isolarcloud_harvester.log"),  # Log to file
        logging.StreamHandler()  # Log to console
    ]
)
logging.info("Script started. Attempting to harvest data from iSolarCloud API.")

def main():
    print("Starting main function")
    # Initialize PostgreSQL database first, as other operations might depend on it
    print("About to initialize database")
    if not init_database():
        print("Database initialization failed")
        logging.error("Exiting script due to database initialization failure.")
        return
    print("Database initialization successful")

    # Attempt to log in to iSolarCloud
    # The token is stored globally in api_client_module.
    print("About to login to iSolarCloud")
    if not login_isolarcloud():
        print("iSolarCloud login failed")
        logging.error("Exiting script due to iSolarCloud login failure.")
        return
    print("iSolarCloud login successful")

    parser = argparse.ArgumentParser(description="iSolarCloud Data Harvester")
    parser.add_argument("--sync-powerstations", action="store_true", help="Synchronize all power stations.")
    parser.add_argument("--sync-devices", type=str, metavar="PS_ID", help="Synchronize devices for a specific power station ID. Use 'all' to sync devices for all known power stations.")
    
    parser.add_argument("--fetch-historical", nargs=2, metavar=("YYYY-MM-DD_START", "YYYY-MM-DD_END"), 
                        help="Fetch historical minute data for a date range.")
    parser.add_argument("--ps-ids", type=str, help="Comma-separated list of power station IDs to filter for --fetch-historical.")
    parser.add_argument("--device-types", type=str, help="Comma-separated list of device type names (e.g., inverter, meter) to filter for --fetch-historical.")

    parser.add_argument("--fetch-yesterday", action="store_true", help="Fetch all of yesterday's data for all devices.")
    
    args = parser.parse_args()
    print(f"Arguments parsed: {vars(args)}")

    if not any(vars(args).values()): # Check if any argument was passed
        print("No arguments were provided")
        parser.print_help()
        logging.info("No action specified. Exiting.")
        return
    
    print(f"Arguments received: sync_powerstations={args.sync_powerstations}")


    if args.sync_powerstations:
        print("About to sync power stations")
        logging.info("Action: Synchronizing power stations.")
        try:
            sync_power_stations()
            print("Power stations sync completed")
        except Exception as e:
            print(f"Error during sync_power_stations: {e}")
            print(traceback.format_exc())

    if args.sync_devices:
        if args.sync_devices.lower() == 'all':
            logging.info("Action: Synchronizing devices for all power stations.")
            try:
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT ps_id FROM isolarcloud_power_stations"))
                    for row in result:
                        sync_devices(row[0])
            except Exception as e:
                logging.error(f"Error fetching power station IDs from database: {e}")
        else:
            logging.info(f"Action: Synchronizing devices for power station ID: {args.sync_devices}.")
            sync_devices(args.sync_devices)

    if args.fetch_historical:
        start_date, end_date = args.fetch_historical
        logging.info(f"Action: Fetching historical data from {start_date} to {end_date}.")
        print(f"Fetching historical data from {start_date} to {end_date}")
        
        # Use test_historical_data approach which works properly
        # We'll use our own historical data function that properly initializes the engine
        try:
            fetch_historical_data(start_date, end_date, args.ps_ids, args.device_types)
        except Exception as e:
            print(f"Error during historical data fetch: {e}")
            print(traceback.format_exc())
            return

    if args.fetch_yesterday:
        logging.info("Action: Fetching yesterday's data for all devices.")
        fetch_yesterday_data_for_all_devices()

    logging.info("Script finished.")

if __name__ == "__main__":
    main()
