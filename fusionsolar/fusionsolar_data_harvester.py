import logging
import argparse
from datetime import datetime, timezone

# Import modules from the harvester package
from fusionsolar_harvester_src.fusionsolar_config import FUSIONSOLAR_USERNAME, FUSIONSOLAR_PASSWORD
from fusionsolar_harvester_src.fusionsolar_api_client import login_fusionsolar
from fusionsolar_harvester_src.fusionsolar_db_operations import init_database, sync_plants, sync_devices
from fusionsolar_harvester_src.fusionsolar_data_processing import fetch_historical_data, fetch_yesterday_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fusionsolar_harvester.log"),  # Log to file
        logging.StreamHandler()  # Log to console
    ]
)
logging.info("Script started. Attempting to harvest data from FusionSolar API.")

def main():
    print("Starting FusionSolar Data Harvester")
    
    # Initialize PostgreSQL database
    print("Initializing database connection")
    if not init_database():
        print("Database initialization failed")
        logging.error("Exiting script due to database initialization failure.")
        return
    print("Database initialization successful")

    # Set up command line argument parser
    parser = argparse.ArgumentParser(description="FusionSolar Data Harvester")
    parser.add_argument("--sync-plants", action="store_true", help="Synchronize all plants.")
    parser.add_argument("--sync-devices", type=str, metavar="PLANT_CODE", help="Synchronize devices for a specific plant code. Use 'all' to sync devices for all known plants.")
    
    parser.add_argument("--fetch-historical", nargs=2, metavar=("YYYY-MM-DD_START", "YYYY-MM-DD_END"), 
                        help="Fetch historical minute data for a date range.")
    parser.add_argument("--plant-codes", type=str, help="Comma-separated list of plant codes to filter for --fetch-historical.")
    parser.add_argument("--device-types", type=str, help="Comma-separated list of device type names (e.g., inverter, meter) to filter for --fetch-historical.")

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
            print("Synchronizing plants")
            logging.info("Action: Synchronizing plants.")
            sync_plants()
            print("Plants synchronization completed")

        if args.sync_devices:
            if args.sync_devices.lower() == 'all':
                logging.info("Action: Synchronizing devices for all plants.")
                sync_devices()
            else:
                logging.info(f"Action: Synchronizing devices for plant code: {args.sync_devices}.")
                sync_devices(args.sync_devices)

        if args.fetch_historical:
            start_date, end_date = args.fetch_historical
            logging.info(f"Action: Fetching historical data from {start_date} to {end_date}.")
            print(f"Fetching historical data from {start_date} to {end_date}")
            
            fetch_historical_data(start_date, end_date, args.plant_codes, args.device_types)

        if args.fetch_yesterday:
            logging.info("Action: Fetching yesterday's data for all devices.")
            fetch_yesterday_data()

        if args.create_views:
            logging.info("Action: Creating SQL views for site-device data.")
            print("Creating SQL views for site-device data")
            # Import here to avoid circular imports
            from site_device_views import create_device_views
            create_device_views()
            print("SQL views created successfully")
    except Exception as e:
        logging.error(f"Error during execution: {e}")
    finally:
        logging.info("Script finished.")
        print("FusionSolar Data Harvester completed")

if __name__ == "__main__":
    main()
