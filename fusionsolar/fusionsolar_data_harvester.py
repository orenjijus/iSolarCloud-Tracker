import logging
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Import modules from the harvester package
from fusionsolar_harvester_src.fusionsolar_config import FUSIONSOLAR_USERNAME, FUSIONSOLAR_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from fusionsolar_harvester_src.fusionsolar_api_client import login_fusionsolar
from fusionsolar_harvester_src.fusionsolar_db_operations import init_database, sync_plants, sync_devices
from fusionsolar_harvester_src.fusionsolar_data_processing import fetch_historical_data, fetch_yesterday_data

# Log file next to this script (absolute path so it works from cron)
LOG_DIR = Path(__file__).resolve().parent
LOG_FILE = LOG_DIR / "fusionsolar_harvester.log"

# Configure logging: file + console; file so we have a persistent harvester log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
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
    print(f"Database target: host={POSTGRES_HOST} port={POSTGRES_PORT} database={POSTGRES_DB} (pastikan SQL client connect ke sini)")

    # Set up command line argument parser
    parser = argparse.ArgumentParser(description="FusionSolar Data Harvester")
    parser.add_argument("--sync-plants", action="store_true", help="Synchronize all plants.")
    parser.add_argument("--sync-devices", type=str, nargs='?', const='all', metavar="PLANT_CODE", help="Synchronize devices. If no plant code is provided, syncs devices for all known plants. Use 'all' or a specific plant code.")
    
    parser.add_argument("--fetch-historical", nargs=2, metavar=("YYYY-MM-DD_START", "YYYY-MM-DD_END"), 
                        help="Fetch historical minute data for a date range.")
    parser.add_argument("--plant-codes", type=str, help="Comma-separated list of plant codes to filter for --fetch-historical.")
    parser.add_argument("--device-types", type=str, help="Comma-separated device type names for --fetch-historical: inverter, meter, meteo_station, battery, smart_assistant.")

    parser.add_argument("--fetch-yesterday", action="store_true", help="Fetch all of yesterday's data for all devices.")
    
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
        ok, error_message = login_fusionsolar()
        if not ok:
            msg = error_message or "Unknown error"
            print(f"FusionSolar API login failed: {msg}")
            logging.error("Exiting script due to FusionSolar API login failure: %s", msg)
            sys.exit(1)  # So cron/pipeline fails visibly and does not continue
        print("FusionSolar API login successful")
    
    try:
        # Process the requested actions
        if args.sync_plants:
            print("Synchronizing plants")
            logging.info("Action: Synchronizing plants.")
            sync_plants()
            print("Plants synchronization completed")

        if args.sync_devices is not None:
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
    except Exception as e:
        logging.error(f"Error during execution: {e}")
    finally:
        logging.info("Script finished.")
        print("FusionSolar Data Harvester completed")

if __name__ == "__main__":
    main()
