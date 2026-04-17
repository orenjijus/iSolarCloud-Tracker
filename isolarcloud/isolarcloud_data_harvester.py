import logging
import argparse
import os
import time
import traceback
import sys
from datetime import datetime, timedelta

# Imports from the isolarcloud_harvester_src modules
from isolarcloud_harvester_src.isolar_api_client import login_isolarcloud
from isolarcloud_harvester_src.isolar_db_operations import init_database, sync_power_stations, sync_devices, sync_all_devices
from isolarcloud_harvester_src.isolar_data_processing import fetch_historical_data

# Logging: ensure logs dir exists (for status file and optional ETL logs)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("isolarcloud_harvester.log"),
        logging.StreamHandler(),
    ],
)
logging.info("Script started. Attempting to harvest data from iSolarCloud API.")


def _write_etl_status(
    status,
    execution_time,
    start_date=None,
    end_date=None,
    device_types_str="meter,inverter,meteo_station",
    ps_ids_str=None,
    failure_reason=None,
):
    """Write logs/isolarcloud_last_etl_status.txt with consistent schema."""
    status_path = os.path.join(LOG_DIR, "isolarcloud_last_etl_status.txt")
    with open(status_path, "w") as f:
        f.write(f"timestamp: {datetime.now().isoformat()}\n")
        f.write(f"status: {status}\n")
        f.write(f"execution_time: {execution_time:.2f}\n")
        f.write(f"start_date: {start_date if start_date else '-'}\n")
        f.write(f"end_date: {end_date if end_date else '-'}\n")
        f.write(f"device_type: {device_types_str}\n")
        f.write(f"power_station: {ps_ids_str if ps_ids_str else 'ALL'}\n")
        if failure_reason:
            f.write(f"failure_reason: {failure_reason}\n")


def _run_fetch_and_status(
    start_date,
    end_date,
    ps_ids_str=None,
    device_types_str="meter,inverter,meteo_station",
    parallel_override=None,
    max_workers_override=None,
    logger=None,
):
    """Run fetch_historical_data and write logs/isolarcloud_last_etl_status.txt."""
    logger = logger or logging
    logger.info("Fetching historical data from %s to %s (ps=%s, device_types=%s)", start_date, end_date, ps_ids_str or "ALL", device_types_str)
    total_start = time.time()
    etl_status = "SUCCESS"
    try:
        fetch_historical_data(
            start_date,
            end_date,
            ps_ids_str=ps_ids_str,
            device_types_str=device_types_str,
            parallel_override=parallel_override,
            max_workers_override=max_workers_override,
        )
    except Exception as e:
        logger.error("Error during data fetch: %s", e, exc_info=True)
        etl_status = "FAILED"
        raise
    finally:
        total_time = time.time() - total_start
        logger.info("Fetch completed with status: %s in %.2f s", etl_status, total_time)
        _write_etl_status(
            status=etl_status,
            execution_time=total_time,
            start_date=start_date,
            end_date=end_date,
            device_types_str=device_types_str,
            ps_ids_str=ps_ids_str,
            failure_reason="fetch_historical_data exception" if etl_status == "FAILED" else None,
        )


def main():
    total_start = time.time()
    print("Starting main function")
    # Initialize PostgreSQL database first, as other operations might depend on it
    print("About to initialize database")
    if not init_database():
        print("Database initialization failed")
        logging.error("Exiting script due to database initialization failure.")
        _write_etl_status(
            status="FAILED",
            execution_time=time.time() - total_start,
            failure_reason="database initialization failure",
        )
        sys.exit(1)
    print("Database initialization successful")

    # Attempt to log in to iSolarCloud
    # The token is stored globally in api_client_module.
    print("About to login to iSolarCloud")
    if not login_isolarcloud():
        print("iSolarCloud login failed")
        logging.error("Exiting script due to iSolarCloud login failure.")
        _write_etl_status(
            status="FAILED",
            execution_time=time.time() - total_start,
            failure_reason="isolarcloud login failure",
        )
        sys.exit(1)
    print("iSolarCloud login successful")

    parser = argparse.ArgumentParser(description="iSolarCloud Data Harvester")
    parser.add_argument("--sync-powerstations", action="store_true", help="Synchronize all power stations.")
    parser.add_argument("--sync-devices", type=str, metavar="PS_ID", help="Synchronize devices for a specific power station ID. Use 'all' to sync devices for all known power stations.")
    
    parser.add_argument("--fetch-historical", nargs=2, metavar=("YYYY-MM-DD_START", "YYYY-MM-DD_END"),
                        help="Fetch historical minute data for a date range.")
    parser.add_argument("--ps-ids", type=str, help="Comma-separated list of power station IDs (for --fetch-historical or --fetch-yesterday).")
    parser.add_argument("--ps-id", type=str, help="Single power station ID to filter (convenience for --fetch-yesterday).")
    parser.add_argument("--device-types", type=str, default=None,
                        help="Comma-separated device types (default for fetch: meter,inverter,meteo_station).")
    parser.add_argument("--force-parallel", action="store_true",
                        help="Use parallel workers even for date range > 2 days (may hit API rate limit 2000/h).")
    parser.add_argument("--workers", type=int, default=None, metavar="N",
                        help="Number of parallel workers (default from config). Effective with parallel processing.")

    parser.add_argument("--fetch-yesterday", action="store_true",
                        help="Fetch yesterday's data (local date). Same as --fetch-historical YESTERDAY YESTERDAY with default device types.")
    
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
                sync_all_devices()
                print("All devices sync completed")
            except Exception as e:
                print(f"Error during sync_all_devices: {e}")
                print(traceback.format_exc())
                logging.error(f"Error during sync_all_devices: {e}")
        else:
            logging.info(f"Action: Synchronizing devices for power station ID: {args.sync_devices}.")
            try:
                sync_devices(args.sync_devices)
                print(f"Devices sync completed for power station: {args.sync_devices}")
            except Exception as e:
                print(f"Error during sync_devices: {e}")
                print(traceback.format_exc())
                logging.error(f"Error during sync_devices: {e}")

    if args.fetch_historical:
        start_date, end_date = args.fetch_historical
        _run_fetch_and_status(
            start_date=start_date,
            end_date=end_date,
            ps_ids_str=args.ps_id or args.ps_ids,
            device_types_str=args.device_types or "meter,inverter,meteo_station",
            parallel_override=True if args.force_parallel else None,
            max_workers_override=args.workers,
            logger=logging,
        )

    if args.fetch_yesterday:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logging.info("Action: Fetching yesterday's data (local date): %s", yesterday)
        _run_fetch_and_status(
            start_date=yesterday,
            end_date=yesterday,
            ps_ids_str=args.ps_id or args.ps_ids,
            device_types_str=args.device_types or "meter,inverter,meteo_station",
            parallel_override=True if args.force_parallel else None,
            max_workers_override=args.workers,
            logger=logging,
        )

    logging.info("Script finished.")

if __name__ == "__main__":
    main()
