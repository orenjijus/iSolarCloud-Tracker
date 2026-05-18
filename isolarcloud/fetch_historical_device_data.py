import logging
import os
import time
from datetime import datetime
from functools import wraps
from isolarcloud_harvester_src.isolar_config import DATABASE_URL
from isolarcloud_harvester_src.isolar_db_operations import init_database
from isolarcloud_harvester_src.isolar_api_client import login_isolarcloud
import sys
sys.stdin.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(encoding='utf-8')

# Import fetch_historical_data after database init to avoid circular import issues

# Create logs directory if it doesn't exist
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# Configure logging
log_filename = f"{log_dir}/isolarcloud_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
etl_logger = logging.getLogger('etl_logger')
etl_logger.setLevel(logging.INFO)

# Create file handler
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to logger
etl_logger.addHandler(file_handler)
etl_logger.addHandler(console_handler)

# Timer decorator for measuring execution time
def timer_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        etl_logger.info(f"Starting {func.__name__}...")
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            etl_logger.info(f"{func.__name__} completed successfully in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            etl_logger.error(f"{func.__name__} failed after {execution_time:.2f} seconds with error: {str(e)}")
            raise
    return wrapper

total_start_time = time.time()
etl_logger.info("=== Starting ETL process for iSolarCloud historical data fetch ===")

# Initialize database and login
etl_logger.info("Initializing database...")
try:
    if not init_database():
        etl_logger.error("Failed to initialize database")
        exit(1)
    etl_logger.info("Database initialized successfully")
except Exception as e:
    etl_logger.error(f"Database initialization error: {str(e)}")
    exit(1)

etl_logger.info("Logging in to iSolarCloud...")
try:
    if not login_isolarcloud():
        etl_logger.error("Failed to login to iSolarCloud")
        exit(1)
    etl_logger.info("Login successful")
except Exception as e:
    etl_logger.error(f"Login error: {str(e)}")
    exit(1)

# Fetch data for devices type meter/inverter/meteo_station
start_date = "2026-02-05"
end_date = "2026-02-05"
ps_id = None  # All power stations
device_type = "meter,inverter,meteo_station"  # meter/inverter/meteo_station

etl_logger.info(f"Fetching historical data from {start_date} to {end_date} for power station {ps_id if ps_id else 'ALL'} and device type {device_type}")

# Import fetch_historical_data only after database is initialized to avoid circular import issues
from isolarcloud_harvester_src.isolar_data_processing import fetch_historical_data

# Apply timer decorator
timed_fetch_historical_data = timer_decorator(fetch_historical_data)

try:
    # Execute fetch with timing
    timed_fetch_historical_data(start_date, end_date, ps_id, device_type)
    etl_status = "SUCCESS"
except Exception as e:
    etl_logger.error(f"Error during data fetch: {str(e)}")
    etl_status = "FAILED"

# Log total execution time
total_end_time = time.time()
total_execution_time = total_end_time - total_start_time
etl_logger.info(f"=== ETL process completed with status: {etl_status} ===")
etl_logger.info(f"Total execution time: {total_execution_time:.2f} seconds")

# Write a simple status file that can be easily parsed by monitoring tools
with open(f"{log_dir}/isolarcloud_last_etl_status.txt", "w") as status_file:
    status_file.write(f"timestamp: {datetime.now().isoformat()}\n")
    status_file.write(f"status: {etl_status}\n")
    status_file.write(f"execution_time: {total_execution_time:.2f}\n")
    status_file.write(f"start_date: {start_date}\n")
    status_file.write(f"end_date: {end_date}\n")
    status_file.write(f"device_type: {device_type}\n")
    status_file.write(f"power_station: {ps_id if ps_id else 'ALL'}\n")
