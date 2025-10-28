#!/usr/bin/env python
# Python script to collect historical data for all FusionSolar sites
# This script processes one site at a time to stay within API rate limits

import os
import sys
import time
import subprocess
import math
from datetime import datetime

# Configuration
START_DATE = "2025-05-01"
END_DATE = "2025-05-27"
PYTHON_CMD = "python"  # Change this if you're using a specific Python environment

# Define sites and their device counts
SITES = [
    {
        "name": "MMKI 1",
        "code": "NE=50488260",
        "inverters": 10,
        "sensors": 3,
        "meters": 2
    },
    {
        "name": "MMKI 2",
        "code": "NE=51758766",
        "inverters": 17,
        "sensors": 9,
        "meters": 11
    },
    {
        "name": "Mall Panakkukang",
        "code": "NE=53771627",
        "inverters": 10,
        "sensors": 10,
        "meters": 4
    },
    {
        "name": "Pusan Manis",
        "code": "NE=54435794",
        "inverters": 14,
        "sensors": 18,
        "meters": 5
    }
]

def collect_site_data(site_name, site_code, inverter_count, sensor_count, meter_count):
    """Collect data for a specific site"""
    print("=" * 60)
    print(f"Starting data collection for {site_name} ({site_code})")
    print(f"Devices: {inverter_count} inverters, {sensor_count} sensors, {meter_count} meters")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print("=" * 60)
    
    # Calculate expected API calls
    inverter_batches = math.ceil(inverter_count / 10)
    sensor_batches = math.ceil(sensor_count / 10)
    meter_batches = math.ceil(meter_count / 10)
    total_calls = (inverter_batches + sensor_batches + meter_batches) * 9  # 9 date windows for 27 days
    
    print(f"Estimated API calls: {total_calls}")
    print(f"Daily limit: {math.ceil((inverter_count + sensor_count + meter_count) / 10) + 24}")
    
    # Run the data harvester with rate limiting for this site
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                              "fusionsolar_data_harvester.py")
    command = [
        PYTHON_CMD,
        script_path,
        "--fetch-historical", START_DATE, END_DATE,
        "--plant-codes", site_code,
        "--device-types", "inverter,meter,meteo_station"
    ]
    
    print(f"Executing: {' '.join(command)}")
    result = subprocess.run(command)
    
    if result.returncode != 0:
        print(f"Error collecting data for {site_name}. Please check the logs.", file=sys.stderr)
        return False
    
    print(f"Successfully collected data for {site_name}")
    return True

def main():
    """Main execution function"""
    overall_success = True
    
    for site in SITES:
        success = collect_site_data(
            site["name"], 
            site["code"], 
            site["inverters"], 
            site["sensors"], 
            site["meters"]
        )
        
        if not success:
            overall_success = False
            print(f"Stopping collection due to error with site: {site['name']}", file=sys.stderr)
            break
        
        # Add a small delay between sites to avoid any potential issues
        print("Waiting 30 seconds before proceeding to next site...")
        time.sleep(30)
    
    print("=" * 70)
    if overall_success:
        print("Historical data collection completed successfully for all sites!")
    else:
        print("Historical data collection incomplete. Please check the logs.")
    print("=" * 70)

if __name__ == "__main__":
    main()
