import logging
import argparse
import json
import time
from datetime import datetime, timezone, timedelta

# Import modules from the harvester package
from fusionsolar_harvester_src.fusionsolar_config import FUSIONSOLAR_USERNAME, FUSIONSOLAR_PASSWORD
from fusionsolar_harvester_src.fusionsolar_api_client import login_fusionsolar, _make_api_request
from fusionsolar_harvester_src.fusionsolar_db_operations import init_database

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fusionsolar_api_test.log", mode='w'),
        logging.StreamHandler()
    ]
)
logging.info("Starting FusionSolar API test script")

def test_api_variations(start_date_str, end_date_str, plant_code, device_type_id=1):
    """Test various API parameter combinations to find what works."""
    # Initialize database for any required config
    print("Initializing database connection")
    if not init_database():
        print("Database initialization failed")
        return
    print("Database initialization successful")
    
    # Login to FusionSolar API
    print("Attempting to login to FusionSolar API")
    if not login_fusionsolar():
        print("FusionSolar API login failed")
        return
    print("FusionSolar API login successful")
    
    # Parse date strings
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    
    # Convert timestamps to milliseconds for the FusionSolar API
    start_timestamp_ms = int(start_date.timestamp() * 1000)
    end_timestamp_ms = int(end_date.timestamp() * 1000)
    current_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Get device IDs for the plant code
    devices = get_devices_for_plant(plant_code, device_type_id)
    if not devices:
        print(f"No devices found for plant {plant_code}")
        return
    
    print(f"Found {len(devices)} devices for plant {plant_code}")
    device_ids = [d.get('dev_id') for d in devices if d.get('dev_id')]
    
    # Test variation 1: Original approach with all devices
    print("\n--- TEST 1: Original approach with all devices ---")
    payload1 = {
        "devIds": ",".join(device_ids),
        "devTypeId": device_type_id,
        "startTime": start_timestamp_ms,
        "endTime": end_timestamp_ms
    }
    test_api_call("/thirdData/getDevHistoryKpi", payload1)
    
    # Test variation 2: Try with single device
    if device_ids:
        print("\n--- TEST 2: Single device test ---")
        payload2 = {
            "devIds": device_ids[0],
            "devTypeId": device_type_id,
            "startTime": start_timestamp_ms,
            "endTime": end_timestamp_ms
        }
        test_api_call("/thirdData/getDevHistoryKpi", payload2)
    
    # Test variation 3: Try with numeric plant code (without NE= prefix)
    print("\n--- TEST 3: Numeric plant code ---")
    numeric_plant_code = plant_code.split('=')[-1] if '=' in plant_code else plant_code
    payload3 = {
        "devIds": ",".join(device_ids),
        "devTypeId": device_type_id,
        "startTime": start_timestamp_ms,
        "endTime": end_timestamp_ms,
        "plantCode": numeric_plant_code
    }
    test_api_call("/thirdData/getDevHistoryKpi", payload3)
    
    # Test variation 4: Try with SNs instead of devIds
    print("\n--- TEST 4: Using SNs instead of devIds ---")
    payload4 = {
        "sns": ",".join(device_ids),  # Use same IDs but as SNs parameter
        "devTypeId": device_type_id,
        "startTime": start_timestamp_ms,
        "endTime": end_timestamp_ms
    }
    test_api_call("/thirdData/getDevHistoryKpi", payload4)
    
    # Test variation 5: Try with additional parameters
    print("\n--- TEST 5: Additional parameters ---")
    payload5 = {
        "devIds": ",".join(device_ids),
        "devTypeId": device_type_id,
        "startTime": start_timestamp_ms,
        "endTime": end_timestamp_ms,
        "collectTime": start_timestamp_ms,
        "currentTime": current_timestamp_ms,
        "timeZone": 7,  # GMT+7 for Indonesia
        "timeFormat": "0",
        "dateFormat": "yyyy-MM-dd"
    }
    test_api_call("/thirdData/getDevHistoryKpi", payload5)
    
    # Test variation 6: Try with a shorter date range (just one day)
    print("\n--- TEST 6: Shorter date range (one day) ---")
    one_day_end = start_date.replace(hour=23, minute=59, second=59)
    one_day_end_ms = int(one_day_end.timestamp() * 1000)
    payload6 = {
        "devIds": ",".join(device_ids),
        "devTypeId": device_type_id,
        "startTime": start_timestamp_ms,
        "endTime": one_day_end_ms
    }
    test_api_call("/thirdData/getDevHistoryKpi", payload6)
    
    # Test variation 7: Try a different endpoint
    print("\n--- TEST 7: Alternate endpoint ---")
    test_api_call("/thirdData/getHistoryData", payload1)

def test_api_call(endpoint, payload):
    """Make an API call and print the results."""
    print(f"Testing endpoint: {endpoint}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    response = _make_api_request(endpoint, payload)
    
    if response:
        print(f"Success! Response received with {len(json.dumps(response))} characters")
        # Check if data exists
        if isinstance(response, dict):
            if 'data' in response:
                if isinstance(response['data'], list):
                    print(f"Data is a list with {len(response['data'])} items")
                    if response['data']:
                        print(f"Sample data item: {json.dumps(response['data'][0], indent=2)}")
                elif isinstance(response['data'], dict) and 'list' in response['data']:
                    print(f"Data is a dictionary with a list containing {len(response['data']['list'])} items")
                    if response['data']['list']:
                        print(f"Sample data item: {json.dumps(response['data']['list'][0], indent=2)}")
                else:
                    print(f"Data format: {type(response['data'])}")
            else:
                print("No 'data' field in response")
    else:
        print("Failed to get response")
    
    print("-" * 40)

def get_devices_for_plant(plant_code, device_type_id):
    """Get devices for a specific plant and device type."""
    from sqlalchemy import text, create_engine
    from sqlalchemy.orm import sessionmaker
    from fusionsolar_harvester_src.fusionsolar_db_operations import engine
    
    if not engine:
        return []
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Extract just the numeric part if it's in the format "NE=12345"
        numeric_part = plant_code.split('=')[-1] if '=' in plant_code else plant_code
        
        query = text("""
            SELECT * FROM fusionsolar_devices 
            WHERE (plant_code = :plant_code OR plant_code = :alt_plant_code)
            AND dev_type_id = :dev_type_id
        """)
        
        result = session.execute(query, {
            'plant_code': plant_code,
            'alt_plant_code': f"NE={numeric_part}" if not plant_code.startswith("NE=") else numeric_part,
            'dev_type_id': device_type_id
        })
        
        return [dict(row._mapping) for row in result]
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Test FusionSolar API variations")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--plant-code", required=True, help="Plant code (e.g., NE=50488260)")
    parser.add_argument("--device-type", type=int, default=1, help="Device type ID (default: 1 for inverter)")
    
    args = parser.parse_args()
    
    test_api_variations(args.start_date, args.end_date, args.plant_code, args.device_type)

if __name__ == "__main__":
    main()
