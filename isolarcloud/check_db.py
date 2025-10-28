from sqlalchemy import create_engine, text
import json
from isolarcloud_harvester_src.isolar_config import DATABASE_URL

# Create database engine
engine = create_engine(DATABASE_URL)

# Connect and execute query
with engine.connect() as conn:
    # Check power stations
    result = conn.execute(text('SELECT COUNT(*) FROM isolarcloud_power_stations'))
    print(f'Number of power stations in database: {result.fetchone()[0]}')
    
    # Get list of power stations
    result = conn.execute(text('SELECT ps_id, ps_name FROM isolarcloud_power_stations LIMIT 5'))
    print("\nFirst 5 power stations:")
    for row in result:
        print(f"ID: {row[0]}, Name: {row[1]}")
    
    # Check number of devices
    result = conn.execute(text('SELECT COUNT(*) FROM isolarcloud_devices'))
    print(f'\nNumber of devices in database: {result.fetchone()[0]}')
    
    # Get list of device types with safer encoding handling
    result = conn.execute(text('''
        SELECT device_type, COUNT(*) as count
        FROM isolarcloud_devices
        GROUP BY device_type
        ORDER BY count DESC
    '''))
    print("\nDevice types and counts:")
    for row in result:
        try:
            print(f"Device Type ID: {row[0]}, Count: {row[1]}")
        except UnicodeEncodeError:
            print(f"Device Type ID: [encoding error], Count: {row[1]}")
    
    # Get some sample devices with safer encoding handling
    result = conn.execute(text('''
        SELECT d.device_ps_key, d.device_type, d.device_sn, p.ps_id
        FROM isolarcloud_devices d
        JOIN isolarcloud_power_stations p ON d.ps_id = p.ps_id
        LIMIT 10
    '''))
    print("\nSample devices:")
    for row in result:
        try:
            print(f"Device Key: {row[0]}, Type ID: {row[1]}, SN: {row[2]}, Power Station ID: {row[3]}")
        except UnicodeEncodeError:
            print(f"Device Key: {row[0]}, Type ID: {row[1]}, SN: [encoding error], Power Station ID: {row[3]}")
    
    # Check historical data
    result = conn.execute(text('SELECT COUNT(*) FROM isolarcloud_historical_data'))
    print(f'\nNumber of historical data points in database: {result.fetchone()[0]}')
    
    # Get sample historical data points
    result = conn.execute(text('''
        SELECT device_ps_key, timestamp, measurement_data
        FROM isolarcloud_historical_data
        ORDER BY timestamp DESC
        LIMIT 5
    '''))
    print("\nSample historical data points (most recent first):")
    for row in result:
        try:
            # The measurement_data is already a Python dict or object when returned by SQLAlchemy
            measurement_data = row[2] if row[2] else {}
            # Show just a few measurements (up to 3) to avoid overwhelming output
            if isinstance(measurement_data, dict):
                sample_measurements = list(measurement_data.items())[:3]
            else:
                # Handle the case where it might be a string (if not auto-deserialized)
                try:
                    if isinstance(measurement_data, str):
                        measurement_data = json.loads(measurement_data)
                        sample_measurements = list(measurement_data.items())[:3]
                    else:
                        sample_measurements = [("data_type", type(measurement_data).__name__)]
                except Exception as e:
                    sample_measurements = [("error", str(e))]
            print(f"Device: {row[0]}, Time: {row[1]}")
            print(f"  Sample measurements: {sample_measurements}")
        except (UnicodeEncodeError, json.JSONDecodeError) as e:
            print(f"Device: {row[0]}, Time: {row[1]}, Error parsing data: {type(e).__name__}")

