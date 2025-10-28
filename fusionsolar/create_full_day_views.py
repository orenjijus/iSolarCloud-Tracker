import logging
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime, timedelta

# Import database config from existing module
from fusionsolar_harvester_src.fusionsolar_config import DATABASE_URL, DEVICE_TYPES

def init_database():
    """Initialize database connection."""
    try:
        # Use autocommit to ensure DDL statements are committed
        engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")
        Session = sessionmaker(bind=engine)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print(f"Database connection test: {result.scalar() == 1}")
            
        return engine, Session
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        return None, None

def create_full_day_views(site_code, start_date=None, end_date=None, friendly_name=None):
    """Create SQL views for full day data (00:00-23:55) for the specified site.
    
    Args:
        site_code: Site code (e.g., 'NE=50488260')
        start_date: Optional start date for the view data
        end_date: Optional end date for the view data
        friendly_name: Optional friendly name to use in view name instead of site code
    """
    engine, Session = init_database()
    if not engine:
        print("Database initialization failed")
        return False
    
    # Use provided friendly name or generate one from the site code
    if friendly_name:
        plant_name = friendly_name
    else:
        # Just use the site code but make it more readable
        if site_code.startswith("NE="):
            # For NE=50488260, convert to MMKI_phase_1
            if site_code == "NE=50488260":
                plant_name = "MMKI_phase_1"
            else:
                # Generic transformation
                plant_name = site_code.replace("=", "_")
        else:
            plant_name = site_code
    
    print(f"Creating full day views for site: {site_code} with name: {plant_name}")
    
    # Get all devices for the site
    with engine.connect() as conn:
        # Query to get devices by site/plant code
        query = text("""
            SELECT d.dev_id, d.dev_name, d.dev_type_id, d.plant_code
            FROM fusionsolar_devices d
            WHERE d.plant_code = :site_code
            ORDER BY d.dev_type_id, d.dev_name
        """)
        
        result = conn.execute(query, {"site_code": site_code})
        devices = [dict(row._mapping) for row in result]
        
        if not devices:
            print(f"No devices found for site: {site_code}")
            return False
        
        print(f"Found {len(devices)} devices for site {site_code}")
        
        # Group devices by type
        devices_by_type = {}
        for device in devices:
            dev_type_id = device['dev_type_id']
            if dev_type_id not in devices_by_type:
                devices_by_type[dev_type_id] = []
            devices_by_type[dev_type_id].append(device)
        
        # Create views for each device type
        for dev_type_id, devices_of_type in devices_by_type.items():
            # Find device type name
            device_type_name = "unknown"
            for name, type_id in DEVICE_TYPES.items():
                if type_id == dev_type_id:
                    device_type_name = name
                    break
            
            # Create view name: clean plant name + device type
            clean_plant_name = plant_name.replace(" ", "_").replace("-", "_").replace(".", "_").replace("&", "and")
            # Make the name lowercase and replace any other special characters
            clean_plant_name = ''.join(c.lower() if c.isalnum() or c == '_' else '_' for c in clean_plant_name)
            view_name = f"{clean_plant_name}_{device_type_name}_full_day_data"
            
            # Get device IDs for the IN clause
            device_ids = [f"'{d['dev_id']}'" for d in devices_of_type]
            device_ids_str = ", ".join(device_ids)
            
            # Create the view with full day timestamps - proper quoting of identifiers
            view_sql = f"""
            DROP VIEW IF EXISTS \"{view_name}\";
            
            CREATE OR REPLACE VIEW \"{view_name}\" AS
            WITH time_series AS (
                SELECT 
                    generate_series(
                        COALESCE((SELECT MIN(date_trunc('day', collect_time)::timestamp) 
                                FROM fusionsolar_historical_data 
                                WHERE dev_id IN ({device_ids_str})),
                                '2025-05-01'::timestamp), 
                        COALESCE((SELECT MAX(date_trunc('day', collect_time)::timestamp) 
                                FROM fusionsolar_historical_data 
                                WHERE dev_id IN ({device_ids_str})),
                                '2025-05-04'::timestamp) + INTERVAL '23 hours 55 minutes',
                        INTERVAL '5 minutes'
                    ) AS timestamp
            ),
            base_data AS (
                SELECT 
                    dev_id,
                    collect_time,
                    measurement_data
                FROM fusionsolar_historical_data
                WHERE dev_id IN ({device_ids_str})
            )
            SELECT 
                ts.timestamp AS timestamp,
                d.dev_id,
                d.collect_time,
                d.measurement_data
            FROM time_series ts
            LEFT JOIN base_data d ON 
                date_trunc('minute', ts.timestamp) = date_trunc('minute', d.collect_time)
            ORDER BY ts.timestamp, d.dev_id;
            """
            
            try:
                # Execute the view creation and print the SQL for debugging
                print(f"\nExecuting SQL for {view_name}:")
                print(view_sql)
                
                # Execute with explicit commit
                conn.execute(text(view_sql))
                print(f"Created view: {view_name}")
                
                # Verify view exists - account for case sensitivity
                verify_query = text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.views 
                        WHERE table_schema = 'public' AND table_name ILIKE :view_name
                    )
                """)
                exists = conn.execute(verify_query, {"view_name": view_name}).scalar()
                print(f"View existence check: {exists}")
                
                if exists:
                    # Check if view has data - use quotes to handle case sensitivity
                    count_query = text(f'SELECT COUNT(*) FROM "{view_name}"')
                    count = conn.execute(count_query).scalar()
                    print(f"View {view_name} contains {count} rows")
                    
                    # Get column info
                    cols_query = text("""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_schema = 'public' AND table_name ILIKE :view_name
                    """)
                    cols = [row[0] for row in conn.execute(cols_query, {"view_name": view_name})]
                    print(f"View columns: {cols}")
                else:
                    print(f"WARNING: View {view_name} does not exist after creation attempt")
                
            except Exception as e:
                print(f"Error creating view {view_name}: {e}")
                print(f"Full SQL that failed:\n{view_sql}")

def main():
    parser = argparse.ArgumentParser(description="Create full day (00:00-23:55) SQL views for FusionSolar data")
    parser.add_argument("--site", required=True, help="Site code (e.g., NE=50488260)")
    parser.add_argument("--name", help="Friendly name to use in view (e.g., 'MMKI_phase_1')")
    parser.add_argument("--debug", action="store_true", help="Print detailed debug information")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Try to query the database directly first to check connection
    engine, _ = init_database()
    if engine:
        with engine.connect() as conn:
            try:
                # Get a list of existing tables
                tables = conn.execute(text("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)).fetchall()
                print(f"Database connection successful. Found {len(tables)} tables.")
                
                # Count data in historical table
                count = conn.execute(text("SELECT COUNT(*) FROM fusionsolar_historical_data")).scalar()
                print(f"Found {count} records in fusionsolar_historical_data")
            except Exception as e:
                print(f"Database query test failed: {e}")
    
    create_full_day_views(args.site, friendly_name=args.name)

if __name__ == "__main__":
    main()
