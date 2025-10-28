import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from .env file
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DB_PARAMS with the loaded credentials
DB_PARAMS = {
    'host': POSTGRES_HOST,
    'port': POSTGRES_PORT,
    'dbname': POSTGRES_DB,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD
}

def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def list_views():
    """List views in the database matching specific patterns."""
    conn = connect_to_db()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            # List all views
            cur.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public'
            """)
            
            all_views = [row[0] for row in cur.fetchall()]
            print(f"Found {len(all_views)} total views in the database")
            
            # Check for inverter summary views (non-pivoted)
            summary_views = [v for v in all_views if 'inverter_summary' in v and 'pivoted' not in v]
            print(f"\nFound {len(summary_views)} inverter summary views (non-pivoted):")
            for view in summary_views:
                print(f"  - {view}")
                
                # Get row count for each view
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{view}"')
                    count = cur.fetchone()[0]
                    print(f"    - Contains {count} rows")
                    
                    # Check the first few rows
                    cur.execute(f'SELECT * FROM "{view}" LIMIT 3')
                    sample_rows = cur.fetchall()
                    if sample_rows:
                        print(f"    - Sample data available: Yes")
                    else:
                        print(f"    - Sample data available: No")
                except Exception as e:
                    print(f"    - Error accessing view: {e}")
            
            # Check for pivoted inverter summary views
            pivoted_summary_views = [v for v in all_views if 'inverter_summary_pivoted' in v]
            print(f"\nFound {len(pivoted_summary_views)} inverter summary views (pivoted):")
            for view in pivoted_summary_views:
                print(f"  - {view}")
                
                # Get row count for each view
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{view}"')
                    count = cur.fetchone()[0]
                    print(f"    - Contains {count} rows")
                    
                    # Get column count to verify pivoting worked
                    cur.execute(f"""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_name = '{view}' AND table_schema = 'public'
                    """)
                    col_count = cur.fetchone()[0]
                    print(f"    - Contains {col_count} columns")
                    
                    # Check the first few rows
                    cur.execute(f'SELECT * FROM "{view}" LIMIT 3')
                    sample_rows = cur.fetchall()
                    if sample_rows:
                        print(f"    - Sample data available: Yes")
                    else:
                        print(f"    - Sample data available: No")
                except Exception as e:
                    print(f"    - Error accessing view: {e}")
            
            # Check for standard MPPT views (non-pivoted)
            mppt_views = [v for v in all_views if 'inverter_mppt' in v and 'pivoted' not in v]
            print(f"\nFound {len(mppt_views)} inverter MPPT views (non-pivoted):")
            for view in mppt_views:
                print(f"  - {view}")
                
                # Get row count for each view
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{view}"')
                    count = cur.fetchone()[0]
                    print(f"    - Contains {count} rows")
                    
                    # Check the first few rows
                    cur.execute(f'SELECT * FROM "{view}" LIMIT 3')
                    sample_rows = cur.fetchall()
                    if sample_rows:
                        print(f"    - Sample data available: Yes")
                    else:
                        print(f"    - Sample data available: No")
                except Exception as e:
                    print(f"    - Error accessing view: {e}")
                    
            # Check for pivoted MPPT views
            pivoted_mppt_views = [v for v in all_views if 'inverter_mppt_pivoted' in v]
            print(f"\nFound {len(pivoted_mppt_views)} inverter MPPT views (pivoted):")
            for view in pivoted_mppt_views:
                print(f"  - {view}")
                
                # Get row count for each view
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{view}"')
                    count = cur.fetchone()[0]
                    print(f"    - Contains {count} rows")
                    
                    # Get column count to verify pivoting worked
                    cur.execute(f"""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_name = '{view}' AND table_schema = 'public'
                    """)
                    col_count = cur.fetchone()[0]
                    print(f"    - Contains {col_count} columns")
                    
                    # Check the first few rows
                    cur.execute(f'SELECT * FROM "{view}" LIMIT 3')
                    sample_rows = cur.fetchall()
                    if sample_rows:
                        print(f"    - Sample data available: Yes")
                    else:
                        print(f"    - Sample data available: No")
                except Exception as e:
                    print(f"    - Error accessing view: {e}")
                    
            # List standard full_day views for comparison
            full_day_views = [v for v in all_views if 'full_day' in v and 'inverter_summary' not in v and 'inverter_mppt' not in v]
            print(f"\nFound {len(full_day_views)} other full_day views:")
            for view in full_day_views[:5]:  # Limit to 5 to avoid too much output
                print(f"  - {view}")
    except Exception as e:
        print(f"Error listing views: {e}")
    finally:
        if conn:
            conn.close()

def show_columns():
    """Show column names from the measurement_data JSON in an inverter record."""
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get a sample of inverter data to check columns
            cur.execute("""
                SELECT h.measurement_data
                FROM fusionsolar_historical_data h
                JOIN fusionsolar_devices d ON h.dev_id = d.dev_id
                WHERE d.dev_type_id = 1
                  AND h.measurement_data IS NOT NULL
                  AND h.measurement_data != '{}'
                LIMIT 1
            """)
            
            row = cur.fetchone()
            if row and row['measurement_data']:
                print("\nMeasurement data columns found in inverter data:")
                for key in row['measurement_data'].keys():
                    print(f"  - {key}")
            else:
                print("No valid measurement data found")
    except Exception as e:
        print(f"Error checking columns: {e}")
    finally:
        if conn:
            conn.close()

def check_device_data():
    """Check the device data in the database to understand what's available."""
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check for plant with code NE=50488260
            cur.execute("""
                SELECT * FROM fusionsolar_plants WHERE plant_code = 'NE=50488260'
            """)
            plant = cur.fetchone()
            if plant:
                print(f"Found plant: {plant['plant_name']} ({plant['plant_code']})")
            else:
                print("Plant NE=50488260 not found")
            
            # Check for inverter devices in this plant
            cur.execute("""
                SELECT * FROM fusionsolar_devices 
                WHERE plant_code = 'NE=50488260' AND dev_type_id = 1
            """)
            inverters = cur.fetchall()
            print(f"\nFound {len(inverters)} inverter devices:")
            for inv in inverters[:3]:  # Show only first 3
                print(f"  - {inv['dev_name']} (ID: {inv['dev_id']})")
            
            # Check for measurement data for these inverters
            if inverters:
                inv_id = inverters[0]['dev_id']
                cur.execute(f"""
                    SELECT collect_time, measurement_data 
                    FROM fusionsolar_historical_data 
                    WHERE dev_id = '{inv_id}'
                    LIMIT 5
                """)
                measurements = cur.fetchall()
                print(f"\nFound {len(measurements)} measurement records for inverter {inv_id}:")
                if measurements:
                    sample = measurements[0]
                    print(f"  - Time: {sample['collect_time']}")
                    print(f"  - Available keys in measurement_data: {list(sample['measurement_data'].keys() if sample['measurement_data'] else [])}")
    except Exception as e:
        print(f"Error checking device data: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    list_views()
    print("\n" + "-"*50 + "\n")
    show_columns()
    print("\n" + "-"*50 + "\n")
    check_device_data()
