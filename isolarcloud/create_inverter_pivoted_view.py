import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL
import urllib.parse
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def create_inverter_pivoted_view():
    """Create the pivoted view for inverter data with proper grouping by measurement type."""
    try:
        # Create database connection
        engine = create_engine(DATABASE_URL)
        
        # Connect to the database
        with engine.connect() as conn:
            # Drop existing view if it exists
            conn.execute(text("DROP VIEW IF EXISTS Garuda_Metalindo_IKP_inverter_pivoted;"))
            
            # Define voltage points and current points
            voltage_points = ["p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", 
                             "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                             "p7166", "p7167", "p7168", "p7169", "p7170", "p7171"]
            
            current_points = ["p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                             "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85",
                             "p92", "p93", "p313", "p314", "p315", "p316", "p317", "p318"]
            
            # Get device IDs and names for inverters
            devices_query = text("""
                SELECT device_ps_key, device_name
                FROM isolarcloud_devices d
                JOIN isolarcloud_power_stations ps ON d.ps_id = ps.ps_id
                WHERE ps.ps_name = 'Garuda Metalindo (IKP)' AND d.device_type = 1
                ORDER BY device_name;
            """)
            
            devices_result = conn.execute(devices_query)
            devices = [(row[0], row[1]) for row in devices_result]
            
            # Generate column definitions for the pivoted view
            column_defs = ["h.timestamp"]
            
            # First add all voltage columns for all devices
            for device_ps_key, device_name in devices:
                clean_device_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in device_name)
                if clean_device_name and clean_device_name[0].isdigit():
                    clean_device_name = 'Inverter_' + clean_device_name
                
                # Add voltage columns for this device
                for i, point in enumerate(voltage_points):
                    string_num = i + 1
                    column_name = f"{clean_device_name}_string_{string_num}_voltage"
                    column_def = f"""MAX(CASE WHEN h.device_ps_key = '{device_ps_key}' 
                              THEN (h.measurement_data::jsonb->'{point}')::NUMERIC 
                              ELSE NULL END) AS "{column_name}" """
                    column_defs.append(column_def)
            
            # Then add all current columns for all devices
            for device_ps_key, device_name in devices:
                clean_device_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in device_name)
                if clean_device_name and clean_device_name[0].isdigit():
                    clean_device_name = 'Inverter_' + clean_device_name
                
                # Add current columns for this device
                for i, point in enumerate(current_points):
                    string_num = i + 1
                    column_name = f"{clean_device_name}_string_{string_num}_current"
                    column_def = f"""MAX(CASE WHEN h.device_ps_key = '{device_ps_key}' 
                              THEN (h.measurement_data::jsonb->'{point}')::NUMERIC 
                              ELSE NULL END) AS "{column_name}" """
                    column_defs.append(column_def)
            
            # Create the pivoted view SQL
            pivoted_view_sql = f"""
            CREATE OR REPLACE VIEW Garuda_Metalindo_IKP_inverter_pivoted AS
            SELECT 
                {', '.join(column_defs)}
            FROM 
                isolarcloud_historical_data h
            JOIN 
                isolarcloud_devices d ON h.device_ps_key = d.device_ps_key
            JOIN 
                isolarcloud_power_stations ps ON d.ps_id = ps.ps_id
            WHERE 
                ps.ps_name = 'Garuda Metalindo (IKP)' 
                AND d.device_type = 1
            GROUP BY 
                h.timestamp
            ORDER BY
                h.timestamp;
            """
            
            # Execute the SQL to create the pivoted view
            conn.execute(text(pivoted_view_sql))
            logger.info("Created pivoted view for Garuda Metalindo (IKP) inverter data")
            
    except Exception as e:
        logger.error(f"Error creating inverter pivoted view: {e}")

if __name__ == "__main__":
    create_inverter_pivoted_view()
