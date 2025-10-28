import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL from individual PostgreSQL settings
import urllib.parse
# URL encode the password to handle special characters
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
logger.info(f"Database URL constructed: postgresql://{POSTGRES_USER}:***@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

def create_simple_view():
    try:
        # Create database connection
        engine = create_engine(DATABASE_URL)
        
        # Connect to the database
        with engine.connect() as conn:
            # Drop the test view if it exists
            conn.execute(text("DROP VIEW IF EXISTS New_view;"))
            
            # Create a simple pivoted view with just a few columns
            query = text("""
            CREATE OR REPLACE VIEW New_view AS
            SELECT 
                h.timestamp,
                MAX(CASE WHEN h.device_ps_key = '1445767_1_1_1' 
                    THEN (h.measurement_data->'p96'->>'value')::text::numeric ELSE NULL END) AS "inverter101_string1_voltage",
                MAX(CASE WHEN h.device_ps_key = '1445767_1_1_1' 
                    THEN (h.measurement_data->'p70'->>'value')::text::numeric ELSE NULL END) AS "inverter101_string1_current",
                MAX(CASE WHEN h.device_ps_key = '1445767_1_2_1' 
                    THEN (h.measurement_data->'p96'->>'value')::text::numeric ELSE NULL END) AS "inverter102_string1_voltage",
                MAX(CASE WHEN h.device_ps_key = '1445767_1_2_1' 
                    THEN (h.measurement_data->'p70'->>'value')::text::numeric ELSE NULL END) AS "inverter102_string1_current"
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
            """)
            
            conn.execute(query)
            # Explicitly commit the transaction
            conn.commit()
            logger.info("Simple pivoted view created successfully with explicit commit")
            
    except Exception as e:
        logger.error(f"Error creating simple view: {e}")

if __name__ == "__main__":
    create_simple_view()
