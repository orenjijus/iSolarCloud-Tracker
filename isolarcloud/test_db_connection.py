import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Construct DATABASE_URL from individual PostgreSQL settings
import urllib.parse
# URL encode the password to handle special characters
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
logger.info(f"Database URL constructed: postgresql://{POSTGRES_USER}:***@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

def test_connection():
    try:
        # Create database connection
        engine = create_engine(DATABASE_URL)
        
        # Connect to the database
        with engine.connect() as conn:
            # Test a simple query
            test_query = text("SELECT 1 as test;")
            result = conn.execute(test_query)
            logger.info(f"Connection test result: {result.fetchone()[0]}")
            
            # Check if the required tables exist
            check_tables_query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name IN ('isolarcloud_historical_data', 'isolarcloud_devices', 'isolarcloud_power_stations');
            """)
            result = conn.execute(check_tables_query)
            tables = [row[0] for row in result]
            logger.info(f"Found tables: {tables}")
            
            # Try to create a simple view
            try:
                test_view_query = text("""
                    CREATE OR REPLACE VIEW test_view AS
                    SELECT h.timestamp, d.device_name
                    FROM isolarcloud_historical_data h
                    JOIN isolarcloud_devices d ON h.device_ps_key = d.device_ps_key
                    LIMIT 10;
                """)
                conn.execute(test_view_query)
                logger.info("Test view created successfully")
            except Exception as e:
                logger.error(f"Error creating test view: {e}")
            
            # Test a simple JSON field access
            try:
                test_json_query = text("""
                    SELECT 
                        (h.measurement_data::jsonb->>'p96')::NUMERIC as test_value
                    FROM 
                        isolarcloud_historical_data h
                    LIMIT 1;
                """)
                result = conn.execute(test_json_query)
                logger.info(f"JSON field access test result: {result.fetchone()}")
            except Exception as e:
                logger.error(f"Error testing JSON field access: {e}")
                
            logger.info("Database connection test completed")
    except Exception as e:
        logger.error(f"Database connection error: {e}")

if __name__ == "__main__":
    test_connection()
