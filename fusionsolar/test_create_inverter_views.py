#!/usr/bin/env python
"""
Test script to create specialized inverter views for FusionSolar data.
This script focuses only on creating inverter summary and MPPT views.
"""

import logging
import argparse
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

# Construct DATABASE_URL from individual PostgreSQL settings
import urllib.parse
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_database():
    """Initialize database connection."""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Test connection
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def escape_sql(text_value):
    """Escape special characters in SQL strings."""
    if not text_value:
        return text_value
    return text_value.replace("'", "''")

def create_inverter_views(site_code=None):
    """Create specialized inverter summary and MPPT views for the given site."""
    logger.info(f"Creating inverter views for site {site_code}")
    
    engine = init_database()
    if not engine:
        logger.error("Database initialization failed")
        return False
    
    with engine.connect() as conn:
        try:
            # First, get all inverter devices for this site
            device_query = text("""
                SELECT d.dev_id, d.dev_name, d.dev_type_id
                FROM fusionsolar_devices d
                WHERE d.plant_code = :site_code AND d.dev_type_id = 1
                ORDER BY d.dev_name
            """)
            
            result = conn.execute(device_query, {"site_code": site_code})
            devices = [dict(row._mapping) for row in result]
            
            if not devices:
                logger.warning(f"No inverter devices found for site {site_code}")
                return False
            
            logger.info(f"Found {len(devices)} inverter devices for site {site_code}")
            
            # Get device IDs for the IN clause
            device_ids = [f"'{d['dev_id']}'" for d in devices]
            device_ids_str = ", ".join(device_ids)
            
            # Get site name for view naming
            site_name_query = text("""
                SELECT plant_name FROM fusionsolar_plants 
                WHERE plant_code = :site_code
            """)
            
            try:
                result = conn.execute(site_name_query, {"site_code": site_code})
                site_name = result.scalar()
                if not site_name:
                    site_name = site_code
            except Exception as e:
                logger.error(f"Error getting site name: {e}")
                site_name = site_code
            
            logger.info(f"Using site name: {site_name}")
            
            # Create a normalized site name for view naming
            clean_name = ''
            for char in site_name:
                if char.isalnum():
                    clean_name += char.lower()
                else:
                    clean_name += '_'
            
            # Ensure name starts with letter
            if clean_name and clean_name[0].isdigit():
                clean_name = 'p_' + clean_name
                
            # Replace double underscores with single
            while '__' in clean_name:
                clean_name = clean_name.replace('__', '_')
                
            # Remove trailing underscores
            clean_name = clean_name.rstrip('_')
            
            logger.info(f"Using clean name for views: {clean_name}")
            
            # 1. Create inverter summary view
            summary_view_name = f"{clean_name}_inverter_summary"
            
            # Define the summary points we want with proper JSON extraction
            summary_points = [
                "h.measurement_data->>'day_cap' as daily_energy_yield",
                "h.measurement_data->>'active_power' as active_power",
                "h.measurement_data->>'reactive_power' as output_reactive_power",
                "h.measurement_data->>'mppt_total_cap' as total_dc_input_energy"
            ]
            
            # Create SQL for the summary view
            summary_view_sql = f"""
            DROP VIEW IF EXISTS "{summary_view_name}" CASCADE;
            
            CREATE OR REPLACE VIEW "{summary_view_name}" AS
            SELECT
                h.collect_time as timestamp,
                d.dev_name as device_name,
                {', '.join(summary_points)}
            FROM 
                fusionsolar_historical_data h
            JOIN 
                fusionsolar_devices d ON h.dev_id = d.dev_id
            WHERE 
                h.dev_id IN ({device_ids_str})
            ORDER BY 
                h.collect_time;
            """
            
            # Execute the summary view creation
            try:
                conn.execute(text(summary_view_sql))
                logger.info(f"Successfully created inverter summary view: {summary_view_name}")
                
                # Verify the view exists
                verify_query = text(f'SELECT COUNT(*) FROM "{summary_view_name}"')
                count = conn.execute(verify_query).scalar()
                logger.info(f"View {summary_view_name} contains {count} rows")
            except Exception as e:
                logger.error(f"Error creating summary view: {e}")
                logger.error(f"SQL: {summary_view_sql}")
            
            # 2. Create MPPT view
            mppt_view_name = f"{clean_name}_inverter_mppt"
            
            # Define the MPPT points we want with proper JSON extraction
            mppt_points = [
                "h.measurement_data->>'mppt_1_cap' as mppt1_dc_total_yield",
                "h.measurement_data->>'mppt_2_cap' as mppt2_dc_total_yield",
                "h.measurement_data->>'mppt_3_cap' as mppt3_dc_total_yield",
                "h.measurement_data->>'mppt_4_cap' as mppt4_dc_total_yield",
                "h.measurement_data->>'mppt_5_cap' as mppt5_dc_total_yield",
                "h.measurement_data->>'mppt_6_cap' as mppt6_dc_total_yield",
                "h.measurement_data->>'mppt_7_cap' as mppt7_dc_total_yield",
                "h.measurement_data->>'mppt_8_cap' as mppt8_dc_total_yield",
                "h.measurement_data->>'mppt_9_cap' as mppt9_dc_total_yield",
                "h.measurement_data->>'mppt_10_cap' as mppt10_dc_total_yield"
            ]
            
            # Create SQL for the MPPT view
            mppt_view_sql = f"""
            DROP VIEW IF EXISTS "{mppt_view_name}" CASCADE;
            
            CREATE OR REPLACE VIEW "{mppt_view_name}" AS
            SELECT
                h.collect_time as timestamp,
                d.dev_name as device_name,
                {', '.join(mppt_points)}
            FROM 
                fusionsolar_historical_data h
            JOIN 
                fusionsolar_devices d ON h.dev_id = d.dev_id
            WHERE 
                h.dev_id IN ({device_ids_str})
            ORDER BY 
                h.collect_time;
            """
            
            # Execute the MPPT view creation
            try:
                conn.execute(text(mppt_view_sql))
                logger.info(f"Successfully created MPPT view: {mppt_view_name}")
                
                # Verify the view exists
                verify_query = text(f'SELECT COUNT(*) FROM "{mppt_view_name}"')
                count = conn.execute(verify_query).scalar()
                logger.info(f"View {mppt_view_name} contains {count} rows")
            except Exception as e:
                logger.error(f"Error creating MPPT view: {e}")
                logger.error(f"SQL: {mppt_view_sql}")
            
            logger.info("Views creation completed")
            return True
            
        except Exception as e:
            logger.error(f"Error creating inverter views: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description="Create specialized inverter views for FusionSolar data")
    parser.add_argument("--site", default="NE=50488260", help="Site code to create views for")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug logging enabled")
    
    create_inverter_views(args.site)

if __name__ == "__main__":
    main()
