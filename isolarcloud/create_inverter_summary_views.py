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
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")  # Use MMSR as default since that's what the test showed
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL from individual PostgreSQL settings
import urllib.parse
# URL encode the password to handle special characters
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Inverter summary points definition
INVERTER_SUMMARY_POINTS = ["p1", "p14", "p24", "p25"]  # Yield Today, Total DC Power, Total Active Power, Total Reactive Power

# Point descriptions for better column naming
POINT_DESCRIPTIONS = {
    # Inverter summary points
    "p1": "yield_today_kWh",
    "p14": "total_dc_power_W",
    "p24": "total_active_power_W",
    "p25": "total_reactive_power_var",
}

# Site names in iSolarCloud
ISOLARCLOUD_SITES = [
    "Garuda Metalindo (IKP)",
    "Garuda Metalindo (MPF)",
    "Garuda Metalindo 1",
    "Garuda Metalindo 2",
    "Shoetown Ligung Indonesia"
]

def escape_sql(text):
    """Escape special characters in SQL strings."""
    if text is None:
        return "NULL"
    return text.replace("'", "''")

def create_inverter_summary_views(sites=None, drop_existing=False, debug=False):
    """
    Create SQL views for inverter summary data.
    
    Args:
        sites (list): List of sites to create views for. If None, create for all sites.
        drop_existing (bool): Whether to drop existing inverter summary views before creating new ones.
        debug (bool): Enable debug logging.
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        
    if sites is None:
        sites = ISOLARCLOUD_SITES
    
    logger.info(f"Creating inverter summary views for sites: {', '.join(sites)}")
    
    try:
        # Create database connection
        engine = create_engine(DATABASE_URL)
        
        # Connect to the database
        with engine.connect() as conn:
            # Drop existing inverter summary views if requested
            if drop_existing:
                logger.info("Dropping existing inverter summary views...")
                for site_name in sites:
                    clean_site_name = site_name.replace(' ', '_').replace('(', '').replace(')', '')
                    summary_view_name = f"{clean_site_name}_inverter_summary_data"
                    pivoted_summary_view_name = f"{clean_site_name}_inverter_summary_pivoted"
                    
                    # Drop views if they exist
                    drop_summary_sql = f"DROP VIEW IF EXISTS {summary_view_name};"
                    drop_pivoted_sql = f"DROP VIEW IF EXISTS {pivoted_summary_view_name};"
                    
                    conn.execute(text(drop_summary_sql))
                    conn.execute(text(drop_pivoted_sql))
                    logger.info(f"Dropped views for {site_name} if they existed")
            
            # Create inverter summary views for each site
            for site_name in sites:
                # Clean site name for view naming
                clean_site_name = site_name.replace(' ', '_').replace('(', '').replace(')', '')
                escaped_site_name = escape_sql(site_name)
                device_type_id = 1  # Inverter device type
                
                # Create inverter_summary regular view
                summary_view_name = f"{clean_site_name}_inverter_summary_data"
                
                # Generate the SQL for extracting just the summary measurement points
                point_extractions = []
                for point in INVERTER_SUMMARY_POINTS:
                    description = POINT_DESCRIPTIONS.get(point, point.replace("p", "point_"))
                    point_extractions.append(f"CASE WHEN (h.measurement_data::jsonb->>{repr(point)}) ~ '^-?\\d+(\\.\\d+)?$' THEN (h.measurement_data::jsonb->>{repr(point)})::NUMERIC ELSE NULL END AS \"{description}\"")
                
                # Create the SQL for inverter_summary regular view
                summary_view_sql = f"""
                CREATE OR REPLACE VIEW {summary_view_name} AS
                SELECT 
                    h.timestamp,
                    d.device_name,
                    {', '.join(point_extractions)}
                FROM 
                    isolarcloud_historical_data h
                JOIN 
                    isolarcloud_devices d ON h.device_ps_key = d.device_ps_key
                JOIN 
                    isolarcloud_power_stations ps ON d.ps_id = ps.ps_id
                WHERE 
                    ps.ps_name = '{escaped_site_name}' 
                    AND d.device_type = {device_type_id};
                """
                
                # Execute the SQL to create the regular summary view
                conn.execute(text(summary_view_sql))
                logger.info(f"Created view {summary_view_name} for {site_name}")
                
                # Create pivoted summary view
                pivoted_summary_view_name = f"{clean_site_name}_inverter_summary_pivoted"
                
                # Get all inverters for this site
                query_devices = text(f"""
                    SELECT device_ps_key, device_name
                    FROM isolarcloud_devices d
                    JOIN isolarcloud_power_stations ps ON d.ps_id = ps.ps_id
                    WHERE ps.ps_name = '{escaped_site_name}' AND d.device_type = {device_type_id}
                    ORDER BY device_name;
                """)
                
                devices_result = conn.execute(query_devices)
                devices_list = [(row[0], row[1]) for row in devices_result]
                
                if not devices_list:
                    logger.warning(f"No inverter devices found for {site_name}. Skipping pivot view.")
                    continue
                
                logger.info(f"Found {len(devices_list)} inverter devices for {site_name}")
                
                # Generate pivoted column definitions for summary points
                summary_column_defs = []
                used_summary_columns = set()
                
                for device_ps_key, device_name in devices_list:
                    # Clean device name for column names
                    clean_device_name = ''
                    for char in device_name:
                        if char.isalnum():
                            clean_device_name += char
                        else:
                            clean_device_name += '_'
                            
                    if clean_device_name and clean_device_name[0].isdigit():
                        clean_device_name = 'dev_' + clean_device_name
                        
                    # Add columns for each summary point
                    for point in INVERTER_SUMMARY_POINTS:
                        point_desc = POINT_DESCRIPTIONS.get(point, point.replace("p", "point_"))
                        column_name = f"{clean_device_name}_{point_desc}"
                        
                        if column_name in used_summary_columns:
                            continue
                            
                        used_summary_columns.add(column_name)
                        column_def = f"""
                        MAX(CASE WHEN h.device_ps_key = {repr(device_ps_key)} 
                            THEN CASE WHEN (h.measurement_data::jsonb->>{repr(point)}) ~ '^-?\\d+(\\.\\d+)?$' THEN (h.measurement_data::jsonb->>{repr(point)})::NUMERIC ELSE NULL END 
                            ELSE NULL END) AS \"{column_name}\"
                        """
                        summary_column_defs.append(column_def)
                
                # Create the pivoted summary view SQL
                pivoted_summary_sql = f"""
                CREATE OR REPLACE VIEW {pivoted_summary_view_name} AS
                SELECT 
                    h.timestamp,
                    {', '.join(summary_column_defs)}
                FROM 
                    isolarcloud_historical_data h
                JOIN 
                    isolarcloud_devices d ON h.device_ps_key = d.device_ps_key
                JOIN 
                    isolarcloud_power_stations ps ON d.ps_id = ps.ps_id
                WHERE 
                    ps.ps_name = '{escaped_site_name}' 
                    AND d.device_type = {device_type_id}
                GROUP BY 
                    h.timestamp
                ORDER BY
                    h.timestamp;
                """
                
                # Execute the SQL to create the pivoted summary view
                conn.execute(text(pivoted_summary_sql))
                logger.info(f"Created pivoted view {pivoted_summary_view_name} for {site_name}")
            
            conn.commit()
            logger.info("All inverter summary views created successfully")
            
    except Exception as e:
        logger.error(f"Error creating inverter summary views: {e}")
        return False
        
    return True

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Create inverter summary views for iSolarCloud sites.')
    
    # Site selection parameters
    site_group = parser.add_argument_group('Site Selection')
    site_group.add_argument('--sites', type=str, default=None,
                        help='Comma-separated list of site names to create views for. If not specified, all sites will be processed.')
    
    # Advanced options
    advanced_group = parser.add_argument_group('Advanced Options')
    advanced_group.add_argument('--drop-existing', action='store_true',
                        help='Drop existing inverter summary views before creating new ones')
    advanced_group.add_argument('--list-sites', action='store_true',
                        help='List all available sites and exit')
    advanced_group.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    
    return parser.parse_args()

def main():
    """Main function to ensure proper execution order"""
    # Parse command-line arguments
    args = parse_arguments()
    
    # List sites if requested
    if args.list_sites:
        print("Available iSolarCloud sites:")
        for site in ISOLARCLOUD_SITES:
            print(f"  - {site}")
        return 0
    
    # Set up logging based on debug flag
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Debug logging enabled")
    
    # Parse sites parameter
    sites = None
    if args.sites:
        sites = [site.strip() for site in args.sites.split(',')]
        # Validate sites
        invalid_sites = [site for site in sites if site not in ISOLARCLOUD_SITES]
        if invalid_sites:
            logger.error(f"Invalid site names: {', '.join(invalid_sites)}")
            logger.error(f"Available sites: {', '.join(ISOLARCLOUD_SITES)}")
            return 1
    
    # Create inverter summary views
    success = create_inverter_summary_views(
        sites=sites,
        drop_existing=args.drop_existing,
        debug=args.debug
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
