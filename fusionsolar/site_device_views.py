#!/usr/bin/env python
"""
Create SQL views for FusionSolar device data.

This script creates SQL views for various device types in the FusionSolar database,
including inverters, meters, and meteo stations. It also creates specialized views
for inverter summary data and MPPT data.
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
POSTGRES_DB = os.getenv("POSTGRES_DB", "Monitoring")  # Use Monitoring as default per memory
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL from individual PostgreSQL settings
import urllib.parse
# URL encode the password to handle special characters
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
logging.info(f"Database URL constructed: postgresql://{POSTGRES_USER}:***@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Point descriptions for better column naming
POINT_DESCRIPTIONS = {
    # Inverter points
    "inverter_state": "inverter_state",
    "ab_u": "a_b_line_voltage",
    "bc_u": "b_c_line_voltage",
    "ca_u": "c_a_line_voltage",
    "a_u": "phase_a_voltage",
    "b_u": "phase_b_voltage",
    "c_u": "phase_c_voltage",
    "a_i": "phase_a_current",
    "b_i": "phase_b_current",
    "c_i": "phase_c_current",
    "efficiency": "inverter_efficiency",
    "temperature": "internal_temperature",
    "power_factor": "power_factor",
    "elec_freq": "grid_frequency",
    "active_power": "active_power",
    "reactive_power": "reactive_power",
    "day_cap": "daily_energy_yield",
    "total_cap": "total_energy_yield",
    
    # PV inputs (strings)
    "pv1_u": "string_1_voltage",
    "pv2_u": "string_2_voltage",
    "pv3_u": "string_3_voltage",
    "pv4_u": "string_4_voltage",
    "pv5_u": "string_5_voltage",
    "pv6_u": "string_6_voltage",
    "pv7_u": "string_7_voltage",
    "pv8_u": "string_8_voltage",
    
    "pv1_i": "string_1_current",
    "pv2_i": "string_2_current",
    "pv3_i": "string_3_current",
    "pv4_i": "string_4_current",
    "pv5_i": "string_5_current",
    "pv6_i": "string_6_current",
    "pv7_i": "string_7_current",
    "pv8_i": "string_8_current",
    
    # Meter points
    "active_power": "active_power",
    "reactive_power": "reactive_power",
    "forward_active_energy": "forward_active_energy",
    "reverse_active_energy": "reverse_active_energy",
    "forward_reactive_energy": "forward_reactive_energy",
    "reverse_reactive_energy": "reverse_reactive_energy",
    
    # Meteo station points
    "total_irradiance": "total_irradiance",
    "direct_irradiance": "direct_irradiance",
    "ambient_temperature": "ambient_temperature",
    "module_temperature": "module_temperature",
    "wind_speed": "wind_speed",
    "wind_direction": "wind_direction"
}

def drop_existing_views(conn):
    """Drop all existing FusionSolar data views to avoid conflicts when recreating them."""
    try:
        # Get a list of all views that start with 'fusionsolar_'
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'VIEW' AND table_name LIKE 'fusionsolar_%'
        """))
        
        views_to_drop = [row[0] for row in result]
        
        for view_name in views_to_drop:
            logger.info(f"Dropping view: {view_name}")
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        
        logger.info(f"Dropped {len(views_to_drop)} existing views.")
        return len(views_to_drop)
    except Exception as e:
        logger.error(f"Error dropping existing views: {e}")
        return 0

def escape_sql(text_value):
    """Escape special characters in SQL strings."""
    if not text_value:
        return text_value
    return text_value.replace("'", "''")

def create_device_views(plants=None, create_inverter=True, create_inverter_summary=True, create_inverter_mppt=True, create_meter=True, create_meteo=True, drop_existing=True):
    """
    Create SQL views for device types based on selection.
    
    Args:
        plants (list): List of plant names to create views for. If None, create for all plants.
        create_inverter (bool): Whether to create inverter string-level views.
        create_inverter_summary (bool): Whether to create inverter summary views.
        create_inverter_mppt (bool): Whether to create inverter MPPT views.
        create_meter (bool): Whether to create meter views.
        create_meteo (bool): Whether to create meteo station views.
        drop_existing (bool): Whether to drop existing views before creating new ones.
    """
    logger.info("Starting to create SQL views for FusionSolar data")
    
    try:
        # Create database engine
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Drop existing views if requested
            if drop_existing:
                drop_existing_views(conn)
            
            # Get plants from the database
            plants_result = conn.execute(text("""
                SELECT plant_code, plant_name 
                FROM fusionsolar_plants
            """))
            
            all_plants = [dict(row._mapping) for row in plants_result]
            
            if not all_plants:
                logger.warning("No plants found in the database. Cannot create views.")
                return
            
            # Filter plants if specified
            plant_list = []
            if plants:
                # Filter plants by name
                for plant in all_plants:
                    if plant['plant_name'] in plants:
                        plant_list.append(plant)
                        
                if not plant_list:
                    logger.warning(f"None of the specified plants were found in the database. Valid plants are: {', '.join(p['plant_name'] for p in all_plants)}")
                    return
                    
                logger.info(f"Filtered to {len(plant_list)} plants: {', '.join(p['plant_name'] for p in plant_list)}")
            else:
                # Use all plants
                plant_list = all_plants
                logger.info(f"Using all {len(plant_list)} plants from the database")
            
            # Iterate through each plant
            for plant in plant_list:
                plant_code = plant['plant_code']
                plant_name = plant['plant_name']
                
                # Escape plant name for use in SQL queries
                safe_plant_name = escape_sql(plant_name)
                
                # Create normalized plant name for use in view names
                # Create a safe identifier that doesn't break SQL
                normalized_plant_name = ''
                for char in plant_name:
                    if char.isalnum():
                        normalized_plant_name += char
                    else:
                        normalized_plant_name += '_'
                
                # Ensure name starts with letter or underscore
                if normalized_plant_name and normalized_plant_name[0].isdigit():
                    normalized_plant_name = 'p_' + normalized_plant_name
                    
                # Replace any double underscores with single
                while '__' in normalized_plant_name:
                    normalized_plant_name = normalized_plant_name.replace('__', '_')
                    
                # Remove trailing underscores
                normalized_plant_name = normalized_plant_name.rstrip('_')
                
                logger.info(f"Processing plant: {plant_name} (Code: {plant_code})")
                
                # Get devices for this plant, grouped by device type
                devices_result = conn.execute(text("""
                    SELECT dev_id, dev_name, dev_type_id
                    FROM fusionsolar_devices
                    WHERE plant_code = :plant_code
                """), {"plant_code": plant_code})
                
                devices = [dict(row._mapping) for row in devices_result]
                
                if not devices:
                    logger.warning(f"No devices found for plant {plant_name}. Skipping.")
                    continue
                
                # Group devices by type
                device_types = {}
                for device in devices:
                    dev_type_id = device['dev_type_id']
                    if dev_type_id not in device_types:
                        device_types[dev_type_id] = []
                    device_types[dev_type_id].append(device)
                
                # Create views only for these known device types
                device_type_names = {
                    1: "inverter",
                    10: "meteo_station",
                    17: "meter"
                }
                
                # Only process device types we know about
                for dev_type_id, devices_of_type in device_types.items():
                    # Skip unknown device types entirely
                    if dev_type_id not in device_type_names:
                        continue
                    
                    device_type_name = device_type_names[dev_type_id]
                    
                    # Skip device types based on parameters
                    if device_type_name == 'inverter' and not create_inverter:
                        logger.info(f"Skipping {device_type_name} devices for {plant_name} (disabled)")
                        continue
                    elif device_type_name == 'meter' and not create_meter:
                        logger.info(f"Skipping {device_type_name} devices for {plant_name} (disabled)")
                        continue
                    elif device_type_name == 'meteo_station' and not create_meteo:
                        logger.info(f"Skipping {device_type_name} devices for {plant_name} (disabled)")
                        continue
                        
                    logger.info(f"Creating view for {device_type_name} devices ({len(devices_of_type)} devices)")
                    
                    # Create a view name based on plant name and device type
                    view_name = f"fusionsolar_{normalized_plant_name}_{device_type_name}_data"
                    
                    # Build the SQL query to extract data points from the JSONB
                    # Get device IDs for this plant and device type
                    dev_ids = [device['dev_id'] for device in devices_of_type]
                    
                    # Determine which points to extract based on device type
                    data_points = []
                    if device_type_name == "inverter":
                        data_points = [
                            "inverter_state", "ab_u", "bc_u", "ca_u", "a_u", "b_u", "c_u",
                            "a_i", "b_i", "c_i", "efficiency", "temperature", "power_factor",
                            "elec_freq", "active_power", "reactive_power", "day_cap", "total_cap",
                            "pv1_u", "pv2_u", "pv3_u", "pv4_u", "pv5_u", "pv6_u", "pv7_u", "pv8_u",
                            "pv1_i", "pv2_i", "pv3_i", "pv4_i", "pv5_i", "pv6_i", "pv7_i", "pv8_i"
                        ]
                    elif device_type_name == "meteo_station":
                        data_points = [
                            "total_irradiance", "direct_irradiance", "ambient_temperature",
                            "module_temperature", "wind_speed", "wind_direction"
                        ]
                    elif device_type_name == "meter":
                        data_points = [
                            "active_power", "reactive_power", "forward_active_energy",
                            "reverse_active_energy", "forward_reactive_energy", "reverse_reactive_energy"
                        ]
                    
                    # Build SQL for extracting each data point
                    columns_sql = []
                    for point in data_points:
                        column_name = POINT_DESCRIPTIONS.get(point, point)
                        columns_sql.append(f"measurement_data->'{point}' as {column_name}")
                    
                    # Create the SQL view
                    create_view_sql = f"""
                        CREATE OR REPLACE VIEW {view_name} AS
                        SELECT
                            dev_id,
                            d.dev_name as device_name,
                            collect_time as timestamp,
                            {', '.join(columns_sql)}
                        FROM
                            fusionsolar_historical_data h
                        JOIN
                            fusionsolar_devices d ON h.dev_id = d.dev_id
                        WHERE
                            d.plant_code = '{plant_code}'
                            AND d.dev_type_id = {dev_type_id}
                        ORDER BY
                            collect_time
                    """
                    
                    # Execute the SQL to create the view
                    try:
                        conn.execute(text(create_view_sql))
                        logger.info(f"Successfully created view: {view_name}")
                        
                        # Create specialized views for inverters
                        if device_type_name == "inverter":
                            # 1. Create inverter summary view if requested
                            if create_inverter_summary:
                                summary_view_name = f"fusionsolar_{normalized_plant_name}_inverter_summary_data"
                                
                                summary_columns = [
                                    "day_cap as daily_energy_yield",
                                    "active_power",
                                    "reactive_power as output_reactive_power",
                                    "mppt_total_cap as total_dc_input_energy"
                                ]
                                
                                # Check if mppt_power exists in the data (not in the list we saw)
                                # Adding a NULL placeholder in case it doesn't exist
                                summary_columns.append("NULL::numeric as mppt_power")
                                
                                summary_view_sql = f"""
                                    CREATE OR REPLACE VIEW {summary_view_name} AS
                                    SELECT
                                        dev_id,
                                        d.dev_name as device_name,
                                        collect_time as timestamp,
                                        {', '.join(summary_columns)}
                                    FROM
                                        fusionsolar_historical_data h
                                    JOIN
                                        fusionsolar_devices d ON h.dev_id = d.dev_id
                                    WHERE
                                        d.plant_code = '{plant_code}'
                                        AND d.dev_type_id = {dev_type_id}
                                    ORDER BY
                                        collect_time
                                """
                                
                                try:
                                    conn.execute(text(summary_view_sql))
                                    logger.info(f"Successfully created inverter summary view: {summary_view_name}")
                                except Exception as e:
                                    logger.error(f"Error creating inverter summary view: {e}")
                            
                            # 2. Create MPPT view if requested
                            if create_inverter_mppt:
                                mppt_view_name = f"fusionsolar_{normalized_plant_name}_inverter_mppt_data"
                                
                                mppt_columns = [
                                    "mppt_1_cap as mppt1_dc_total_yield",
                                    "mppt_2_cap as mppt2_dc_total_yield",
                                    "mppt_3_cap as mppt3_dc_total_yield",
                                    "mppt_4_cap as mppt4_dc_total_yield",
                                    "mppt_5_cap as mppt5_dc_total_yield",
                                    "mppt_6_cap as mppt6_dc_total_yield",
                                    "mppt_7_cap as mppt7_dc_total_yield",
                                    "mppt_8_cap as mppt8_dc_total_yield",
                                    "mppt_9_cap as mppt9_dc_total_yield",
                                    "mppt_10_cap as mppt10_dc_total_yield"
                                ]
                                
                                mppt_view_sql = f"""
                                    CREATE OR REPLACE VIEW {mppt_view_name} AS
                                    SELECT
                                        dev_id,
                                        d.dev_name as device_name,
                                        collect_time as timestamp,
                                        {', '.join(mppt_columns)}
                                    FROM
                                        fusionsolar_historical_data h
                                    JOIN
                                        fusionsolar_devices d ON h.dev_id = d.dev_id
                                    WHERE
                                        d.plant_code = '{plant_code}'
                                        AND d.dev_type_id = {dev_type_id}
                                    ORDER BY
                                        collect_time
                                """
                                
                                try:
                                    conn.execute(text(mppt_view_sql))
                                    logger.info(f"Successfully created MPPT view: {mppt_view_name}")
                                except Exception as e:
                                    logger.error(f"Error creating MPPT view: {e}")
                            
                            # 3. Create the original pivoted string view
                            pivoted_view_name = f"fusionsolar_{normalized_plant_name}_inverter_strings_pivoted"
                            
                            # SQL for pivoting string voltage and current data
                            pivot_sql = f"""
                                CREATE OR REPLACE VIEW {pivoted_view_name} AS
                                SELECT
                                    device_name,
                                    timestamp,
                                    'Voltage' as measurement_type,
                                    string_1_voltage as string_1,
                                    string_2_voltage as string_2,
                                    string_3_voltage as string_3,
                                    string_4_voltage as string_4,
                                    string_5_voltage as string_5,
                                    string_6_voltage as string_6,
                                    string_7_voltage as string_7,
                                    string_8_voltage as string_8
                                FROM
                                    {view_name}
                                UNION ALL
                                SELECT
                                    device_name,
                                    timestamp,
                                    'Current' as measurement_type,
                                    string_1_current as string_1,
                                    string_2_current as string_2,
                                    string_3_current as string_3,
                                    string_4_current as string_4,
                                    string_5_current as string_5,
                                    string_6_current as string_6,
                                    string_7_current as string_7,
                                    string_8_current as string_8
                                FROM
                                    {view_name}
                                ORDER BY
                                    device_name, timestamp, measurement_type
                            """
                            
                            conn.execute(text(pivot_sql))
                            logger.info(f"Successfully created pivoted view: {pivoted_view_name}")
                    except Exception as e:
                        logger.error(f"Error creating view {view_name}: {e}")
            
            logger.info("Finished creating SQL views for FusionSolar data")
            
    except Exception as e:
        logger.error(f"Error in create_device_views: {e}")

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Create SQL views for FusionSolar device data.')
    
    # View selection parameters
    view_group = parser.add_argument_group('View Selection')
    view_group.add_argument('--inverter', action='store_true',
                        help='Create inverter string-level views')
    view_group.add_argument('--inverter-summary', action='store_true',
                        help='Create inverter summary views (yield today, active/reactive power, etc.)')
    view_group.add_argument('--inverter-mppt', action='store_true',
                        help='Create inverter MPPT views (mppt_1_cap through mppt_10_cap)')
    view_group.add_argument('--meter', action='store_true',
                        help='Create meter views')
    view_group.add_argument('--meteo-station', action='store_true',
                        help='Create meteo station views')
    view_group.add_argument('--all', action='store_true',
                        help='Create all views (default if no specific view type is selected)')
    
    # Plant selection parameters
    plant_group = parser.add_argument_group('Plant Selection')
    plant_group.add_argument('--plants', type=str, default=None,
                        help='Comma-separated list of plant names to create views for. If not specified, all plants will be processed.')
    
    # Advanced options
    advanced_group = parser.add_argument_group('Advanced Options')
    advanced_group.add_argument('--no-drop-existing', action='store_true',
                        help='Do not drop existing views before creating new ones')
    advanced_group.add_argument('--list-plants', action='store_true',
                        help='List all available plants and exit')
    advanced_group.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    
    return parser.parse_args()

def main():
    """Main function to ensure proper execution order"""
    # Parse command-line arguments
    args = parse_arguments()
    
    # Enable debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug logging enabled")
    
    # List plants if requested
    if args.list_plants:
        try:
            # Create database engine
            engine = create_engine(DATABASE_URL)
            
            with engine.connect() as conn:
                plants_result = conn.execute(text("""SELECT plant_name FROM fusionsolar_plants ORDER BY plant_name"""))
                plants = [row[0] for row in plants_result]
                
                print("Available plants:")
                for plant in plants:
                    print(f"  - {plant}")
        except Exception as e:
            logger.error(f"Error listing plants: {e}")
            return 1
        return 0
    
    # Determine which views to create
    create_inverter = args.inverter or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_inverter_summary = args.inverter_summary or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_inverter_mppt = args.inverter_mppt or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_meter = args.meter or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_meteo = args.meteo_station or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    
    # Parse plants if specified
    plants = None
    if args.plants:
        plants = [plant.strip() for plant in args.plants.split(',')]
    
    # Create views
    try:
        create_device_views(
            plants=plants,
            create_inverter=create_inverter,
            create_inverter_summary=create_inverter_summary,
            create_inverter_mppt=create_inverter_mppt,
            create_meter=create_meter,
            create_meteo=create_meteo,
            drop_existing=not args.no_drop_existing
        )
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
