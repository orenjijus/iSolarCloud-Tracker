#!/usr/bin/env python
"""
Create SQL views for iSolarCloud device data.

This script creates SQL views for various device types in the iSolarCloud database,
including inverters (string-level), meters, and meteo stations. For inverter summary
views, it delegates to the create_inverter_summary_views.py module if available.
"""

import logging
from sqlalchemy import create_engine, text
import os
import argparse
from dotenv import load_dotenv
import importlib.util
import sys
from pathlib import Path

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

# Inverter summary points definition
INVERTER_SUMMARY_POINTS = ["p1", "p14", "p24", "p25"]  # Yield Today, Total DC Power, Total Active Power, Total Reactive Power

# Point descriptions for better column naming
POINT_DESCRIPTIONS = {
    # Inverter summary points
    "p1": "yield_today_kWh",
    "p14": "total_dc_power_W",
    "p24": "total_active_power_W",
    "p25": "total_reactive_power_var",
    
    # Meter points
    "p8018": "active_power_W",
    "p8014": "power_factor",
    "p8031": "negative_active_energy_kWh",
    "p8033": "negative_reactive_energy_kvarh",
    "p8030": "positive_active_energy_kWh",
    "p8032": "positive_reactive_energy_kvarh",
    
    # Meteo station points
    "p9001": "module_temp_C",
    "p9002": "ambient_temp_C",
    "p9003": "irradiance_W_m2",
    "p9005": "wind_speed_m_s",
    "p9006": "wind_direction_deg",
}

# Site measuring points configuration - organized as voltage strings 1-24 followed by current strings 1-24
ISOLARCLOUD_SITE_MEASURING_POINTS = {
    "Garuda Metalindo (IKP)": {
        "inverter": {
            # First all voltage points (1-24), then all current points (1-24)
            "points": [
                # String voltages 1-24
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104",
                "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                "p7166", "p7167", "p7168", "p7169", "p7170", "p7171",
                # String currents 1-24
                "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77",
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85",
                "p92", "p93",
                "p313", "p314", "p315", "p316", "p317", "p318"
            ]
        },
        "meter": {
            "points": [
                "p8018", # active_power_W
                "p8014", # power_factor
                "p8031", # negative_active_energy_kWh
                "p8033", # negative_reactive_energy_kvarh
                "p8030", # positive_active_energy_kWh
                "p8032"  # positive_reactive_energy_kvarh
            ]
        },
        "meteo_station": {
            "points": [
                "p9001", # module_temp
                "p9002", # ambient_temp
                "p9003", # irradiance
                "p9005", # wind_speed
                "p9006"  # wind_direction
            ]
        }
    },
    "Garuda Metalindo (MPF)": {
        "inverter": {
            # First all voltage points (1-24), then all current points (1-24)
            "points": [
                # String voltages 1-24
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104",
                "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                "p7166", "p7167", "p7168", "p7169", "p7170", "p7171",
                # String currents 1-24
                "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77",
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85",
                "p92", "p93",
                "p313", "p314", "p315", "p316", "p317", "p318"
            ]
        },
        "meter": {
            "points": [
                "p8018", # active_power_W
                "p8014", # power_factor
                "p8031", # negative_active_energy_kWh
                "p8033", # negative_reactive_energy_kvarh
                "p8030", # positive_active_energy_kWh
                "p8032"  # positive_reactive_energy_kvarh
            ]
        },
        "meteo_station": {
            "points": [
                "p9001", # module_temp
                "p9002", # ambient_temp
                "p9003", # irradiance
                "p9005", # wind_speed
                "p9006"  # wind_direction
            ]
        }
    },
    "Garuda Metalindo 1": {
        "inverter": {
            # First all voltage points (1-24), then all current points (1-24)
            "points": [
                # String voltages 1-18
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104",
                "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                # String currents 1-18
                "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", "p78",
                "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93"
            ]
        },
        "meter": {
            "points": [
                "p8018", # active_power_W
                "p8014", # power_factor
                "p8031", # negative_active_energy_kWh
                "p8033", # negative_reactive_energy_kvarh
                "p8030", # positive_active_energy_kWh
                "p8032"  # positive_reactive_energy_kvarh
            ]
        },
        "meteo_station": {
            "points": [
                "p9001", # module_temp
                "p9002", # ambient_temp
                "p9003", # irradiance
                "p9005", # wind_speed
                "p9006"  # wind_direction
            ]
        }
    },
    "Garuda Metalindo 2": {
        "inverter": {
            # First all voltage points (1-24), then all current points (1-24)
            "points": [
                # String voltages 1-18
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104",
                "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                # String currents 1-18
                "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", "p78",
                "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93"
            ]
        },
        "meter": {
            "points": [
                "p8018", # active_power_W
                "p8014", # power_factor
                "p8031", # negative_active_energy_kWh
                "p8033", # negative_reactive_energy_kvarh
                "p8030", # positive_active_energy_kWh
                "p8032"  # positive_reactive_energy_kvarh
            ]
        },
        "meteo_station": {
            "points": [
                "p9001", # module_temp
                "p9002", # ambient_temp
                "p9003", # irradiance
                "p9005", # wind_speed
                "p9006"  # wind_direction
            ]
        }
    },
    "Shoetown Ligung Indonesia": {
        "inverter": {
            # First all voltage points (1-24), then all current points (1-24)
            "points": [
                # String voltages 1-24
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104",
                "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                "p7166", "p7167", "p7168", "p7169", "p7170", "p7171",
                # String currents 1-24
                "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77",
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85",
                "p92", "p93",
                "p313", "p314", "p315", "p316", "p317", "p318"
            ]
        },
        "meter": {
            "points": [
                "p8018", # active_power_W
                "p8014", # power_factor
                "p8031", # negative_active_energy_kWh
                "p8033", # negative_reactive_energy_kvarh
                "p8030", # positive_active_energy_kWh
                "p8032"  # positive_reactive_energy_kvarh
            ]
        },
        "meteo_station": {
            "points": [
                "p9001", # module_temp
                "p9002", # ambient_temp
                "p9003", # irradiance
                "p9005", # wind_speed
                "p9006"  # wind_direction
            ]
        }
    },
    "Charoen Pokphand Majalengka": {
        "inverter": {
            # First all voltage points (1-24), then all current points (1-24)
            "points": [
                # String voltages 1-24
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104",
                "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                "p7166", "p7167", "p7168", "p7169", "p7170", "p7171",
                # String currents 1-24
                "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77",
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85",
                "p92", "p93",
                "p313", "p314", "p315", "p316", "p317", "p318"
            ]
        },
        "meter": {
            "points": [
                "p8018", # active_power_W
                "p8014", # power_factor
                "p8031", # negative_active_energy_kWh
                "p8033", # negative_reactive_energy_kvarh
                "p8030", # positive_active_energy_kWh
                "p8032"  # positive_reactive_energy_kvarh
            ]
        },
        "meteo_station": {
            "points": [
                "p9001", # module_temp
                "p9002", # ambient_temp
                "p9003", # irradiance
                "p9005", # wind_speed
                "p9006"  # wind_direction
            ]
        }
    }
}

def escape_sql(text):
    """Escape special characters in SQL strings."""
    if text is None:
        return "NULL"
    return text.replace("'", "''")

def drop_existing_views(conn):
    """Drop all existing data views to avoid conflicts when recreating them."""
    logger.info("Dropping existing views...")
    
    # Get all views in the database with our naming pattern
    query = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'VIEW'
        AND (table_name LIKE '%_data' OR table_name LIKE '%_pivoted')
    """)
    
    # Execute the query and get results
    result = conn.execute(query)
    views = [row[0] for row in result]
    
    # Drop each view
    for view_name in views:
        try:
            drop_sql = f"DROP VIEW {view_name};"
            conn.execute(text(drop_sql))
            logger.info(f"Dropped view: {view_name}")
        except Exception as e:
            logger.error(f"Error dropping view {view_name}: {e}")
    
    logger.info(f"Dropped {len(views)} views")

def create_device_views(sites=None, create_inverter=True, create_inverter_summary=True, create_meter=True, create_meteo=True, drop_existing=False):
    """
    Create SQL views for device types based on selection.
    
    Args:
        sites (list): List of sites to create views for. If None, create for all sites.
        create_inverter (bool): Whether to create inverter string-level views.
        create_inverter_summary (bool): Whether to create inverter summary views.
        create_meter (bool): Whether to create meter views.
        create_meteo (bool): Whether to create meteo station views.
        drop_existing (bool): Whether to drop existing views before creating new ones.
    """
    try:
        # Create database connection
        engine = create_engine(DATABASE_URL)
        
        # Connect to the database
        with engine.connect() as conn:
            # Determine which sites to process
            if sites is not None:
                site_list = sites
            else:
                site_list = list(ISOLARCLOUD_SITE_MEASURING_POINTS.keys())
                
            logger.info(f"Creating views for {len(site_list)} sites")
            
            # Drop existing views if requested
            if drop_existing:
                drop_existing_views(conn)
            
            # Process each site
            for site_name in site_list:
                # Skip sites that don't exist in our configuration
                if site_name not in ISOLARCLOUD_SITE_MEASURING_POINTS:
                    logger.warning(f"Site {site_name} not found in configuration")
                    continue
                    
                site_devices = ISOLARCLOUD_SITE_MEASURING_POINTS[site_name]
                
                # Process each device type
                for device_type_name, device_config in site_devices.items():
                    # Skip device types that are not selected
                    if (device_type_name == "inverter" and not create_inverter) or \
                       (device_type_name == "meteo_station" and not create_meteo) or \
                       (device_type_name == "meter" and not create_meter):
                        logger.info(f"Skipping {device_type_name} views for {site_name} (not selected)")
                        continue
                        
                    # Clean site name for view naming
                    clean_site_name = site_name.replace(' ', '_').replace('(', '').replace(')', '')
                    escaped_site_name = escape_sql(site_name)
                    
                    # Map device type name to device_type_id in the database
                    device_type_id = {
                        "inverter": 1,
                        "meter": 4,
                        "meteo_station": 16
                    }.get(device_type_name)
                    
                    if device_type_id is None:
                        logger.warning(f"Unknown device type: {device_type_name}. Skipping.")
                        continue
                    
                    # Get measuring points for this device type
                    points = device_config.get("points", [])
                    
                    if not points:
                        logger.warning(f"No measuring points defined for {device_type_name} at {site_name}. Skipping.")
                        continue
                    
                    # Create view name
                    view_name = f"{clean_site_name}_{device_type_name}_data"
                    
                    # Generate the SQL for extracting each measurement point
                    point_extractions = []
                    for point in points:
                        description = POINT_DESCRIPTIONS.get(point, point.replace("p", "point_"))
                        point_extractions.append(f"CASE WHEN (h.measurement_data::jsonb->>{repr(point)}) ~ '^-?\\d+(\\.\\d+)?$' THEN (h.measurement_data::jsonb->>{repr(point)})::NUMERIC ELSE NULL END AS \"{description}\"")
                    
                    # Create the SQL for the view
                    view_sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
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
                    
                    # Execute the SQL to create the view
                    conn.execute(text(view_sql))
                    logger.info(f"Created view {view_name} for {site_name} {device_type_name} data")
                    
                    # Create pivoted view for this device type
                    pivoted_view_name = f"{clean_site_name}_{device_type_name}_pivoted"
                    
                    # Get all devices of this type for this site
                    query_devices = text(f"""
                        SELECT device_ps_key, device_name
                        FROM isolarcloud_devices d
                        JOIN isolarcloud_power_stations ps ON d.ps_id = ps.ps_id
                        WHERE ps.ps_name = '{escaped_site_name}' AND d.device_type = {device_type_id}
                        ORDER BY device_name;
                    """)
                    
                    # Execute query to get devices
                    devices_result = conn.execute(query_devices)
                    devices_list = [(row[0], row[1]) for row in devices_result]
                    
                    if not devices_list:
                        logger.warning(f"No {device_type_name} devices found for {site_name}. Skipping pivoted view.")
                        continue
                    
                    # Generate pivoted column definitions for each device and point
                    column_defs = []
                    used_column_names = set()
                    
                    for device_ps_key, device_name in devices_list:
                        # Clean device name for column names
                        clean_device_name = ''
                        for char in device_name:
                            if char.isalnum():
                                clean_device_name += char
                            else:
                                clean_device_name += '_'
                                
                        # Ensure column name starts with a letter (SQL requirement)
                        if clean_device_name and clean_device_name[0].isdigit():
                            clean_device_name = 'dev_' + clean_device_name
                            
                        # Add columns for each measurement point
                        for point in points:
                            point_desc = POINT_DESCRIPTIONS.get(point, point.replace("p", "point_"))
                            column_name = f"{clean_device_name}_{point_desc}"
                            
                            if column_name in used_column_names:
                                continue
                            used_column_names.add(column_name)
                            column_def = f"""
                            MAX(CASE WHEN h.device_ps_key = {repr(device_ps_key)} 
                                THEN CASE WHEN (h.measurement_data::jsonb->>{repr(point)}) ~ '^-?\\d+(\\.\\d+)?$' THEN (h.measurement_data::jsonb->>{repr(point)})::NUMERIC ELSE NULL END 
                                ELSE NULL END) AS "{column_name}"
                            """
                            column_defs.append(column_def)
                    
                    # Create the pivoted view SQL
                    pivoted_view_sql = f"""
                    CREATE OR REPLACE VIEW {pivoted_view_name} AS
                    SELECT 
                        h.timestamp,
                        {', '.join(column_defs) if column_defs else 'NULL::numeric AS placeholder'}
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
                    
                    # Execute the SQL to create the pivoted view
                    conn.execute(text(pivoted_view_sql))
                    logger.info(f"Created pivoted view {pivoted_view_name} for {site_name} {device_type_name} data")
            
            # Create inverter summary views if requested
            if create_inverter_summary:
                logger.info("Creating inverter summary views...")
                
                # Iterate through sites to create inverter summary views
                for site_name in site_list:
                    # Skip sites without inverters or that don't exist
                    if site_name not in ISOLARCLOUD_SITE_MEASURING_POINTS:
                        logger.warning(f"Site {site_name} not found in configuration")
                        continue
                        
                    site_devices = ISOLARCLOUD_SITE_MEASURING_POINTS[site_name]
                    if "inverter" not in site_devices:
                        logger.info(f"Skipping inverter summary views for {site_name} - no inverters defined")
                        continue
                    
                    # Clean site name for view naming
                    clean_site_name = site_name.replace(' ', '_').replace('(', '').replace(')', '')
                    escaped_site_name = escape_sql(site_name)
                    device_type_id = 1  # Inverter device type
                    
                    # Create inverter_summary regular view
                    summary_view_name = f"{clean_site_name}_inverter_summary_data"
                    
                    # Generate the SQL for extracting just the summary measurement points
                    point_extractions = []
                    logger.info(f"Processing inverter summary points for {site_name}: {', '.join(INVERTER_SUMMARY_POINTS)}")
                    
                    for point in INVERTER_SUMMARY_POINTS:
                        description = POINT_DESCRIPTIONS.get(point, point.replace("p", "point_"))
                        logger.debug(f"Adding summary point {point} as {description}")
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
                    try:
                        conn.execute(text(summary_view_sql))
                        logger.info(f"Created view {summary_view_name} for {site_name}")
                    except Exception as e:
                        logger.error(f"Error creating summary view for {site_name}: {e}")
                        continue
                    
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
                    
                    # Log the SQL query for debugging
                    logger.debug(f"SQL query for finding inverters: {query_devices}")
                    
                    # Execute query to get devices
                    devices_result = conn.execute(query_devices)
                    devices_list = [(row[0], row[1]) for row in devices_result]
                    
                    if not devices_list:
                        logger.warning(f"No inverter devices found for {site_name}. Skipping pivoted summary view.")
                        continue
                        
                    logger.info(f"Found {len(devices_list)} inverter devices for {site_name}")
                    
                    # Generate pivoted column definitions for summary points
                    column_defs = []
                    used_column_names = set()
                    
                    for device_ps_key, device_name in devices_list:
                        logger.debug(f"Processing device: {device_name} (device_ps_key: {device_ps_key})")
                        
                        # Clean device name for column names
                        clean_device_name = ''
                        for char in device_name:
                            if char.isalnum():
                                clean_device_name += char
                            else:
                                clean_device_name += '_'
                                
                        if clean_device_name and clean_device_name[0].isdigit():
                            clean_device_name = 'dev_' + clean_device_name
                            
                        logger.debug(f"Cleaned device name: {clean_device_name}")
                            
                        # Add columns for each summary point
                        for point in INVERTER_SUMMARY_POINTS:
                            point_desc = POINT_DESCRIPTIONS.get(point, point.replace("p", "point_"))
                            column_name = f"{clean_device_name}_{point_desc}"
                            
                            if column_name in used_column_names:
                                logger.debug(f"Skipping duplicate column: {column_name}")
                                continue
                                
                            used_column_names.add(column_name)
                            
                            # Add column definition for this device and point
                            column_def = f"""
                            MAX(CASE WHEN h.device_ps_key = {repr(device_ps_key)} 
                                THEN CASE WHEN (h.measurement_data::jsonb->>{repr(point)}) ~ '^-?\\d+(\\.\\d+)?$' THEN (h.measurement_data::jsonb->>{repr(point)})::NUMERIC ELSE NULL END 
                                ELSE NULL END) AS \"{column_name}\"
                            """
                            
                            # Add to our column definitions list
                            column_defs.append(column_def)
                            logger.debug(f"Added column: {column_name}")
                    
                    # Create the pivoted summary view SQL
                    pivoted_summary_sql = f"""
                    CREATE OR REPLACE VIEW {pivoted_summary_view_name} AS
                    SELECT 
                        h.timestamp,
                        {', '.join(column_defs) if column_defs else 'NULL::numeric AS placeholder'}
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
                    try:
                        conn.execute(text(pivoted_summary_sql))
                        logger.info(f"Created pivoted view {pivoted_summary_view_name} for {site_name}")
                    except Exception as e:
                        logger.error(f"Error creating pivoted summary view: {e}")
                        logger.error(f"SQL query: {pivoted_summary_sql}")
                        continue
            
            conn.commit()
            logger.info("All views created successfully")
            
    except Exception as e:
        logger.error(f"Error creating device views: {e}")
        raise

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Create SQL views for iSolarCloud device data.')
    
    # View selection parameters
    view_group = parser.add_argument_group('View Selection')
    view_group.add_argument('--inverter', action='store_true',
                        help='Create inverter string-level views')
    view_group.add_argument('--inverter-summary', action='store_true',
                        help='Create inverter summary views (yield today, total DC power, total active/reactive power)')
    view_group.add_argument('--meter', action='store_true',
                        help='Create meter views')
    view_group.add_argument('--meteo-station', action='store_true',
                        help='Create meteo station views')
    view_group.add_argument('--all', action='store_true',
                        help='Create all views (default if no specific view type is selected)')
    
    # Site selection parameters
    site_group = parser.add_argument_group('Site Selection')
    site_group.add_argument('--sites', type=str, default=None,
                        help='Comma-separated list of site names to create views for. If not specified, all sites will be processed.')
    
    # Advanced options
    advanced_group = parser.add_argument_group('Advanced Options')
    advanced_group.add_argument('--no-drop-existing', action='store_true',
                        help='Do not drop existing views before creating new ones')
    advanced_group.add_argument('--list-sites', action='store_true',
                        help='List all available sites and exit')
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
    
    # List sites if requested
    if args.list_sites:
        print("Available sites:")
        for site in sorted(ISOLARCLOUD_SITE_MEASURING_POINTS.keys()):
            print(f"  - {site}")
        return 0
    
    # Determine which views to create
    create_inverter = args.inverter or args.all or (not any([args.inverter, args.inverter_summary, args.meter, args.meteo_station]))
    create_inverter_summary = args.inverter_summary or args.all or (not any([args.inverter, args.inverter_summary, args.meter, args.meteo_station]))
    create_meter = args.meter or args.all or (not any([args.inverter, args.inverter_summary, args.meter, args.meteo_station]))
    create_meteo = args.meteo_station or args.all or (not any([args.inverter, args.inverter_summary, args.meter, args.meteo_station]))
    
    # Parse sites if specified
    sites = None
    if args.sites:
        sites = [site.strip() for site in args.sites.split(',')]
        # Validate site names
        invalid_sites = [site for site in sites if site not in ISOLARCLOUD_SITE_MEASURING_POINTS]
        if invalid_sites:
            logger.warning(f"Unknown sites: {', '.join(invalid_sites)}. Valid sites are: {', '.join(ISOLARCLOUD_SITE_MEASURING_POINTS.keys())}")
            return 1
    
    # Create views
    try:
        create_device_views(
            sites=sites,
            create_inverter=create_inverter,
            create_inverter_summary=create_inverter_summary,
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
