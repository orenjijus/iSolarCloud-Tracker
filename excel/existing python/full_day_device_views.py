import logging
import argparse
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL from individual PostgreSQL settings
import urllib.parse
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
logging.info(f"Database URL constructed: postgresql://{POSTGRES_USER}:***@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# Import device types from config
from fusionsolar_harvester_src.fusionsolar_config import DEVICE_TYPES

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
    "mppt_1_cap": "mppt1_dc_total_yield",     # MPPT 1 DC total yield (kWh)
    "mppt_2_cap": "mppt2_dc_total_yield",     # MPPT 2 DC total yield (kWh)
    "mppt_3_cap": "mppt3_dc_total_yield",     # MPPT 3 DC total yield (kWh)
    "mppt_4_cap": "mppt4_dc_total_yield",     # MPPT 4 DC total yield (kWh)
    "mppt_5_cap": "mppt5_dc_total_yield",     # MPPT 5 DC total yield (kWh)
    "mppt_6_cap": "mppt6_dc_total_yield",     # MPPT 6 DC total yield (kWh)
    "mppt_7_cap": "mppt7_dc_total_yield",     # MPPT 7 DC total yield (kWh)
    "mppt_8_cap": "mppt8_dc_total_yield",     # MPPT 8 DC total yield (kWh)
    "mppt_9_cap": "mppt9_dc_total_yield",     # MPPT 9 DC total yield (kWh)
    "mppt_10_cap": "mppt10_dc_total_yield",   # MPPT 10 DC total yield (kWh)
    "mppt_total_cap": "total_dc_input_energy",# Total DC input energy (kWh)
    "open_time": "inverter_startup_time",     # Inverter startup time (ms)
    "close_time": "inverter_shutdown_time",    # Inverter shutdown time (ms)
    
    # PV inputs (strings)
    "pv1_u": "string_1_voltage",
    "pv2_u": "string_2_voltage",
    "pv3_u": "string_3_voltage",
    "pv4_u": "string_4_voltage",
    "pv5_u": "string_5_voltage",
    "pv6_u": "string_6_voltage",
    "pv7_u": "string_7_voltage",
    "pv8_u": "string_8_voltage",
    "pv9_u": "string_9_voltage",
    "pv10_u": "string_10_voltage",
    "pv11_u": "string_11_voltage",
    "pv12_u": "string_12_voltage",
    "pv13_u": "string_13_voltage",
    "pv14_u": "string_14_voltage",
    "pv15_u": "string_15_voltage",
    "pv16_u": "string_16_voltage",
    "pv17_u": "string_17_voltage",
    "pv18_u": "string_18_voltage",
    "pv19_u": "string_19_voltage",
    "pv20_u": "string_20_voltage",
    "pv21_u": "string_21_voltage",
    "pv22_u": "string_22_voltage",
    "pv23_u": "string_23_voltage",
    "pv24_u": "string_24_voltage",
    "pv25_u": "string_25_voltage",
    "pv26_u": "string_26_voltage",
    "pv27_u": "string_27_voltage",
    "pv28_u": "string_28_voltage",
    
    "pv1_i": "string_1_current",
    "pv2_i": "string_2_current",
    "pv3_i": "string_3_current",
    "pv4_i": "string_4_current",
    "pv5_i": "string_5_current",
    "pv6_i": "string_6_current",
    "pv7_i": "string_7_current",
    "pv8_i": "string_8_current",
    "pv9_i": "string_9_current",
    "pv10_i": "string_10_current",
    "pv11_i": "string_11_current",
    "pv12_i": "string_12_current",
    "pv13_i": "string_13_current",
    "pv14_i": "string_14_current",
    "pv15_i": "string_15_current",
    "pv16_i": "string_16_current",
    "pv17_i": "string_17_current",
    "pv18_i": "string_18_current",
    "pv19_i": "string_19_current",
    "pv20_i": "string_20_current",
    "pv21_i": "string_21_current",
    "pv22_i": "string_22_current",
    "pv23_i": "string_23_current",
    "pv24_i": "string_24_current",
    "pv25_i": "string_25_current",
    "pv26_i": "string_26_current",
    "pv27_i": "string_27_current",
    "pv28_i": "string_28_current",
    
    # Meter points
    "active_power": "active_power",                   # Active power (W or kW)
    "reverse_active_cap": "negative_active_energy",   # Negative active energy (kWh)
    "active_cap": "positive_active_energy",           # Positive active energy (kWh)
    "forward_reactive_cap": "positive_reactive_energy", # Positive reactive energy (kWh)
    "reverse_reactive_cap": "negative_reactive_energy", # Negative reactive energy (kWh)
    "power_factor": "power_factor",                   # Power factor (None)
    "grid_frequency": "grid_frequency",               # Grid frequency (Hz)
    "ab_u": "a_b_line_voltage",                       # A-B line voltage of grid (V)
    "bc_u": "b_c_line_voltage",                       # B-C line voltage of grid (V)
    "ca_u": "c_a_line_voltage",                       # C-A line voltage of grid (V)
    "a_u": "phase_a_voltage",                         # Phase A voltage (V)
    "b_u": "phase_b_voltage",                         # Phase B voltage (V)
    "c_u": "phase_c_voltage",                         # Phase C voltage (V)
    "a_i": "phase_a_current",                         # Phase A current (A)
    "b_i": "phase_b_current",                         # Phase B current (A)
    "c_i": "phase_c_current",                         # Phase C current (A)
    "meter_status": "meter_state",                    # Meter state (0: offline; 1: normal)
    "total_apparent_power": "total_apparent_power",   # Total apparent power (kVA)
    "reverse_active_peak": "negative_active_energy_peak", # Negative active energy (peak) (kWh)
    "reverse_active_power": "negative_active_energy_shoulder", # Negative active energy (shoulder) (kWh)
    "reverse_active_valley": "negative_active_energy_off_peak", # Negative active energy (off-peak) (kWh)

    
    # Meteo station points
    "temperature": "ambient_temperature",               # Ambient temperature (°C)
    "pv_temperature": "module_temperature",             # PV module temperature (°C)
    "radiant_line": "irradiance",                       # Irradiance (W/m²)
    "radiant_total": "daily_irradiance",                # Total irradiance (MJ/m²)
    "horiz_radiant_line": "horizontal_irradiance",      # Horizontal irradiance (W/m²)
    "horiz_radiant_total": "horizontal_irradiation",    # Horizontal irradiation (MJ/m²)
    "wind_speed": "wind_speed",                         # Wind speed (m/s)
    "wind_direction": "wind_direction",                 # Optional, if available
    "rainfall": "rainfall",                             # Optional, if available
    "humidity": "humidity"                              # Optional, if available
}

def init_database():
    """Initialize database connection."""
    try:
        # Use autocommit to ensure DDL statements are committed
        engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info(f"Database connection test: {result.scalar() == 1}")
            
        return engine
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return None

def escape_sql(text):
    """Escape special characters in SQL strings."""
    if text is None:
        return ""
    return text.replace("'", "''")

def drop_existing_views(conn, view_pattern=None):
    """Drop all existing data views that match the given pattern to avoid conflicts when recreating them."""
    try:
        # Query to find all views
        if view_pattern:
            query = text("""
                SELECT table_name FROM information_schema.views 
                WHERE table_schema = 'public' AND table_name LIKE :pattern
            """)
            result = conn.execute(query, {"pattern": f"%{view_pattern}%"})
        else:
            query = text("""
                SELECT table_name FROM information_schema.views 
                WHERE table_schema = 'public' AND (
                    table_name LIKE '%_full_day_data' OR 
                    table_name LIKE '%_pivoted_full_day'
                )
            """)
            result = conn.execute(query)
        
        # Drop each view
        for row in result:
            view_name = row[0]
            logger.info(f"Dropping view: {view_name}")
            drop_sql = f'DROP VIEW IF EXISTS "{view_name}" CASCADE;'
            conn.execute(text(drop_sql))
        
        logger.info("Existing views dropped successfully")
    except Exception as e:
        logger.error(f"Error dropping existing views: {e}")

def get_device_points_by_type(dev_type_id, view_type="standard"):
    """Get common measurement points for a specific device type and view type.
    
    Args:
        dev_type_id: The device type ID to get points for
        view_type: The type of view to get points for (standard, summary, or mppt)
    """
    if dev_type_id == DEVICE_TYPES.get("inverter", 1):
        if view_type == "summary":
            # Inverter summary points
            return [
                "day_cap",           # Daily energy yield (kWh)
                "active_power",      # Active power (W)
                "reactive_power",    # Reactive power (var)
                "mppt_total_cap"     # Total DC input energy (kWh)
            ]
        elif view_type == "mppt":
            # Inverter MPPT points
            return [
                "mppt_1_cap", "mppt_2_cap", "mppt_3_cap", "mppt_4_cap", "mppt_5_cap",
                "mppt_6_cap", "mppt_7_cap", "mppt_8_cap", "mppt_9_cap", "mppt_10_cap"
            ]
        else:
            # Standard inverter points (string voltage and current)
            voltage_points = [
               "pv1_u", "pv2_u", "pv3_u", "pv4_u", "pv5_u", "pv6_u", "pv7_u", 
               "pv8_u", "pv9_u", "pv10_u", "pv11_u", "pv12_u", "pv13_u", "pv14_u", 
               "pv15_u", "pv16_u", "pv17_u", "pv18_u", "pv19_u", "pv20_u", "pv21_u", 
               "pv22_u", "pv23_u", "pv24_u", "pv25_u", "pv26_u", "pv27_u", "pv28_u"
            ]
            current_points = [
                "pv1_i", "pv2_i", "pv3_i", "pv4_i", "pv5_i", "pv6_i", "pv7_i", 
                "pv8_i", "pv9_i", "pv10_i", "pv11_i", "pv12_i", "pv13_i", "pv14_i", 
                "pv15_i", "pv16_i", "pv17_i", "pv18_i", "pv19_i", "pv20_i", "pv21_i", 
                "pv22_i", "pv23_i", "pv24_i", "pv25_i", "pv26_i", "pv27_i", "pv28_i"
            ]
            return voltage_points + current_points
    
    elif dev_type_id == DEVICE_TYPES.get("meter", 7):
        return ["active_power",             # Active power (W or kW)
                "power_factor",             # Power factor (None)
                "reverse_active_cap",       # Negative active energy (W)
                "reverse_reactive_cap",     # Negative reactive energy (W)
                "active_cap",               # Positive active energy (W)
                "forward_reactive_cap"]     # Positive reactive energy (W)
    
    elif dev_type_id == DEVICE_TYPES.get("meteo_station", 5):
        return ["radiant_line", "radiant_total"]
    
    else:
        return []

def create_full_day_device_views(site_code=None, friendly_name=None, create_inverter=True, create_inverter_summary=True, create_inverter_mppt=True, create_meter=True, create_meteo=True, drop_existing=False):
    """Create SQL views for full day data (00:00-23:55) with pivoted device data.
    
    Args:
        site_code: Optional site code (e.g., 'NE=50488260'). If None, process all sites.
        friendly_name: Optional friendly name to use in view name instead of site code
        create_inverter (bool): Whether to create inverter string-level views.
        create_inverter_summary (bool): Whether to create inverter summary views.
        create_inverter_mppt (bool): Whether to create inverter MPPT views.
        create_meter (bool): Whether to create meter views.
        create_meteo (bool): Whether to create meteo station views.
        drop_existing (bool): Whether to drop existing views before creating new ones.
    """
    engine = init_database()
    if not engine:
        logger.error("Database initialization failed")
        return False
    
    with engine.connect() as conn:
        # Drop existing views if requested
        if drop_existing:
            logger.info("Dropping existing views")
            drop_existing_views(conn)
        else:
            logger.info("Skipping drop of existing views (--no-drop-existing)")
        
        # Get sites to process
        if site_code:
            # Query to get sites by site code
            query = text("""
                SELECT DISTINCT plant_code
                FROM fusionsolar_devices 
                WHERE plant_code = :site_code
            """)
            result = conn.execute(query, {"site_code": site_code})
        else:
            # Query to get all sites
            query = text("""
                SELECT DISTINCT plant_code
                FROM fusionsolar_devices
            """)
            result = conn.execute(query)
        
        sites = [dict(row._mapping) for row in result]
        
        if not sites:
            logger.warning(f"No sites found{' for site_code: ' + site_code if site_code else ''}")
            return False
        
        logger.info(f"Found {len(sites)} sites to process")
        
        # Process each site
        for site in sites:
            site_code = site['plant_code']
            # Use the site_code as the site_name if no friendly name is provided
            site_name = site_code
            
            # Use provided friendly name or the site_code
            plant_name = friendly_name if friendly_name else site_code
            
            # Create a normalized plant name for SQL view names
            clean_plant_name = ''
            for char in plant_name:
                if char.isalnum():
                    clean_plant_name += char.lower()
                else:
                    clean_plant_name += '_'
            
            # Ensure name starts with letter or underscore
            if clean_plant_name and clean_plant_name[0].isdigit():
                clean_plant_name = 'p_' + clean_plant_name
                
            # Replace any double underscores with single
            while '__' in clean_plant_name:
                clean_plant_name = clean_plant_name.replace('__', '_')
                
            # Remove trailing underscores
            clean_plant_name = clean_plant_name.rstrip('_')
            
            logger.info(f"Processing site: {site_name} ({site_code})")
            
            # Get all devices for the site grouped by device type
            query = text("""
                SELECT d.dev_id, d.dev_name, d.dev_type_id, d.plant_code
                FROM fusionsolar_devices d
                WHERE d.plant_code = :site_code
                ORDER BY d.dev_type_id, d.dev_name
            """)
            
            result = conn.execute(query, {"site_code": site_code})
            devices = [dict(row._mapping) for row in result]
            
            if not devices:
                logger.warning(f"No devices found for site: {site_code}")
                continue
            
            logger.info(f"Found {len(devices)} devices for site {site_code}")
            
            # Group devices by type
            devices_by_type = {}
            for device in devices:
                dev_type_id = device['dev_type_id']
                if dev_type_id not in devices_by_type:
                    devices_by_type[dev_type_id] = []
                devices_by_type[dev_type_id].append(device)
                
            # Log available device types for debugging
            logger.info(f"Found device types for {site_code}: {list(devices_by_type.keys())}")
            
            # Map device types to names for reference
            type_names = {}
            for name, type_id in DEVICE_TYPES.items():
                type_names[type_id] = name
            logger.info(f"Device type mapping: {type_names}")
            
            # Filter to only include known device types
            filtered_devices_by_type = {}
            for dev_type_id, devices_list in devices_by_type.items():
                if dev_type_id == DEVICE_TYPES.get("inverter", 1):
                    filtered_devices_by_type[dev_type_id] = devices_list
                    logger.info(f"Including {len(devices_list)} inverter devices")
                elif dev_type_id == DEVICE_TYPES.get("meter", 7):
                    filtered_devices_by_type[dev_type_id] = devices_list
                    logger.info(f"Including {len(devices_list)} meter devices")
                elif dev_type_id == DEVICE_TYPES.get("meteo_station", 10):
                    filtered_devices_by_type[dev_type_id] = devices_list
                    logger.info(f"Including {len(devices_list)} meteo_station devices")
                else:
                    logger.info(f"Skipping unsupported device type ID: {dev_type_id}")
            
            if not filtered_devices_by_type:
                logger.warning(f"No supported device types found for site {site_code}")
                continue
                
            # Replace with filtered list
            devices_by_type = filtered_devices_by_type
            
            # Create views for each device type
            for dev_type_id, devices_of_type in devices_by_type.items():
                # Find device type name first
                device_type_name = "unknown"
                for name, type_id in DEVICE_TYPES.items():
                    if type_id == dev_type_id:
                        device_type_name = name
                        break
                
                # Process each device type
                if device_type_name == 'inverter' and not (create_inverter or create_inverter_summary or create_inverter_mppt):
                    logger.info(f"Skipping all inverter views for {site_code} (all inverter view types disabled)")
                    continue
                elif device_type_name == 'meter' and not create_meter:
                    logger.info(f"Skipping meter views for {site_code} (disabled)")
                    continue
                elif device_type_name == 'meteo_station' and not create_meteo:
                    logger.info(f"Skipping meteo_station views for {site_code} (disabled)")
                    continue
                
                # For inverters, we can skip the standard views but still create summary/MPPT
                if device_type_name == 'inverter' and not create_inverter:
                    logger.info(f"Standard inverter views disabled for {site_code}, but will check for summary/MPPT")
                
                # Create view names
                full_day_view_name = f"{clean_plant_name}_{device_type_name}_full_day_data"
                pivoted_full_day_view_name = f"{clean_plant_name}_{device_type_name}_pivoted_full_day"
                
                # Get device IDs for the IN clause
                device_ids = [f"'{d['dev_id']}'" for d in devices_of_type]
                device_ids_str = ", ".join(device_ids)
                
                # Get relevant measurement points for this device type
                points = get_device_points_by_type(dev_type_id, "standard")
                
                if not points:
                    logger.warning(f"No measurement points defined for device type {device_type_name}. Skipping.")
                    continue
                
                # Create the base full day view with all device data
                full_day_view_sql = f"""
                DROP VIEW IF EXISTS "{full_day_view_name}" CASCADE;
                
                CREATE OR REPLACE VIEW "{full_day_view_name}" AS
                WITH time_series AS (
                    SELECT 
                        generate_series(
                            COALESCE(
                                (SELECT MIN(date_trunc('day', collect_time)::timestamp) 
                                 FROM fusionsolar_historical_data 
                                 WHERE dev_id IN ({device_ids_str})),
                                '2025-05-01'::timestamp
                            ), 
                            COALESCE(
                                (SELECT MAX(date_trunc('day', collect_time)::timestamp) 
                                 FROM fusionsolar_historical_data 
                                 WHERE dev_id IN ({device_ids_str})),
                                '2025-05-04'::timestamp
                            ) + INTERVAL '23 hours 55 minutes',
                            INTERVAL '5 minutes'
                        ) AS timestamp
                ),
                base_data AS (
                    SELECT 
                        h.dev_id,
                        d.dev_name,
                        h.collect_time,
                        h.measurement_data
                    FROM fusionsolar_historical_data h
                    JOIN fusionsolar_devices d ON h.dev_id = d.dev_id
                    WHERE h.dev_id IN ({device_ids_str})
                )
                SELECT 
                    ts.timestamp AS timestamp,
                    d.dev_id,
                    d.dev_name,
                    d.measurement_data
                FROM time_series ts
                LEFT JOIN base_data d ON 
                    date_trunc('minute', ts.timestamp) = date_trunc('minute', d.collect_time)
                ORDER BY ts.timestamp, d.dev_id;
                """
                
                try:
                    # Execute the view creation
                    logger.info(f"Creating full day view: {full_day_view_name}")
                    conn.execute(text(full_day_view_sql))
                    
                    # Now create the pivoted view with full time series and device measurements as columns
                    column_defs = []
                    used_column_names = set()
                    
                    # Special handling for inverters to group all voltages together first, then all currents
                    if device_type_name == "inverter":
                        # Separate voltage and current points
                        voltage_points = [p for p in points if p.endswith("_u")]
                        current_points = [p for p in points if p.endswith("_i")]
                        
                        # First output all voltage points for all devices
                        for device in devices_of_type:
                            device_id = device['dev_id']
                            device_name = device['dev_name']
                            
                            # Clean device name for column names
                            clean_device_name = ''
                            for char in device_name:
                                if char.isalnum():
                                    clean_device_name += char
                                else:
                                    clean_device_name += '_'
                            
                            # Ensure the name doesn't start with a number
                            if clean_device_name and clean_device_name[0].isdigit():
                                clean_device_name = 'dev_' + clean_device_name
                            
                            # Add columns for each voltage point
                            for point in voltage_points:
                                point_desc = POINT_DESCRIPTIONS.get(point, point)
                                column_name = f"{clean_device_name}_{point_desc}"
                                
                                if column_name in used_column_names:
                                    # Add a suffix to avoid duplicate column names
                                    column_name = f"{column_name}_{device_id.replace('-', '_')}"
                                
                                used_column_names.add(column_name)
                                
                                # Extract numeric value from JSON
                                column_def = f"""
                                MAX(CASE WHEN base.dev_id = '{device_id}' 
                                    THEN CASE 
                                        WHEN base.measurement_data::jsonb ? {repr(point)} AND
                                             (base.measurement_data::jsonb->{repr(point)})::text ~ '^-?\\d+(\\.\\d+)?$' 
                                        THEN (base.measurement_data::jsonb->{repr(point)})::NUMERIC 
                                        ELSE NULL 
                                    END 
                                    ELSE NULL 
                                END) AS "{column_name}"
                                """
                                column_defs.append(column_def)
                        
                        # Then output all current points for all devices
                        for device in devices_of_type:
                            device_id = device['dev_id']
                            device_name = device['dev_name']
                            
                            # Clean device name for column names
                            clean_device_name = ''
                            for char in device_name:
                                if char.isalnum():
                                    clean_device_name += char
                                else:
                                    clean_device_name += '_'
                            
                            # Ensure the name doesn't start with a number
                            if clean_device_name and clean_device_name[0].isdigit():
                                clean_device_name = 'dev_' + clean_device_name
                            
                            # Add columns for each current point
                            for point in current_points:
                                point_desc = POINT_DESCRIPTIONS.get(point, point)
                                column_name = f"{clean_device_name}_{point_desc}"
                                
                                if column_name in used_column_names:
                                    # Add a suffix to avoid duplicate column names
                                    column_name = f"{column_name}_{device_id.replace('-', '_')}"
                                
                                used_column_names.add(column_name)
                                
                                # Extract numeric value from JSON
                                column_def = f"""
                                MAX(CASE WHEN base.dev_id = '{device_id}' 
                                    THEN CASE 
                                        WHEN base.measurement_data::jsonb ? {repr(point)} AND
                                             (base.measurement_data::jsonb->{repr(point)})::text ~ '^-?\\d+(\\.\\d+)?$' 
                                        THEN (base.measurement_data::jsonb->{repr(point)})::NUMERIC 
                                        ELSE NULL 
                                    END 
                                    ELSE NULL 
                                END) AS "{column_name}"
                                """
                                column_defs.append(column_def)
                                
                        # Add other inverter points after voltage and current
                        other_points = [p for p in points if not p.endswith("_u") and not p.endswith("_i")]
                        if other_points:
                            for device in devices_of_type:
                                device_id = device['dev_id']
                                device_name = device['dev_name']
                                
                                # Clean device name for column names
                                clean_device_name = ''
                                for char in device_name:
                                    if char.isalnum():
                                        clean_device_name += char
                                    else:
                                        clean_device_name += '_'
                                
                                # Ensure the name doesn't start with a number
                                if clean_device_name and clean_device_name[0].isdigit():
                                    clean_device_name = 'dev_' + clean_device_name
                                
                                # Add columns for other points
                                for point in other_points:
                                    point_desc = POINT_DESCRIPTIONS.get(point, point)
                                    column_name = f"{clean_device_name}_{point_desc}"
                                    
                                    if column_name in used_column_names:
                                        # Add a suffix to avoid duplicate column names
                                        column_name = f"{column_name}_{device_id.replace('-', '_')}"
                                    
                                    used_column_names.add(column_name)
                                    
                                    # Extract numeric value from JSON
                                    column_def = f"""
                                    MAX(CASE WHEN base.dev_id = '{device_id}' 
                                        THEN CASE 
                                            WHEN base.measurement_data::jsonb ? {repr(point)} AND
                                                 (base.measurement_data::jsonb->{repr(point)})::text ~ '^-?\\d+(\\.\\d+)?$' 
                                            THEN (base.measurement_data::jsonb->{repr(point)})::NUMERIC 
                                            ELSE NULL 
                                        END 
                                        ELSE NULL 
                                    END) AS "{column_name}"
                                    """
                                    column_defs.append(column_def)
                    else:
                        # For non-inverter devices, just add all points for each device
                        for device in devices_of_type:
                            device_id = device['dev_id']
                            device_name = device['dev_name']
                            
                            # Clean device name for column names
                            clean_device_name = ''
                            for char in device_name:
                                if char.isalnum():
                                    clean_device_name += char
                                else:
                                    clean_device_name += '_'
                            
                            # Ensure the name doesn't start with a number
                            if clean_device_name and clean_device_name[0].isdigit():
                                clean_device_name = 'dev_' + clean_device_name
                            
                            # Add columns for each measurement point
                            for point in points:
                                point_desc = POINT_DESCRIPTIONS.get(point, point)
                                column_name = f"{clean_device_name}_{point_desc}"
                                
                                if column_name in used_column_names:
                                    # Add a suffix to avoid duplicate column names
                                    column_name = f"{column_name}_{device_id.replace('-', '_')}"
                                
                                used_column_names.add(column_name)
                                
                                # Extract numeric value from JSON
                                column_def = f"""
                                MAX(CASE WHEN base.dev_id = '{device_id}' 
                                    THEN CASE 
                                        WHEN base.measurement_data::jsonb ? {repr(point)} AND
                                             (base.measurement_data::jsonb->{repr(point)})::text ~ '^-?\\d+(\\.\\d+)?$' 
                                        THEN (base.measurement_data::jsonb->{repr(point)})::NUMERIC 
                                        ELSE NULL 
                                    END 
                                    ELSE NULL 
                                END) AS "{column_name}"
                                """
                                column_defs.append(column_def)
                    
                    # Create the pivoted full day view
                    pivoted_full_day_sql = f"""
                    DROP VIEW IF EXISTS "{pivoted_full_day_view_name}" CASCADE;
                    
                    CREATE OR REPLACE VIEW "{pivoted_full_day_view_name}" AS
                    SELECT 
                        base.timestamp,
                        {', '.join(column_defs)}
                    FROM 
                        "{full_day_view_name}" base
                    GROUP BY 
                        base.timestamp
                    ORDER BY
                        base.timestamp;
                    """
                    
                    # Execute the pivoted view creation
                    logger.info(f"Creating pivoted full day view: {pivoted_full_day_view_name}")
                    conn.execute(text(pivoted_full_day_sql))
                    
                    # Verify the views exist and get row counts
                    for view_name in [full_day_view_name, pivoted_full_day_view_name]:
                        verify_query = text(f'SELECT COUNT(*) FROM "{view_name}"')
                        count = conn.execute(verify_query).scalar()
                        logger.info(f"View {view_name} contains {count} rows")
                    
                    # Create specialized views for inverters
                    if device_type_name == 'inverter':
                        try:
                            # 1. Create inverter summary view with 5-minute intervals if requested
                            if create_inverter_summary:
                                summary_view_name = f"{clean_plant_name}_inverter_summary_full_day"
                                logger.info(f"Creating inverter summary view: {summary_view_name}")
                                
                                # Get summary points from our function
                                summary_points_raw = get_device_points_by_type(dev_type_id, "summary")
                                
                                # Format points with aliases as needed - extract from JSON
                                summary_points = [
                                    "h.measurement_data->>'day_cap' as daily_energy_yield",
                                    "h.measurement_data->>'active_power' as active_power",
                                    "h.measurement_data->>'reactive_power' as output_reactive_power",
                                    "h.measurement_data->>'mppt_total_cap' as total_dc_input_energy",
                                    "NULL::numeric as mppt_power"  # Placeholder if it doesn't exist
                                ]
                                
                                # Log the points for debugging
                                logger.debug(f"Using summary points: {summary_points}")
                                
                                # Create SQL for the summary view (with 5-minute intervals)
                                summary_view_sql = f"""
                                DROP VIEW IF EXISTS "{summary_view_name}" CASCADE;
                                
                                CREATE OR REPLACE VIEW "{summary_view_name}" AS
                                WITH time_series AS (
                                    SELECT 
                                        generate_series(
                                            COALESCE(
                                                (SELECT MIN(date_trunc('day', collect_time)::timestamp) 
                                                 FROM fusionsolar_historical_data 
                                                 WHERE dev_id IN ({device_ids_str})),
                                                '2025-05-01'::timestamp
                                            ), 
                                            COALESCE(
                                                (SELECT MAX(date_trunc('day', collect_time)::timestamp) 
                                                 FROM fusionsolar_historical_data 
                                                 WHERE dev_id IN ({device_ids_str})),
                                                '2025-05-04'::timestamp
                                            ) + INTERVAL '23 hours 55 minutes',
                                            INTERVAL '5 minutes'
                                        ) AS timestamp
                                ),
                                base_data AS (
                                    SELECT 
                                        h.dev_id,
                                        d.dev_name as device_name,
                                        h.collect_time as timestamp,
                                        {', '.join(summary_points)}
                                    FROM 
                                        fusionsolar_historical_data h
                                    JOIN 
                                        fusionsolar_devices d ON h.dev_id = d.dev_id
                                    WHERE 
                                        h.dev_id IN ({device_ids_str})
                                )
                                SELECT 
                                    ts.timestamp,
                                    d.device_name,
                                    d.daily_energy_yield,
                                    d.active_power,
                                    d.output_reactive_power,
                                    d.total_dc_input_energy,
                                    d.mppt_power
                                FROM 
                                    time_series ts
                                LEFT JOIN 
                                    base_data d ON ts.timestamp = d.timestamp
                                ORDER BY 
                                    ts.timestamp;
                                """
                                
                                # Execute the summary view creation
                                try:
                                    conn.execute(text(summary_view_sql))
                                    logger.info(f"Successfully created summary view: {summary_view_name}")
                                    
                                    # Verify the view exists
                                    try:
                                        verify_query = text(f'SELECT COUNT(*) FROM "{summary_view_name}"')
                                        count = conn.execute(verify_query).scalar()
                                        logger.info(f"View {summary_view_name} contains {count} rows")
                                        
                                        # Create a pivoted version of the summary view
                                        pivoted_summary_view_name = f"{clean_plant_name}_inverter_summary_pivoted_full_day"
                                        logger.info(f"Creating pivoted summary view: {pivoted_summary_view_name}")
                                        logger.debug(f"Using plant name: '{plant_name}', clean name: '{clean_plant_name}'")
                                        logger.debug(f"Found {len(devices_of_type)} devices for pivoting")
                                        
                                        # Create column definitions for each device
                                        summary_column_defs = []
                                        for device in devices_of_type:
                                            device_id = device['dev_id']
                                            device_name = device['dev_name'].replace(' ', '_')
                                            
                                            # Add column for daily energy yield
                                            summary_column_defs.append(f"""
                                            MAX(CASE WHEN base.device_name = '{device['dev_name']}' THEN base.daily_energy_yield ELSE NULL END) AS "{device_name}_daily_energy_yield"
                                            """)
                                            
                                            # Add column for active power
                                            summary_column_defs.append(f"""
                                            MAX(CASE WHEN base.device_name = '{device['dev_name']}' THEN base.active_power ELSE NULL END) AS "{device_name}_active_power"
                                            """)
                                            
                                            # Add column for reactive power
                                            summary_column_defs.append(f"""
                                            MAX(CASE WHEN base.device_name = '{device['dev_name']}' THEN base.output_reactive_power ELSE NULL END) AS "{device_name}_reactive_power"
                                            """)
                                            
                                            # Add column for total DC input energy
                                            summary_column_defs.append(f"""
                                            MAX(CASE WHEN base.device_name = '{device['dev_name']}' THEN base.total_dc_input_energy ELSE NULL END) AS "{device_name}_total_dc_input"
                                            """)
                                        
                                        # Create the pivoted summary view SQL
                                        pivoted_summary_sql = f"""
                                        DROP VIEW IF EXISTS "{pivoted_summary_view_name}" CASCADE;
                                        
                                        CREATE OR REPLACE VIEW "{pivoted_summary_view_name}" AS
                                        SELECT 
                                            base.timestamp,
                                            {', '.join(summary_column_defs)}
                                        FROM 
                                            "{summary_view_name}" base
                                        GROUP BY 
                                            base.timestamp
                                        ORDER BY
                                            base.timestamp;
                                        """
                                        
                                        # Execute the pivoted view creation
                                        try:
                                            conn.execute(text(pivoted_summary_sql))
                                            logger.info(f"Successfully created pivoted summary view: {pivoted_summary_view_name}")
                                            
                                            # Verify the view exists
                                            pivoted_verify_query = text(f'SELECT COUNT(*) FROM "{pivoted_summary_view_name}"')
                                            pivoted_count = conn.execute(pivoted_verify_query).scalar()
                                            logger.info(f"View {pivoted_summary_view_name} contains {pivoted_count} rows")
                                        except Exception as pe:
                                            logger.error(f"Error creating pivoted summary view: {pe}")
                                            logger.error(f"Pivoted SQL: {pivoted_summary_sql}")
                                    except Exception as ve:
                                        logger.error(f"Error verifying summary view: {ve}")
                                except Exception as e:
                                    logger.error(f"Error creating summary view: {e}")
                                    logger.error(f"SQL: {summary_view_sql}")
                                
                            # 2. Create MPPT view with 5-minute intervals if requested
                            if create_inverter_mppt:
                                mppt_view_name = f"{clean_plant_name}_inverter_mppt_full_day"
                                logger.info(f"Creating inverter MPPT view: {mppt_view_name}")
                                
                                # Get MPPT points from our function
                                mppt_points_raw = get_device_points_by_type(dev_type_id, "mppt")
                                
                                # Format with aliases - extract from JSON
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
                                
                                # Log the points for debugging
                                logger.debug(f"Using MPPT points: {mppt_points}")
                                
                                # Create SQL for the MPPT view (with 5-minute intervals)
                                mppt_view_sql = f"""
                                DROP VIEW IF EXISTS "{mppt_view_name}" CASCADE;
                                
                                CREATE OR REPLACE VIEW "{mppt_view_name}" AS
                                WITH time_series AS (
                                    SELECT 
                                        generate_series(
                                            COALESCE(
                                                (SELECT MIN(date_trunc('day', collect_time)::timestamp) 
                                                 FROM fusionsolar_historical_data 
                                                 WHERE dev_id IN ({device_ids_str})),
                                                '2025-05-01'::timestamp
                                            ), 
                                            COALESCE(
                                                (SELECT MAX(date_trunc('day', collect_time)::timestamp) 
                                                 FROM fusionsolar_historical_data 
                                                 WHERE dev_id IN ({device_ids_str})),
                                                '2025-05-04'::timestamp
                                            ) + INTERVAL '23 hours 55 minutes',
                                            INTERVAL '5 minutes'
                                        ) AS timestamp
                                ),
                                base_data AS (
                                    SELECT 
                                        h.dev_id,
                                        d.dev_name as device_name,
                                        h.collect_time as timestamp,
                                        {', '.join(mppt_points)}
                                    FROM 
                                        fusionsolar_historical_data h
                                    JOIN 
                                        fusionsolar_devices d ON h.dev_id = d.dev_id
                                    WHERE 
                                        h.dev_id IN ({device_ids_str})
                                )
                                SELECT 
                                    ts.timestamp,
                                    d.device_name,
                                    d.mppt1_dc_total_yield,
                                    d.mppt2_dc_total_yield,
                                    d.mppt3_dc_total_yield,
                                    d.mppt4_dc_total_yield,
                                    d.mppt5_dc_total_yield,
                                    d.mppt6_dc_total_yield,
                                    d.mppt7_dc_total_yield,
                                    d.mppt8_dc_total_yield,
                                    d.mppt9_dc_total_yield,
                                    d.mppt10_dc_total_yield
                                FROM 
                                    time_series ts
                                LEFT JOIN 
                                    base_data d ON ts.timestamp = d.timestamp
                                ORDER BY 
                                    ts.timestamp;
                                """
                                
                                # Execute the MPPT view creation
                                try:
                                    conn.execute(text(mppt_view_sql))
                                    logger.info(f"Successfully created MPPT view: {mppt_view_name}")
                                    
                                    # Verify the view exists
                                    try:
                                        verify_query = text(f'SELECT COUNT(*) FROM "{mppt_view_name}"')
                                        count = conn.execute(verify_query).scalar()
                                        logger.info(f"View {mppt_view_name} contains {count} rows")
                                        
                                        # Create a pivoted version of the MPPT view
                                        pivoted_mppt_view_name = f"{clean_plant_name}_inverter_mppt_pivoted_full_day"
                                        logger.info(f"Creating pivoted MPPT view: {pivoted_mppt_view_name}")
                                        logger.debug(f"Using plant name: '{plant_name}', clean name: '{clean_plant_name}'")
                                        logger.debug(f"Found {len(devices_of_type)} devices for pivoting")
                                        
                                        # Create column definitions for each device
                                        mppt_column_defs = []
                                        for device in devices_of_type:
                                            device_id = device['dev_id']
                                            device_name = device['dev_name'].replace(' ', '_')
                                            
                                            # Add column for each MPPT channel
                                            for i in range(1, 11):  # MPPT 1-10
                                                mppt_column_defs.append(f"""
                                                MAX(CASE WHEN base.device_name = '{device['dev_name']}' THEN base.mppt{i}_dc_total_yield ELSE NULL END) AS "{device_name}_mppt{i}_yield"
                                                """)
                                        
                                        # Create the pivoted MPPT view SQL
                                        pivoted_mppt_sql = f"""
                                        DROP VIEW IF EXISTS "{pivoted_mppt_view_name}" CASCADE;
                                        
                                        CREATE OR REPLACE VIEW "{pivoted_mppt_view_name}" AS
                                        SELECT 
                                            base.timestamp,
                                            {', '.join(mppt_column_defs)}
                                        FROM 
                                            "{mppt_view_name}" base
                                        GROUP BY 
                                            base.timestamp
                                        ORDER BY
                                            base.timestamp;
                                        """
                                        
                                        # Execute the pivoted view creation
                                        try:
                                            conn.execute(text(pivoted_mppt_sql))
                                            logger.info(f"Successfully created pivoted MPPT view: {pivoted_mppt_view_name}")
                                            
                                            # Verify the view exists
                                            pivoted_verify_query = text(f'SELECT COUNT(*) FROM "{pivoted_mppt_view_name}"')
                                            pivoted_count = conn.execute(pivoted_verify_query).scalar()
                                            logger.info(f"View {pivoted_mppt_view_name} contains {pivoted_count} rows")
                                        except Exception as pe:
                                            logger.error(f"Error creating pivoted MPPT view: {pe}")
                                            logger.error(f"Pivoted SQL: {pivoted_mppt_sql}")
                                    except Exception as ve:
                                        logger.error(f"Error verifying MPPT view: {ve}")
                                except Exception as e:
                                    logger.error(f"Error creating MPPT view: {e}")
                                    logger.error(f"SQL: {mppt_view_sql}")
                                
                        except Exception as e:
                            logger.error(f"Error creating specialized inverter views: {e}")
                            logger.debug(f"Error details: {str(e)}")
                            # Continue with other device types even if specialized views fail
                
                except Exception as e:
                    logger.error(f"Error creating views for {device_type_name}: {e}")
        
        logger.info("Full day device views creation completed")
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Create full day (00:00-23:55) SQL views with pivoted device data")
    
    # Site selection parameters
    parser.add_argument("--site", help="Site code (e.g., 'NE=50488260'). If not provided, all sites will be processed.")
    parser.add_argument("--name", help="Friendly name to use in view (e.g., 'MMKI_phase_1')")
    
    # View selection parameters
    view_group = parser.add_argument_group('View Selection')
    view_group.add_argument("--inverter", action="store_true", help="Create inverter string-level views")
    view_group.add_argument("--inverter-summary", action="store_true", help="Create inverter summary views (yield today, active/reactive power, etc.)")
    view_group.add_argument("--inverter-mppt", action="store_true", help="Create inverter MPPT views (mppt_1_cap through mppt_10_cap)")
    view_group.add_argument("--meter", action="store_true", help="Create meter views")
    view_group.add_argument("--meteo-station", action="store_true", help="Create meteo station views")
    view_group.add_argument("--all", action="store_true", help="Create all views (default if no specific view type is selected)")
    
    # Advanced options
    advanced_group = parser.add_argument_group('Advanced Options')
    advanced_group.add_argument("--no-drop-existing", action="store_true", help="Do not drop existing views before creating new ones")
    advanced_group.add_argument("--list-plants", action="store_true", help="List all available plants and exit")
    advanced_group.add_argument("--debug", action="store_true", help="Print detailed debug information")
    
    args = parser.parse_args()
    
    # Set up logging with detailed format for debugging
    logging_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    # Force the logger to use debug level if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        # Also set the root logger
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Check if we should list plants and exit
    if args.list_plants:
        try:
            # Create database engine
            engine = init_database()
            
            if not engine:
                logger.error("Database initialization failed")
                return 1
                
            with engine.connect() as conn:
                # Query to get all plants
                query = text("""SELECT DISTINCT plant_code FROM fusionsolar_devices""")
                result = conn.execute(query)
                sites = [row[0] for row in result]
                
                # Get plant names
                plant_names = {}
                for site_code in sites:
                    # Try to get plant name from the database
                    name_query = text("""
                        SELECT plant_name FROM fusionsolar_plants WHERE plant_code = :site_code
                    """)
                    try:
                        result = conn.execute(name_query, {"site_code": site_code})
                        plant_name = result.scalar()
                        if plant_name:
                            plant_names[site_code] = plant_name
                        else:
                            plant_names[site_code] = site_code
                    except:
                        plant_names[site_code] = site_code
                
                print("Available plants:")
                for site_code in sorted(sites):
                    print(f"  - {site_code}: {plant_names.get(site_code, site_code)}")
                    
            return 0
            
        except Exception as e:
            logger.error(f"Error listing plants: {e}")
            return 1
    
    # Determine which views to create
    create_inverter = args.inverter or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_inverter_summary = args.inverter_summary or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_inverter_mppt = args.inverter_mppt or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_meter = args.meter or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    create_meteo = args.meteo_station or args.all or (not any([args.inverter, args.inverter_summary, args.inverter_mppt, args.meter, args.meteo_station]))
    
    # Create full day device views with the specified options
    create_full_day_device_views(
        site_code=args.site,
        friendly_name=args.name,
        create_inverter=create_inverter,
        create_inverter_summary=create_inverter_summary,
        create_inverter_mppt=create_inverter_mppt,
        create_meter=create_meter,
        create_meteo=create_meteo,
        drop_existing=not args.no_drop_existing
    )

if __name__ == "__main__":
    main()
