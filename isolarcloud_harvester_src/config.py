import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# iSolarCloud API Configuration
ISOLARCLOUD_BASE_URL = "https://gateway.isolarcloud.com.hk"
ISOLARCLOUD_APP_KEY = os.getenv("ISOLARCLOUD_APP_KEY")
ISOLARCLOUD_SECRET_KEY = os.getenv("ISOLARCLOUD_SECRET_KEY")
ISOLARCLOUD_USERNAME = os.getenv("ISOLARCLOUD_USERNAME")
ISOLARCLOUD_PASSWORD = os.getenv("ISOLARCLOUD_PASSWORD")
SYS_CODE = "901"  # Provided in the API documentation

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
print(f"DEBUG: SUPABASE_URL='{SUPABASE_URL}'") # Temporary debug
print(f"DEBUG: SUPABASE_ANON_KEY='{SUPABASE_ANON_KEY}'") # Temporary debug

# Script Constants
REQUEST_DELAY_SECONDS = 2  # Seconds to wait between API calls
MAX_PS_KEYS_PER_REQUEST = 50  # Max ps_key_list length for getDevicePointMinuteDataList
MAX_POINTS_PER_REQUEST = 50     # Max points length for getDevicePointMinuteDataList
DAYS_PER_HISTORICAL_BATCH = 7 # Number of days to fetch in a single batch for long historical requests
API_CALLS_PER_HOUR_LIMIT = 2000 # For reference, not directly used in delay calculation logic yet

# --- Configuration for Measuring Points ---
DEVICE_TYPE_MEASURING_POINTS = {
    "inverter": {
        "points": ["p1", "p96","p97","p98","p99","p100","p101","p102",
                    "p103","p104","p105","p106","p107","p108","p109",
                    "p110","p111","p112","p113","p70","p71","p72","p73",
                    "p74","p75","p76","p77","p78","p79","p80","p81","p82",
                    "p83","p84","p85","p86","p87","p88","p89","p90","p91","p92","p93"], 
        "api_device_type_code": 1 
    },
    "meteo_station": {
        "points": ["p2003"], 
        "api_device_type_code": 5 
    },
    "meter": {
        "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
        "api_device_type_code": 7 
    }
}

def _parse_point_range(point_range_str):
    """Parses a point range string like "p96-p115" into a list of points ["p96", "p97", ..., "p115"]."""
    if "-" in point_range_str:
        prefix = point_range_str[0]
        start_str, end_str = point_range_str[1:].split("-")
        try:
            start = int(start_str)
            end = int(end_str)
            return [f"{prefix}{i}" for i in range(start, end + 1)]
        except ValueError:
            logging.warning(f"Could not parse point range: {point_range_str}. Returning as is.")
            return [point_range_str] 
    return [point_range_str]

def get_measuring_points_for_device_type(device_type_name):
    """Returns a flat list of measuring points for a given device type name (e.g., 'inverter')."""
    config = DEVICE_TYPE_MEASURING_POINTS.get(device_type_name.lower())
    if not config:
        logging.warning(f"No measuring point configuration found for device type: {device_type_name}")
        return []
    
    all_points = []
    for point_or_range in config.get("points", []):
        all_points.extend(_parse_point_range(point_or_range))
    return all_points

