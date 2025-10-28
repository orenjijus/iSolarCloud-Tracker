import os
import logging
from dotenv import load_dotenv

import sys
sys.stdin.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(encoding='utf-8')


# Load environment variables from .env file
load_dotenv()

# iSolarCloud API Configuration
ISOLARCLOUD_BASE_URL = "https://gateway.isolarcloud.com.hk"
ISOLARCLOUD_APP_KEY = os.getenv("ISOLARCLOUD_APP_KEY")
ISOLARCLOUD_SECRET_KEY = os.getenv("ISOLARCLOUD_SECRET_KEY")
ISOLARCLOUD_USERNAME = os.getenv("ISOLARCLOUD_USERNAME")
ISOLARCLOUD_PASSWORD = os.getenv("ISOLARCLOUD_PASSWORD")
SYS_CODE = "901"  # Provided in the API documentation

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
logging.info(f"Database URL constructed: postgresql://{POSTGRES_USER}:***@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# Script Constants
REQUEST_DELAY_SECONDS = 2  # Seconds to wait between API calls
MAX_PS_KEYS_PER_REQUEST = 50  # Max ps_key_list length for getDevicePointMinuteDataList
MAX_POINTS_PER_REQUEST = 50     # Max points length for getDevicePointMinuteDataList
DAYS_PER_HISTORICAL_BATCH = 7 # Number of days to fetch in a single batch for long historical requests
API_CALLS_PER_HOUR_LIMIT = 2000 # For reference, not directly used in delay calculation logic yet

# --- Configuration for Measuring Points ---
# Point descriptions for better column naming in SQL views
POINT_DESCRIPTIONS = {
    # Meter points
    "p8030": "forward_active_energy_Wh",
    "p8031": "reverse_active_energy_Wh",
    "p8032": "forward_reactive_energy_varh",
    "p8033": "reverse_reactive_energy_varh",
    "p8014": "power_factor",
    "p8018": "active_power",
    
    # Meteo station points
    "p2003": "Transient Horizontal Irradiation _W_per_m2",
    "p2001": "Daily Horizontal Irradiation_W_per_m2",
    "p2007": "Slope Transient Irradiation_W_per_m2",
    "p2005": "Slope Daily Irradiation_W_per_m2",
    "p2009": "Ambient Temperature_C",
    "p2010": "PV Temperature_C",
    "p2022": "Rainfall_mm",

    # String voltage points (p96-p113, p7166-p7171)
    "p96": "string_1_voltage",
    "p97": "string_2_voltage",
    "p98": "string_3_voltage",
    "p99": "string_4_voltage",
    "p100": "string_5_voltage",
    "p101": "string_6_voltage",
    "p102": "string_7_voltage",
    "p103": "string_8_voltage",
    "p104": "string_9_voltage",
    "p105": "string_10_voltage",
    "p106": "string_11_voltage",
    "p107": "string_12_voltage",
    "p108": "string_13_voltage",
    "p109": "string_14_voltage",
    "p110": "string_15_voltage",
    "p111": "string_16_voltage",
    "p112": "string_17_voltage",
    "p113": "string_18_voltage",
    "p7166": "string_19_voltage",
    "p7167": "string_20_voltage",
    "p7168": "string_21_voltage",
    "p7169": "string_22_voltage",
    "p7170": "string_23_voltage",
    "p7171": "string_24_voltage",
    
    # String current points (p70-p93, p313-p318)
    "p70": "string_1_current",
    "p71": "string_2_current",
    "p72": "string_3_current",
    "p73": "string_4_current",
    "p74": "string_5_current",
    "p75": "string_6_current",
    "p76": "string_7_current",
    "p77": "string_8_current",
    "p78": "string_9_current",
    "p79": "string_10_current",
    "p80": "string_11_current",
    "p81": "string_12_current",
    "p82": "string_13_current",
    "p83": "string_14_current",
    "p84": "string_15_current",
    "p85": "string_16_current",
    "p92": "string_17_current",
    "p93": "string_18_current",
    "p313": "string_19_current",
    "p314": "string_20_current",
    "p315": "string_21_current",
    "p316": "string_22_current",
    "p317": "string_23_current",
    "p318": "string_24_current",
    
    # Inverter-level data points
    "p1": "yield_today_kWh",
    "p14": "total_dc_power_W",
    "p24": "total_active_power_W",
    "p25": "total_reactive_power_var"
}

# Site-specific measurement points configuration
ISOLARCLOUD_SITE_MEASURING_POINTS = {
    "Garuda Metalindo (IKP)": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010","p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "Garuda Metalindo (MPF)": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010","p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "Garuda Metalindo 1": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010", "p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "Garuda Metalindo 2": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010", "p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "Shoetown Ligung Indonesia": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010", "p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "Charoen Pokphand Majalengka": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010", "p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "Charoen Pokphand Bandung": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010", "p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "Charoen Pokphand Madiun": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010", "p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    },
    "PLTS Frina Lestari Nusantara": {
        "inverter": {
            "points": [
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", "p105", "p106", "p107", "p108", 
                "p109", "p110", "p111", "p112", "p113", "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85", "p92", "p93", "p7166", "p7167", "p7168", 
                "p7169", "p7170", "p7171", "p313", "p314", "p315", "p316", "p317", "p318"
            ],
            "device_type": 1
        },

        "meteo_station": {
            "points": ["p2003", "p2001", "p2005", "p2007", "p2009", "p2010", "p2022"],
            "device_type": 5
        },
        "meter": {
            "points": ["p8030", "p8031", "p8032", "p8033", "p8018", "p8014"],
            "device_type": 7
        }
    }
}

DEVICE_TYPE_MEASURING_POINTS = {
    "inverter": {
        "points": [
                # String voltages 1-18
                "p96", "p97", "p98", "p99", "p100", "p101", "p102", "p103", "p104", 
                "p105", "p106", "p107", "p108", "p109", "p110", "p111", "p112", "p113",
                # String voltages 19-24 
                "p7166", "p7167", "p7168", "p7169", "p7170", "p7171",
                # String currents 1-16
                "p70", "p71", "p72", "p73", "p74", "p75", "p76", "p77", 
                "p78", "p79", "p80", "p81", "p82", "p83", "p84", "p85",
                # String currents 17-18
                "p92", "p93",
                # String currents 19-24
                "p313", "p314", "p315", "p316", "p317", "p318"  # p313 - p318  (Strings 19-24 Current)
        ], 
        "device_type": 1 
    },
    "inverter_summary": {
        "points": [
                # Inverter-level data points
                "p1",   # Yield Today (kWh)
                "p14",  # Total DC Power (W)
                "p24",  # Total Active Power (W)
                "p25"   # Total Reactive Power (var)
        ],
        "device_type": 1 
    },
    "meteo_station": {
        "points": [
            "p2003", # p2003 (irradiance_W_per_m2), 
            "p2001", # p2001 (daily_irradiation_W_per_m2), 
            "p2005", # p2005 (slope_daily_irradiation_W_per_m2), 
            "p2007", # p2007 (transient_daily_irradiation_W_per_m2), 
            "p2009", # p2009 (ambient_temperature_C), 
            "p2010", # p2010 (pv_temperature_C)
            "p2022"  # p2022 (rainfall_mm)
        ],
        "device_type": 5
    },
    "meter": {
        "points": [
            "p8030", # p8030 (positive_active_energy_Wh),
            "p8031", # p8031 (negative_active_energy_Wh),
            "p8032", # p8032 (positive_reactive_energy_varh),
            "p8033", # p8033 (negative_reactive_energy_varh),
            "p8018", # p8018 (active_power_W),
            "p8014" # p8014 (power_factor),
        ],
        "device_type": 7 
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

