import os
import logging
import urllib.parse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# FusionSolar API Configuration
FUSIONSOLAR_BASE_URL = os.getenv("FUSION_BASE_URL", "https://sg5.fusionsolar.huawei.com")
FUSIONSOLAR_USERNAME = os.getenv("FUSIONSOLAR_USERNAME")
FUSIONSOLAR_PASSWORD = os.getenv("FUSIONSOLAR_PASSWORD")  # This is the systemCode in the API

# Debug prints for environment variables
print(f"Using FusionSolar API URL: {FUSIONSOLAR_BASE_URL}")
print(f"Using FusionSolar Username: {FUSIONSOLAR_USERNAME}")
print(f"Using FusionSolar Password: {'*' * (len(FUSIONSOLAR_PASSWORD) if FUSIONSOLAR_PASSWORD else 0)}")

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL from individual PostgreSQL settings
# URL encode the password to handle special characters
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
logging.info(f"Database URL constructed: postgresql://{POSTGRES_USER}:***@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# Script Constants
REQUEST_DELAY_SECONDS = 2  # Seconds to wait between API calls
MAX_DEVICES_PER_REQUEST = 10  # Max devices per request according to FusionSolar API docs
MAX_DAYS_PER_REQUEST = 3  # Max 3-day window for historical data
MAX_PLANTS_PER_REQUEST = 100  # Max plants in getDevList API call

# Device Type IDs from FusionSolar API
DEVICE_TYPES = {
    "inverter": 1,  # String Inverter
    "meteo_station": 10,  # EMI (Environmental Monitoring Instrument)
    "meter": 17,  # Grid Meter
}

# Point descriptions for better column naming in SQL views
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
    "power_factor": "power_factor",                   # Power factor (None)
    "reverse_active_cap": "negative_active_energy",   # Negative active energy (W)
    "reverse_reactive_cap": "negative_reactive_energy", # Negative reactive energy (W)
    "active_cap": "positive_active_energy",           # Positive active energy (W)
    "forward_reactive_cap": "positive_reactive_energy", # Positive reactive energy (W)
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

# Define measurement points to fetch for each device type
DEVICE_TYPE_MEASURING_POINTS = {
    "inverter": [
        "inverter_state", "ab_u", "bc_u", "ca_u", "a_u", "b_u", "c_u", 
        "a_i", "b_i", "c_i", "efficiency", "temperature", "power_factor", 
        "elec_freq", "active_power", "reactive_power", "day_cap", "total_cap",
        # PV string inputs
        "pv1_u", "pv2_u", "pv3_u", "pv4_u", "pv5_u", "pv6_u", "pv7_u", "pv8_u",
        "pv9_u", "pv10_u", "pv11_u", "pv12_u", "pv13_u", "pv14_u", "pv15_u", "pv16_u",
        "pv17_u", "pv18_u", "pv19_u", "pv20_u", "pv21_u", "pv22_u", "pv23_u", "pv24_u",
        "pv1_i", "pv2_i", "pv3_i", "pv4_i", "pv5_i", "pv6_i", "pv7_i", "pv8_i",
        "pv9_i", "pv10_i", "pv11_i", "pv12_i", "pv13_i", "pv14_i", "pv15_i", "pv16_i",
        "pv17_i", "pv18_i", "pv19_i", "pv20_i", "pv21_i", "pv22_i", "pv23_i", "pv24_i",
        # MPPT yields
        "mppt_1_cap", "mppt_2_cap", "mppt_3_cap", "mppt_4_cap", "mppt_5_cap",
        "mppt_6_cap", "mppt_7_cap", "mppt_8_cap", "mppt_9_cap", "mppt_10_cap",
        "mppt_total_cap", "open_time", "close_time"
    ],
    "meter": [
        "active_power", "power_factor", "reverse_active_cap", "reverse_reactive_cap", 
        "active_cap", "forward_reactive_cap", "grid_frequency", "ab_u", "bc_u", "ca_u", 
        "a_u", "b_u", "c_u", "a_i", "b_i", "c_i", "meter_status", "total_apparent_power",
        "reverse_active_peak", "reverse_active_power", "reverse_active_valley"
    ],
    "meteo_station": [
        "temperature", "pv_temperature", "radiant_line", "radiant_total", 
        "horiz_radiant_line", "horiz_radiant_total", "wind_speed", "wind_direction", 
        "rainfall", "humidity"
    ]
}
