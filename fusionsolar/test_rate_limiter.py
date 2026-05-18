import logging
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("rate_limiter_test.log"),
        logging.StreamHandler()
    ]
)

# Import the rate limiter
from fusionsolar_harvester_src.fusionsolar_rate_limiter import FusionSolarRateLimiter

def simulate_api_calls(device_type, num_calls):
    """Simulate making API calls to test rate limiting."""
    rate_limiter = FusionSolarRateLimiter()
    
    # Update sample device counts
    device_counts = {
        "inverter": 50,
        "meter": 20,
        "meteo_station": 5
    }
    rate_limiter.update_device_counts(device_counts)
    
    # Calculate the daily limit
    daily_limit = rate_limiter.calculate_daily_limit(device_type)
    logging.info(f"Daily limit for {device_type}: {daily_limit} API calls")
    
    # Make simulated API calls
    for i in range(num_calls):
        if rate_limiter.can_make_api_call(device_type):
            logging.info(f"Making API call #{i+1} for {device_type}")
            rate_limiter.record_api_call(device_type)
            calls_made = rate_limiter.get_calls_today(device_type)
            calls_remaining = rate_limiter.get_remaining_calls(device_type)
            logging.info(f"API calls for {device_type}: {calls_made} made, {calls_remaining} remaining")
        else:
            logging.warning(f"Rate limit reached for {device_type} after {i} calls")
            
            # Try waiting
            logging.info(f"Attempting to wait for rate limit reset...")
            if rate_limiter.wait_for_api_call(device_type, max_wait_time=5):
                logging.info(f"Wait successful, continuing with API calls")
            else:
                logging.error(f"Wait unsuccessful, would need to wait until tomorrow")
                break

def test_progress_tracking():
    """Test the progress tracking functionality."""
    rate_limiter = FusionSolarRateLimiter()
    
    # Create a test progress key
    progress_key = rate_limiter.get_progress_tracker_key(
        "2025-05-01", 
        "2025-05-26",
        {
            "plant_codes": "NE=54435794,NE=51758766",
            "device_types": "inverter,meter"
        }
    )
    
    # Update progress a few times
    dates = ["2025-05-01", "2025-05-04", "2025-05-07", "2025-05-10"]
    for date in dates:
        logging.info(f"Updating progress to date: {date}")
        rate_limiter.update_fetch_progress(progress_key, date, ["inverter", "meter"])
        
        # Check the progress
        progress = rate_limiter.get_fetch_progress(progress_key)
        logging.info(f"Current progress: {progress}")
    
    # Clear the progress
    logging.info("Clearing progress")
    rate_limiter.clear_fetch_progress(progress_key)
    
    # Verify it was cleared
    progress = rate_limiter.get_fetch_progress(progress_key)
    logging.info(f"Progress after clearing: {progress}")

def main():
    parser = argparse.ArgumentParser(description="Test the FusionSolar rate limiter")
    parser.add_argument("--test-api-calls", action="store_true", help="Test API call tracking and limits")
    parser.add_argument("--device-type", type=str, default="inverter", help="Device type to use for API call test")
    parser.add_argument("--num-calls", type=int, default=30, help="Number of API calls to simulate")
    parser.add_argument("--test-progress", action="store_true", help="Test progress tracking")
    
    args = parser.parse_args()
    
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    if args.test_api_calls:
        simulate_api_calls(args.device_type, args.num_calls)
    
    if args.test_progress:
        test_progress_tracking()

if __name__ == "__main__":
    main()
