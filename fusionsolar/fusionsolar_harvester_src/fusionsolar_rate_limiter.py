import os
import json
import time
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

class FusionSolarRateLimiter:
    """
    Rate limiter for FusionSolar API to ensure compliance with rate limits.
    
    Historical Device Data API rate limits:
    - Max 10 devices of the same type can be queried within a 3-day window
    - Maximum daily calls = Roundup(Number of devices of each type / 10) + 24
    """
    
    def __init__(self, state_file_path=None, test_mode=False):
        """
        Initialize the rate limiter.
        
        Args:
            state_file_path (str, optional): Path to store the rate limiter state.
            test_mode (bool, optional): If True, uses shorter wait times for testing.
        """
        self.test_mode = test_mode
        """
        Initialize the rate limiter.
        
        Args:
            state_file_path (str, optional): Path to store the rate limiter state.
                If None, uses the default path in the same directory as this file.
        """
        if state_file_path is None:
            # Default state file in the same directory as this module
            base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
            state_file_path = base_dir / "rate_limiter_state.json"
            
        self.state_file_path = state_file_path
        self.state = self._load_state()
        
        # Ensure the state has the required structure
        if 'historical_api' not in self.state:
            self.state['historical_api'] = {
                'calls_by_date': {},
                'device_counts': {},
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            
        # Clean up old dates (older than 7 days)
        self._cleanup_old_dates()
        
    def _load_state(self):
        """Load rate limiter state from file."""
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logging.warning(f"Failed to load rate limiter state: {e}. Creating new state.")
        
        # Default state
        return {
            'historical_api': {
                'calls_by_date': {},
                'device_counts': {},
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
        }
    
    def _save_state(self):
        """Save rate limiter state to file."""
        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(self.state, f, indent=2)
        except IOError as e:
            logging.error(f"Failed to save rate limiter state: {e}")
    
    def _cleanup_old_dates(self):
        """Remove data for dates older than 7 days."""
        now = datetime.now(timezone.utc)
        cutoff_date = (now - timedelta(days=7)).strftime('%Y-%m-%d')
        
        calls_by_date = self.state['historical_api']['calls_by_date']
        dates_to_remove = [date for date in calls_by_date if date < cutoff_date]
        
        for date in dates_to_remove:
            del calls_by_date[date]
        
        self._save_state()
    
    def update_device_counts(self, device_counts_by_type):
        """
        Update the device count information.
        
        Args:
            device_counts_by_type (dict): Dictionary mapping device type names to counts.
                Example: {'inverter': 50, 'meter': 20, 'meteo_station': 5}
        """
        self.state['historical_api']['device_counts'] = device_counts_by_type
        self.state['historical_api']['last_updated'] = datetime.now(timezone.utc).isoformat()
        self._save_state()
    
    def calculate_daily_limit(self, device_type):
        """
        Calculate the daily API call limit for a device type.
        
        Args:
            device_type (str): Device type name (e.g., 'inverter', 'meter')
            
        Returns:
            int: Daily API call limit for the device type
        """
        device_counts = self.state['historical_api']['device_counts']
        if device_type not in device_counts:
            # Default to 24 if we don't know the device count
            return 24
        
        # Maximum daily calls = Roundup(Number of devices of each type / 10) + 24
        device_count = device_counts[device_type]
        return math.ceil(device_count / 10) + 24
    
    def _get_today_date(self):
        """Get today's date as a string in YYYY-MM-DD format."""
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    def record_api_call(self, device_type):
        """
        Record an API call for a device type.
        
        Args:
            device_type (str): Device type name (e.g., 'inverter', 'meter')
        """
        today = self._get_today_date()
        calls_by_date = self.state['historical_api']['calls_by_date']
        
        # Initialize today's entry if it doesn't exist
        if today not in calls_by_date:
            calls_by_date[today] = {}
        
        # Initialize device type count if it doesn't exist
        if device_type not in calls_by_date[today]:
            calls_by_date[today][device_type] = 0
        
        # Increment the call count
        calls_by_date[today][device_type] += 1
        
        # Save the updated state
        self._save_state()
    
    def get_calls_today(self, device_type):
        """
        Get the number of API calls made today for a device type.
        
        Args:
            device_type (str): Device type name (e.g., 'inverter', 'meter')
            
        Returns:
            int: Number of API calls made today for the device type
        """
        today = self._get_today_date()
        calls_by_date = self.state['historical_api']['calls_by_date']
        
        if today not in calls_by_date or device_type not in calls_by_date[today]:
            return 0
        
        return calls_by_date[today][device_type]
    
    def get_remaining_calls(self, device_type):
        """
        Get the number of remaining API calls for today for a device type.
        
        Args:
            device_type (str): Device type name (e.g., 'inverter', 'meter')
            
        Returns:
            int: Number of remaining API calls for today
        """
        daily_limit = self.calculate_daily_limit(device_type)
        calls_today = self.get_calls_today(device_type)
        
        return max(0, daily_limit - calls_today)
    
    def can_make_api_call(self, device_type):
        """
        Check if an API call can be made for a device type without exceeding the daily limit.
        
        Args:
            device_type (str): Device type name (e.g., 'inverter', 'meter')
            
        Returns:
            bool: True if an API call can be made, False otherwise
        """
        return self.get_remaining_calls(device_type) > 0
    
    def wait_for_api_call(self, device_type, max_wait_time=None):
        """
        Wait until an API call can be made for a device type.
        
        Args:
            device_type (str): Device type name (e.g., 'inverter', 'meter')
            max_wait_time (int, optional): Maximum time to wait in seconds.
                If None, uses test mode settings or default of 3600 (1 hour).
        """
        # Use shorter wait times in test mode
        if max_wait_time is None:
            if self.test_mode:
                max_wait_time = 10  # 10 seconds in test mode
            else:
                max_wait_time = 3600  # 1 hour in normal mode
        """
        Wait until an API call can be made for a device type.
        
        Args:
            device_type (str): Device type name (e.g., 'inverter', 'meter')
            max_wait_time (int, optional): Maximum time to wait in seconds. Defaults to 3600 (1 hour).
            
        Returns:
            bool: True if an API call can now be made, False if max_wait_time was reached
        """
        start_time = time.time()
        while not self.can_make_api_call(device_type):
            # Check if we've waited too long
            if time.time() - start_time > max_wait_time:
                logging.warning(f"Max wait time reached for device type {device_type}")
                return False
            
            # Wait for a bit and check again
            remaining_time_until_tomorrow = self._get_seconds_until_tomorrow()
            
            # In test mode, use much shorter wait times
            if self.test_mode:
                wait_time = min(2, remaining_time_until_tomorrow)  # 2 seconds in test mode
            else:
                wait_time = min(300, remaining_time_until_tomorrow)  # Up to 5 minutes in normal mode
            
            logging.info(f"Rate limit reached for {device_type}. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            
            # We might have crossed to a new day, so reload the state
            self.state = self._load_state()
        
        return True
    
    def _get_seconds_until_tomorrow(self):
        """Get the number of seconds until tomorrow."""
        now = datetime.now(timezone.utc)
        tomorrow = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
        return (tomorrow - now).total_seconds()
    
    def get_progress_tracker_key(self, start_date, end_date, filters=None):
        """
        Generate a unique key for tracking progress of a data fetch operation.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            filters (dict, optional): Additional filters like plant_codes, device_types
            
        Returns:
            str: A unique key for the operation
        """
        key_parts = [start_date, end_date]
        
        if filters:
            for key, value in sorted(filters.items()):
                if value:
                    key_parts.append(f"{key}={value}")
        
        return "_".join(key_parts)
    
    def get_fetch_progress(self, progress_key):
        """
        Get progress for a historical data fetch operation.
        
        Args:
            progress_key (str): Progress tracker key
            
        Returns:
            dict: Progress information or None if no progress is stored
        """
        if 'fetch_progress' not in self.state:
            self.state['fetch_progress'] = {}
            
        return self.state['fetch_progress'].get(progress_key)
    
    def update_fetch_progress(self, progress_key, current_date, processed_device_types=None):
        """
        Update progress for a historical data fetch operation.
        
        Args:
            progress_key (str): Progress tracker key
            current_date (str): Current date being processed in YYYY-MM-DD format
            processed_device_types (list, optional): List of device types processed for current_date
        """
        if 'fetch_progress' not in self.state:
            self.state['fetch_progress'] = {}
            
        if progress_key not in self.state['fetch_progress']:
            self.state['fetch_progress'][progress_key] = {
                'current_date': current_date,
                'processed_device_types': {},
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
        
        progress = self.state['fetch_progress'][progress_key]
        progress['current_date'] = current_date
        progress['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        if processed_device_types:
            if not isinstance(progress['processed_device_types'], dict):
                progress['processed_device_types'] = {}
                
            if current_date not in progress['processed_device_types']:
                progress['processed_device_types'][current_date] = []
                
            for dev_type in processed_device_types:
                if dev_type not in progress['processed_device_types'][current_date]:
                    progress['processed_device_types'][current_date].append(dev_type)
        
        self._save_state()
    
    def clear_fetch_progress(self, progress_key):
        """
        Clear progress for a historical data fetch operation.
        
        Args:
            progress_key (str): Progress tracker key
        """
        if 'fetch_progress' in self.state and progress_key in self.state['fetch_progress']:
            del self.state['fetch_progress'][progress_key]
            self._save_state()
