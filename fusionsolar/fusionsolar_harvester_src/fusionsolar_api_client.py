import logging
import time
import requests
from datetime import datetime, timedelta

from .fusionsolar_config import FUSIONSOLAR_BASE_URL, FUSIONSOLAR_USERNAME, FUSIONSOLAR_PASSWORD, REQUEST_DELAY_SECONDS

# Global token storage
_xsrf_token = None
_token_expiry = None

def login_fusionsolar():
    """Authenticates with the FusionSolar API and stores the XSRF token.
    Returns: (True, None) on success, (False, error_message_str) on failure.
    """
    global _xsrf_token, _token_expiry
    
    login_url = f"{FUSIONSOLAR_BASE_URL}/thirdData/login"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, */*"
    }
    
    # Using the exact format from FusionSolar API documentation
    payload = {
        "userName": FUSIONSOLAR_USERNAME,
        "systemCode": FUSIONSOLAR_PASSWORD
    }
    
    try:
        logging.info(f"Attempting to login to FusionSolar API at {login_url}")
        logging.debug(f"Request headers: {headers}")
        logging.debug(f"Request payload: {payload}")
        
        response = requests.post(login_url, headers=headers, json=payload)
        logging.info(f"Login response status code: {response.status_code}")
        
        # Log response headers to check for token
        logging.debug(f"Response headers: {dict(response.headers)}")
        
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        logging.debug(f"Response data: {data}")
        if data.get("failCode") == 0:
            # Extract the token from response headers
            _xsrf_token = response.headers.get('xsrf-token')
            if _xsrf_token:
                # Token is valid for 30 minutes according to API docs
                _token_expiry = datetime.now() + timedelta(minutes=29)  # Set slightly less for safety
                logging.info("Successfully logged into FusionSolar.")
                return (True, None)
            else:
                msg = "Login successful but XSRF token not found in response headers."
                logging.error(msg)
                return (False, msg)
        else:
            msg = data.get('message', 'Unknown error')
            logging.error(f"FusionSolar login failed: {msg}")
            return (False, msg)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during FusionSolar login: {e}")
        return (False, str(e))

def _check_token_validity():
    """Checks if the current token is valid and not expired."""
    if not _xsrf_token or not _token_expiry:
        return False
    return datetime.now() < _token_expiry

def _make_api_request(endpoint, payload, retry=True):
    """Helper function to make requests to the FusionSolar API."""
    global _xsrf_token
    
    # Check if we need to login first
    if not _check_token_validity():
        logging.info("Token is missing or expired. Attempting to login...")
        ok, _ = login_fusionsolar()
        if not ok:
            logging.error("Failed to login to FusionSolar API.")
            return None
    
    url = f"{FUSIONSOLAR_BASE_URL}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, */*",
        "xsrf-token": _xsrf_token
    }
    
    logging.info(f"Making API request to: {url}")
    logging.info(f"Request payload: {payload}")
    
    try:
        logging.debug(f"Making API request to {endpoint}")
        logging.debug(f"Request payload: {payload}")
        
        response = requests.post(url, headers=headers, json=payload)
        logging.info(f"Response status code: {response.status_code}")
        
        # Don't raise_for_status immediately to allow for better error handling
        try:
            data = response.json()
            # Log a truncated version of the response to avoid huge log files
            log_data = str(data)[:500] + '...' if len(str(data)) > 500 else data
            logging.info(f"API response from {url} (truncated): {log_data}")
            
            # Handle non-200 status codes with more detail
            if response.status_code != 200:
                logging.error(f"HTTP error {response.status_code}: {data.get('message', 'No message')}")
                response.raise_for_status()
                
            # Check for API-specific error codes
            fail_code = data.get("failCode")
        except ValueError as e:
            # Handle JSON decode errors
            logging.error(f"JSON decode error: {e}")
            logging.error(f"Response text: {response.text[:200]}...")
            response.raise_for_status()  # Will raise if status code was an error
            return None  # Otherwise return None for non-JSON responses
        
        if fail_code == 0:
            return data
        elif fail_code == 305 and retry:  # Not logged in
            logging.warning("Session expired. Attempting to re-login...")
            ok, _ = login_fusionsolar()
            if ok:
                logging.info("Re-login successful. Retrying original request...")
                return _make_api_request(endpoint, payload, retry=False)  # Retry once
            else:
                logging.error("Re-login failed. Cannot proceed with API request.")
                return None
        elif fail_code == 407:  # API access frequency too high
            if retry:
                # Use exponential backoff: wait longer for rate limit (sama seperti no_limit version approach)
                wait_time = 30  # Wait 30 seconds for rate limit (lebih lama dari sebelumnya)
                logging.warning(f"API access frequency too high. Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                logging.info("Retrying API request after rate limit wait...")
                return _make_api_request(endpoint, payload, retry=False)  # Retry once
            else:
                logging.error("API rate limit hit even after waiting. Cannot proceed.")
                return None
        else:
            logging.error(f"API request to {endpoint} failed: {data.get('message', 'Unknown error')} (Code: {fail_code})")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during API request to {endpoint}: {e}")
        return None
