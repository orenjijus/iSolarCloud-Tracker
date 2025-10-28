import logging
import time
import hashlib
import requests
from datetime import datetime

from .isolar_config import ISOLARCLOUD_BASE_URL, ISOLARCLOUD_SECRET_KEY, SYS_CODE, ISOLARCLOUD_APP_KEY, ISOLARCLOUD_USERNAME, ISOLARCLOUD_PASSWORD, REQUEST_DELAY_SECONDS

# Global token storage
_token = None
_token_expiry = None

def login_isolarcloud():
    """Authenticates with the iSolarCloud API and stores the token."""
    global _token
    login_url = f"{ISOLARCLOUD_BASE_URL}/openapi/login"
    headers = {
        "Content-Type": "application/json",
        "x-access-key": ISOLARCLOUD_SECRET_KEY,
        "sys_code": SYS_CODE
    }
    payload = {
        "appkey": ISOLARCLOUD_APP_KEY,
        "user_account": ISOLARCLOUD_USERNAME,
        "user_password": ISOLARCLOUD_PASSWORD
    }
    try:
        response = requests.post(login_url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        if data.get("result_code") == "1":
            _token = data.get("result_data", {}).get("token")
            if _token:
                logging.info("Successfully logged into iSolarCloud.")
                return True
            else:
                logging.error("Login successful but token not found in response.")
                return False
        else:
            logging.error(f"iSolarCloud login failed: {data.get('result_msg')}")
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during iSolarCloud login: {e}")
        return False

def _make_api_request(endpoint, payload):
    """Helper function to make requests to the iSolarCloud API."""
    if not _token:
        logging.error("Not logged in. Please login to iSolarCloud first.")
        return None

    url = f"{ISOLARCLOUD_BASE_URL}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "x-access-key": ISOLARCLOUD_SECRET_KEY,
        "sys_code": SYS_CODE,
    }
    # Add token and appkey to payload for all requests except login
    payload["token"] = _token
    payload["appkey"] = ISOLARCLOUD_APP_KEY # Appkey also needed for other calls

    try:
        print(f"Making API request to {endpoint}")
        print(f"Request payload: {payload}")
        response = requests.post(url, headers=headers, json=payload)
        print(f"Response status code: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        # Print a safer version of the response data to avoid encoding issues
        print(f"API response code: {data.get('result_code')}")
        print(f"API response message: {data.get('result_msg', 'No message')}")
        if 'pageList' in data:
            print(f"Number of stations found: {len(data.get('pageList', []))}")
        elif 'result_data' in data:
            print("API returned result data successfully")
        logging.debug(f"API response from {url}: {data}")

        if data.get("result_code") == "1":
            return data # Return full response
        elif data.get("result_code") == "30001": # Token expired
            logging.warning("iSolarCloud token expired or invalid. Attempting to re-login...")
            if login_isolarcloud(): # Try to login again
                logging.info("Re-login successful. Retrying original request...")
                payload["token"] = _token # Update token in payload
                response = requests.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                if data.get("result_code") == "1":
                    return data # Return full response
                else:
                    logging.error(f"API request failed after re-login: {data.get('result_msg')} (Code: {data.get('result_code')})")
                    return None
            else:
                logging.error("Re-login failed. Cannot proceed with API request.")
                return None
        else:
            logging.error(f"API request to {endpoint} failed: {data.get('result_msg')} (Code: {data.get('result_code')})")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during API request to {endpoint}: {e}")
        return None
