import requests
import logging

from .config import ISOLARCLOUD_BASE_URL, ISOLARCLOUD_SECRET_KEY, SYS_CODE, ISOLARCLOUD_APP_KEY, ISOLARCLOUD_USERNAME, ISOLARCLOUD_PASSWORD, REQUEST_DELAY_SECONDS

# Global token for iSolarCloud API
ISOLARCLOUD_TOKEN = None

def login_isolarcloud():
    """Authenticates with the iSolarCloud API and stores the token."""
    global ISOLARCLOUD_TOKEN
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
            ISOLARCLOUD_TOKEN = data.get("result_data", {}).get("token")
            if ISOLARCLOUD_TOKEN:
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
    if not ISOLARCLOUD_TOKEN:
        logging.error("Not logged in. Please login to iSolarCloud first.")
        return None

    url = f"{ISOLARCLOUD_BASE_URL}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "x-access-key": ISOLARCLOUD_SECRET_KEY,
        "sys_code": SYS_CODE,
    }
    # Add token and appkey to payload for all requests except login
    payload["token"] = ISOLARCLOUD_TOKEN
    payload["appkey"] = ISOLARCLOUD_APP_KEY # Appkey also needed for other calls

    try:
        logging.debug(f"Making API request to {url} with payload: {payload}")
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        logging.debug(f"API response from {url}: {data}")

        if data.get("result_code") == "1":
            return data # Return full response
        elif data.get("result_code") == "30001": # Token expired
            logging.warning("iSolarCloud token expired or invalid. Attempting to re-login...")
            if login_isolarcloud(): # Try to login again
                logging.info("Re-login successful. Retrying original request...")
                payload["token"] = ISOLARCLOUD_TOKEN # Update token in payload
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
