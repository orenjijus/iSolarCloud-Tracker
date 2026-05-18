"""
iSolarCloud ingestion tasks for Airflow orchestration.

This module provides reusable functions for ingesting data from iSolarCloud API
that can be called from Airflow DAGs.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

from isolarcloud_harvester_src.isolar_config import DATABASE_URL
from isolarcloud_harvester_src.isolar_api_client import login_isolarcloud
from isolarcloud_harvester_src.isolar_db_operations import init_database
from isolarcloud_harvester_src.isolar_data_processing import fetch_historical_data

logger = logging.getLogger(__name__)


def run_isolarcloud_ingest(start_date: str, end_date: str, ps_ids: Optional[str] = None,
                          device_types: Optional[str] = None) -> Dict:
    """
    Run iSolarCloud data ingestion for a given date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        ps_ids: Optional comma-separated list of power station IDs to filter
        device_types: Optional comma-separated list of device types to filter
                     (default: "meter,inverter,meteo_station")
        
    Returns:
        Dictionary with status, rows_inserted, and execution_time
        
    Raises:
        Exception: If database initialization, login, or data fetch fails
    """
    start_time = time.time()
    logger.info(f"Starting iSolarCloud ingestion for date range: {start_date} to {end_date}")
    
    try:
        # Initialize database connection
        logger.info("Initializing database connection")
        if not init_database():
            raise Exception("Failed to initialize database")
        logger.info("Database initialized successfully")
        
        # Login to iSolarCloud API
        logger.info("Logging in to iSolarCloud API")
        if not login_isolarcloud():
            raise Exception("Failed to login to iSolarCloud API")
        logger.info("iSolarCloud API login successful")
        
        # Default device types if not specified
        if device_types is None:
            device_types = "meter,inverter,meteo_station"
        
        # Fetch historical data
        logger.info(f"Fetching historical data from {start_date} to {end_date}")
        rows_inserted = fetch_historical_data(
            start_date_str=start_date,
            end_date_str=end_date,
            ps_ids_str=ps_ids,
            device_types_str=device_types
        )
        
        execution_time = time.time() - start_time
        logger.info(f"iSolarCloud ingestion completed successfully. Rows inserted: {rows_inserted}, Execution time: {execution_time:.2f} seconds")
        
        return {
            "status": "success",
            "rows_inserted": rows_inserted,
            "execution_time": execution_time,
            "start_date": start_date,
            "end_date": end_date
        }
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"iSolarCloud ingestion failed after {execution_time:.2f} seconds: {str(e)}")
        raise

