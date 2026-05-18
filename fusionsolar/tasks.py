"""
FusionSolar ingestion tasks for Airflow orchestration.

This module provides reusable functions for ingesting data from FusionSolar API
that can be called from Airflow DAGs.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from fusionsolar_harvester_src.fusionsolar_config import DATABASE_URL
from fusionsolar_harvester_src.fusionsolar_api_client import login_fusionsolar
from fusionsolar_harvester_src.fusionsolar_db_operations import init_database
from fusionsolar_harvester_src.fusionsolar_data_processing import fetch_historical_data

logger = logging.getLogger(__name__)


def run_fusionsolar_ingest(start_date: str, end_date: str, plant_codes: Optional[str] = None, 
                           device_types: Optional[str] = None) -> Dict:
    """
    Run FusionSolar data ingestion for a given date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        plant_codes: Optional comma-separated list of plant codes to filter
        device_types: Optional comma-separated list of device types to filter
        
    Returns:
        Dictionary with status, rows_inserted, and execution_time
        
    Raises:
        Exception: If database initialization, login, or data fetch fails
    """
    start_time = time.time()
    logger.info(f"Starting FusionSolar ingestion for date range: {start_date} to {end_date}")
    
    try:
        # Initialize database connection
        logger.info("Initializing database connection")
        if not init_database():
            raise Exception("Failed to initialize database")
        logger.info("Database initialized successfully")
        
        # Login to FusionSolar API
        logger.info("Logging in to FusionSolar API")
        if not login_fusionsolar():
            raise Exception("Failed to login to FusionSolar API")
        logger.info("FusionSolar API login successful")
        
        # Fetch historical data
        logger.info(f"Fetching historical data from {start_date} to {end_date}")
        rows_inserted = fetch_historical_data(
            start_date_str=start_date,
            end_date_str=end_date,
            plant_codes_str=plant_codes,
            device_types_str=device_types
        )
        
        execution_time = time.time() - start_time
        logger.info(f"FusionSolar ingestion completed successfully. Rows inserted: {rows_inserted}, Execution time: {execution_time:.2f} seconds")
        
        return {
            "status": "success",
            "rows_inserted": rows_inserted,
            "execution_time": execution_time,
            "start_date": start_date,
            "end_date": end_date
        }
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"FusionSolar ingestion failed after {execution_time:.2f} seconds: {str(e)}")
        raise

