"""
Test script untuk iSolarCloud ingestion di Prefect
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "isolarcloud"))

# Remove local prefect folder from path to avoid import conflict
_prefect_folder = Path(__file__).parent.parent
_prefect_folder_str = str(_prefect_folder)
if _prefect_folder_str in sys.path:
    sys.path.remove(_prefect_folder_str)

import prefect
from prefect import flow, task, get_run_logger

# Import iSolarCloud task
from isolarcloud.tasks import run_isolarcloud_ingest


@task(name="prepare_test_date_window", log_prints=True)
def prepare_test_date_window() -> dict:
    """Prepare date window for testing (yesterday)"""
    logger = get_run_logger()
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_window = {
        'start_date': yesterday,
        'end_date': yesterday
    }
    logger.info(f"Test date window: {date_window}")
    return date_window


@task(name="test_isolarcloud_ingest", log_prints=True)
def test_isolarcloud_ingest_task(date_window: dict) -> dict:
    """Test iSolarCloud ingestion"""
    logger = get_run_logger()
    logger.info(f"Testing iSolarCloud ingestion for {date_window}")
    
    try:
        result = run_isolarcloud_ingest(
            start_date=date_window['start_date'],
            end_date=date_window['end_date']
        )
        logger.info(f"iSolarCloud ingestion test completed: {result}")
        return result
    except Exception as e:
        logger.error(f"iSolarCloud ingestion test failed: {str(e)}")
        raise


@flow(name="test_isolarcloud_flow", log_prints=True)
def test_isolarcloud_flow():
    """Test flow untuk iSolarCloud ingestion"""
    logger = get_run_logger()
    logger.info("Starting iSolarCloud test flow")
    
    # Prepare date window
    date_window = prepare_test_date_window()
    
    # Test iSolarCloud ingestion
    result = test_isolarcloud_ingest_task(date_window)
    
    logger.info(f"Test flow completed. Result: {result}")
    return result


if __name__ == "__main__":
    # Run the test flow
    test_isolarcloud_flow()

