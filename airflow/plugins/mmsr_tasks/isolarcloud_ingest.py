"""
Airflow task wrapper for iSolarCloud ingestion.
"""

import sys
import os
from pathlib import Path

# Add parent directories to path to import harvester modules
project_root = Path(__file__).parent.parent.parent.parent
isolarcloud_path = project_root / "isolarcloud"
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(isolarcloud_path))

from isolarcloud.tasks import run_isolarcloud_ingest


def ingest_isolarcloud_task(**context):
    """
    Airflow task function to ingest iSolarCloud data.
    
    Expects date window from XCom key 'prepare_date_window'.
    """
    # Get date window from previous task
    ti = context['ti']
    date_window = ti.xcom_pull(task_ids='prepare_date_window')
    
    if not date_window:
        raise ValueError("Date window not found in XCom. Ensure prepare_date_window task completed successfully.")
    
    start_date = date_window['start_date']
    end_date = date_window['end_date']
    
    # Run ingestion
    result = run_isolarcloud_ingest(start_date=start_date, end_date=end_date)
    
    # Push result to XCom for downstream tasks
    ti.xcom_push(key='isolarcloud_result', value=result)
    
    return result

