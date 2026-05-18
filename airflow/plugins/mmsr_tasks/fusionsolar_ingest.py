"""
Airflow task wrapper for FusionSolar ingestion.
"""

import sys
import os
from pathlib import Path

# Add parent directories to path to import harvester modules
project_root = Path(__file__).parent.parent.parent.parent
fusionsolar_path = project_root / "fusionsolar"
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(fusionsolar_path))

from fusionsolar.tasks import run_fusionsolar_ingest


def ingest_fusionsolar_task(**context):
    """
    Airflow task function to ingest FusionSolar data.
    
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
    result = run_fusionsolar_ingest(start_date=start_date, end_date=end_date)
    
    # Push result to XCom for downstream tasks
    ti.xcom_push(key='fusionsolar_result', value=result)
    
    return result

