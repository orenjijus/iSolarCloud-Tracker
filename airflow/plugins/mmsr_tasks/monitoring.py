"""
Monitoring and logging utilities for MMSR pipeline.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def log_pipeline_status(
    status: str,
    execution_time: float,
    metrics: Optional[Dict] = None,
    log_dir: str = "logs"
) -> str:
    """
    Log pipeline completion status to a file.
    
    Args:
        status: Pipeline status (SUCCESS/FAILED)
        execution_time: Total execution time in seconds
        metrics: Optional dictionary with additional metrics
        log_dir: Directory to write log file
        
    Returns:
        Path to the status log file
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"mmsr_pipeline_status_{timestamp}.txt"
    filepath = log_path / filename
    
    # Write status file
    with open(filepath, 'w') as f:
        f.write(f"timestamp: {datetime.now().isoformat()}\n")
        f.write(f"status: {status}\n")
        f.write(f"execution_time: {execution_time:.2f}\n")
        
        if metrics:
            for key, value in metrics.items():
                f.write(f"{key}: {value}\n")
    
    logger.info(f"Pipeline status logged to {filepath}")
    return str(filepath)


def log_completion_task(**context):
    """
    Airflow task function to log pipeline completion status.
    """
    from airflow.models import TaskInstance
    
    ti = context['ti']
    dag_run = context['dag_run']
    
    # Get results from upstream tasks
    fusionsolar_result = ti.xcom_pull(task_ids='ingest_fusionsolar', key='fusionsolar_result')
    isolarcloud_result = ti.xcom_pull(task_ids='ingest_isolarcloud', key='isolarcloud_result')
    
    # Calculate total execution time
    execution_time = 0.0
    if fusionsolar_result:
        execution_time += fusionsolar_result.get('execution_time', 0)
    if isolarcloud_result:
        execution_time += isolarcloud_result.get('execution_time', 0)
    
    # Determine status - check if any upstream task failed
    status = "SUCCESS"
    try:
        # Try to get task instance states
        fs_ti = dag_run.get_task_instance('ingest_fusionsolar')
        iso_ti = dag_run.get_task_instance('ingest_isolarcloud')
        
        if fs_ti and fs_ti.state != 'success':
            status = "FAILED"
        elif iso_ti and iso_ti.state != 'success':
            status = "FAILED"
    except Exception as e:
        logger.warning(f"Could not check task states: {e}")
        # If we can't check, assume success if we have results
        if not fusionsolar_result or not isolarcloud_result:
            status = "FAILED"
    
    # Prepare metrics
    metrics = {
        "dag_run_id": dag_run.run_id,
        "data_interval_start": str(dag_run.data_interval_start),
        "data_interval_end": str(dag_run.data_interval_end),
    }
    
    if fusionsolar_result:
        metrics["fusionsolar_rows_inserted"] = fusionsolar_result.get('rows_inserted', 0)
        metrics["fusionsolar_execution_time"] = fusionsolar_result.get('execution_time', 0)
    
    if isolarcloud_result:
        metrics["isolarcloud_rows_inserted"] = isolarcloud_result.get('rows_inserted', 0)
        metrics["isolarcloud_execution_time"] = isolarcloud_result.get('execution_time', 0)
    
    # Log status
    log_file = log_pipeline_status(
        status=status,
        execution_time=execution_time,
        metrics=metrics
    )
    
    logger.info(f"Pipeline completed with status: {status}")
    logger.info(f"Status logged to: {log_file}")
    
    return {
        "status": status,
        "log_file": log_file,
        "metrics": metrics
    }

