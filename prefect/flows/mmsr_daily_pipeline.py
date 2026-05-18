"""
MMSR Daily Data Pipeline - Prefect Flow

This flow orchestrates the daily data ingestion and transformation process:
1. Ingest data from FusionSolar and iSolarCloud (parallel)
2. Transform data using dbt following the flow:
   - raw -> staging (unpivoted data)
   - staging -> mart (device measurements 5min)
   - mart (measurements) -> facts (device calculations 5min)
   - mart (measurements) + facts (calculations) -> mart (daily aggregations)
   - mart (daily) + facts (site calculations) -> mart (site performance daily)
3. Run data quality tests
4. Log completion status

Schedule: Daily at 01:00 WIB (Asia/Jakarta timezone)
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict

# Add project root to path FIRST
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "fusionsolar"))
sys.path.insert(0, str(project_root / "isolarcloud"))

# Remove local prefect folder from path to avoid import conflict with prefect package
_prefect_folder = Path(__file__).parent.parent
_prefect_folder_str = str(_prefect_folder)
if _prefect_folder_str in sys.path:
    sys.path.remove(_prefect_folder_str)

# Now import prefect package (installed package, not local folder)
import pendulum
import prefect

# Try different import methods for ConcurrentTaskRunner (Prefect 2.x and 3.x compatibility)
ConcurrentTaskRunner = None
try:
    # Prefect 3.x
    from prefect.task_runners import ConcurrentTaskRunner
except ImportError:
    try:
        # Alternative Prefect 3.x import
        from prefect import task_runners
        ConcurrentTaskRunner = task_runners.ConcurrentTaskRunner
    except (ImportError, AttributeError):
        try:
            # Prefect 2.x
            from prefect.engine import ConcurrentTaskRunner
        except ImportError:
            # If all imports fail, we'll use None (default task runner)
            # Tasks will still run, just not concurrently
            ConcurrentTaskRunner = None

# Get decorators from prefect package
flow = prefect.flow
task = prefect.task
get_run_logger = prefect.get_run_logger

# Import harvester tasks
from fusionsolar.tasks import run_fusionsolar_ingest
from isolarcloud.tasks import run_isolarcloud_ingest


@task(name="prepare_date_window", log_prints=True)
def prepare_date_window() -> Dict[str, str]:
    """
    Prepare date window for yesterday's data.
    Returns date range in YYYY-MM-DD format.
    """
    logger = get_run_logger()
    
    # Get current time in Asia/Jakarta timezone
    jakarta_tz = pendulum.timezone('Asia/Jakarta')
    now_jakarta = pendulum.now(jakarta_tz)
    
    # Yesterday is the day before today
    yesterday = now_jakarta.subtract(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    
    result = {
        'start_date': yesterday_str,
        'end_date': yesterday_str
    }
    
    logger.info(f"Date window prepared: {result}")
    return result


@task(name="ingest_fusionsolar", log_prints=True, retries=2, retry_delay_seconds=900)
def ingest_fusionsolar_task(date_window: Dict[str, str]) -> Dict:
    """
    Ingest data from FusionSolar API.
    """
    logger = get_run_logger()
    logger.info(f"Starting FusionSolar ingestion for {date_window}")
    
    try:
        result = run_fusionsolar_ingest(
            start_date=date_window['start_date'],
            end_date=date_window['end_date']
        )
        logger.info(f"FusionSolar ingestion completed: {result}")
        return result
    except Exception as e:
        logger.error(f"FusionSolar ingestion failed: {str(e)}")
        raise


@task(name="ingest_isolarcloud", log_prints=True, retries=2, retry_delay_seconds=900)
def ingest_isolarcloud_task(date_window: Dict[str, str]) -> Dict:
    """
    Ingest data from iSolarCloud API.
    """
    logger = get_run_logger()
    logger.info(f"Starting iSolarCloud ingestion for {date_window}")
    
    try:
        result = run_isolarcloud_ingest(
            start_date=date_window['start_date'],
            end_date=date_window['end_date']
        )
        logger.info(f"iSolarCloud ingestion completed: {result}")
        return result
    except Exception as e:
        logger.error(f"iSolarCloud ingestion failed: {str(e)}")
        raise


@task(name="dbt_run_staging", log_prints=True, retries=1)
def dbt_run_staging_task(date_window: Dict[str, str]) -> str:
    """
    Run dbt staging models with date variable.
    """
    logger = get_run_logger()
    dbt_project_dir = project_root / "dbt"
    load_date = date_window['start_date']
    
    logger.info(f"Running dbt staging models for date: {load_date}")
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_staging', '--vars', f'{{"load_date": "{load_date}"}}'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("dbt staging models completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt staging failed: {e.stderr}")
        raise


@task(name="dbt_run_marts_measurement_5min", log_prints=True, retries=1)
def dbt_run_marts_measurement_5min_task() -> str:
    """
    Run dbt mart models for device measurements 5min.
    """
    logger = get_run_logger()
    dbt_project_dir = project_root / "dbt"
    
    logger.info("Running dbt mart models for device measurements 5min")
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_marts_measurement_5min'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("dbt mart measurement 5min models completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt mart measurement 5min failed: {e.stderr}")
        raise


@task(name="dbt_run_facts_calculation_5min", log_prints=True, retries=1)
def dbt_run_facts_calculation_5min_task() -> str:
    """
    Run dbt facts models for device calculations 5min.
    """
    logger = get_run_logger()
    dbt_project_dir = project_root / "dbt"
    
    logger.info("Running dbt facts models for device calculations 5min")
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_facts_calculation_5min'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("dbt facts calculation 5min models completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt facts calculation 5min failed: {e.stderr}")
        raise


@task(name="dbt_run_marts_daily", log_prints=True, retries=1)
def dbt_run_marts_daily_task() -> str:
    """
    Run dbt mart models for daily aggregations.
    """
    logger = get_run_logger()
    dbt_project_dir = project_root / "dbt"
    
    logger.info("Running dbt mart models for daily aggregations")
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_marts_daily'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("dbt mart daily models completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt mart daily failed: {e.stderr}")
        raise


@task(name="dbt_test_critical", log_prints=True, retries=1)
def dbt_test_critical_task() -> str:
    """
    Run critical dbt tests.
    """
    logger = get_run_logger()
    dbt_project_dir = project_root / "dbt"
    
    logger.info("Running critical dbt tests")
    
    try:
        result = subprocess.run(
            ['dbt', 'test', '--select', 'tag:critical'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("Critical dbt tests passed")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Critical dbt tests failed: {e.stderr}")
        raise


@task(name="log_completion_status", log_prints=True)
def log_completion_status_task(
    date_window: Dict[str, str],
    fusionsolar_result: Dict = None,
    isolarcloud_result: Dict = None
) -> Dict:
    """
    Log pipeline completion status to file.
    """
    logger = get_run_logger()
    
    # Calculate total execution time
    execution_time = 0.0
    if fusionsolar_result:
        execution_time += fusionsolar_result.get('execution_time', 0)
    if isolarcloud_result:
        execution_time += isolarcloud_result.get('execution_time', 0)
    
    # Determine status
    status = "SUCCESS"
    if not fusionsolar_result or not isolarcloud_result:
        status = "FAILED"
    
    # Prepare metrics
    metrics = {
        "status": status,
        "execution_time": execution_time,
        "start_date": date_window['start_date'],
        "end_date": date_window['end_date'],
        "timestamp": datetime.now().isoformat(),
    }
    
    if fusionsolar_result:
        metrics["fusionsolar_rows_inserted"] = fusionsolar_result.get('rows_inserted', 0)
        metrics["fusionsolar_execution_time"] = fusionsolar_result.get('execution_time', 0)
    
    if isolarcloud_result:
        metrics["isolarcloud_rows_inserted"] = isolarcloud_result.get('rows_inserted', 0)
        metrics["isolarcloud_execution_time"] = isolarcloud_result.get('execution_time', 0)
    
    # Create logs directory if it doesn't exist
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Write status file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"mmsr_pipeline_status_{timestamp}.txt"
    
    with open(log_file, 'w') as f:
        for key, value in metrics.items():
            f.write(f"{key}: {value}\n")
    
    logger.info(f"Pipeline completed with status: {status}")
    logger.info(f"Status logged to: {log_file}")
    
    return {"status": status, "log_file": str(log_file), "metrics": metrics}


@flow(
    name="mmsr_daily_pipeline",
    description="Daily pipeline for MMSR data ingestion and transformation",
    task_runner=ConcurrentTaskRunner() if ConcurrentTaskRunner is not None else None,
    log_prints=True
)
def mmsr_daily_pipeline():
    """
    Main Prefect flow for MMSR daily pipeline.
    
    Flow:
    1. Prepare date window (yesterday's date)
    2. Ingest data from both platforms (parallel)
    3. Run dbt transformations in sequence:
       - Staging models
       - Mart measurement 5min models
       - Facts calculation 5min models
       - Mart daily models
    4. Run critical tests
    5. Log completion status
    """
    logger = get_run_logger()
    logger.info("Starting MMSR daily pipeline")
    
    # Step 1: Prepare date window
    date_window = prepare_date_window()
    
    # Step 2: Ingest data from both platforms (parallel)
    # Call tasks separately so Prefect can run them in parallel with ConcurrentTaskRunner
    fusionsolar_result = ingest_fusionsolar_task(date_window)
    isolarcloud_result = ingest_isolarcloud_task(date_window)
    
    # Step 3: Run dbt transformations in sequence
    dbt_run_staging_task(date_window)
    dbt_run_marts_measurement_5min_task()
    dbt_run_facts_calculation_5min_task()
    dbt_run_marts_daily_task()
    
    # Step 4: Run critical tests
    dbt_test_critical_task()
    
    # Step 5: Log completion status
    log_completion_status_task(
        date_window=date_window,
        fusionsolar_result=fusionsolar_result,
        isolarcloud_result=isolarcloud_result
    )
    
    logger.info("MMSR daily pipeline completed successfully")


if __name__ == "__main__":
    # Run the flow
    mmsr_daily_pipeline()

