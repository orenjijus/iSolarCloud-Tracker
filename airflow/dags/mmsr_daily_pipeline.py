"""
MMSR Daily Data Pipeline DAG

This DAG orchestrates the daily data ingestion and transformation process:
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

from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from airflow.plugins.mmsr_tasks.fusionsolar_ingest import ingest_fusionsolar_task
from airflow.plugins.mmsr_tasks.isolarcloud_ingest import ingest_isolarcloud_task
from airflow.plugins.mmsr_tasks.monitoring import log_completion_task

# DAG Configuration
default_args = {
    'owner': 'mmsr',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=15),
}

# Get dbt project directory
dbt_project_dir = project_root / "dbt"

with DAG(
    dag_id='mmsr_daily_pipeline',
    default_args=default_args,
    description='Daily pipeline for MMSR data ingestion and transformation',
    schedule='0 1 * * *',  # 01:00 WIB daily
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Jakarta'),
    catchup=False,
    max_active_runs=1,
    tags=['mmsr', 'daily', 'ingestion', 'dbt'],
) as dag:
    
    def prepare_date_window(**context):
        """
        Prepare date window for yesterday's data.
        Airflow's data_interval_start represents the start of the interval.
        For daily runs, we want to process yesterday's data.
        """
        data_interval_start = context['data_interval_start']
        
        # Convert to Asia/Jakarta timezone
        jakarta_tz = pendulum.timezone('Asia/Jakarta')
        interval_start_jakarta = data_interval_start.in_timezone(jakarta_tz)
        
        # Yesterday is the day before the interval start
        yesterday = interval_start_jakarta - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')
        
        result = {
            'start_date': yesterday_str,
            'end_date': yesterday_str
        }
        
        context['ti'].xcom_push(key='date_window', value=result)
        return result
    
    # Task 1: Prepare date window
    prepare_window = PythonOperator(
        task_id='prepare_date_window',
        python_callable=prepare_date_window,
    )
    
    # Task 2 & 3: Ingest data from both platforms (parallel)
    ingest_fusionsolar = PythonOperator(
        task_id='ingest_fusionsolar',
        python_callable=ingest_fusionsolar_task,
    )
    
    ingest_isolarcloud = PythonOperator(
        task_id='ingest_isolarcloud',
        python_callable=ingest_isolarcloud_task,
    )
    
    # Task 4: dbt run staging models
    def run_dbt_staging(**context):
        """Run dbt staging models with date variable."""
        import subprocess
        ti = context['ti']
        date_window = ti.xcom_pull(task_ids='prepare_date_window')
        load_date = date_window['start_date']
        
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_staging', '--vars', f'{{"load_date": "{load_date}"}}'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")
        return result.stdout
    
    dbt_run_staging = PythonOperator(
        task_id='dbt_run_staging',
        python_callable=run_dbt_staging,
    )
    
    # Task 5: dbt run mart models - device measurements 5min
    def run_dbt_marts_measurement_5min(**context):
        """Run dbt mart models for device measurements 5min."""
        import subprocess
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_marts_measurement_5min'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")
        return result.stdout
    
    dbt_run_marts_measurement_5min = PythonOperator(
        task_id='dbt_run_marts_measurement_5min',
        python_callable=run_dbt_marts_measurement_5min,
    )
    
    # Task 6: dbt run facts models - device calculations 5min
    def run_dbt_facts_calculation_5min(**context):
        """Run dbt facts models for device calculations 5min."""
        import subprocess
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_facts_calculation_5min'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")
        return result.stdout
    
    dbt_run_facts_calculation_5min = PythonOperator(
        task_id='dbt_run_facts_calculation_5min',
        python_callable=run_dbt_facts_calculation_5min,
    )
    
    # Task 7: dbt run mart models - daily aggregations
    def run_dbt_marts_daily(**context):
        """Run dbt mart models for daily aggregations."""
        import subprocess
        result = subprocess.run(
            ['dbt', 'run', '--select', 'tag:daily_marts_daily'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")
        return result.stdout
    
    dbt_run_marts_daily = PythonOperator(
        task_id='dbt_run_marts_daily',
        python_callable=run_dbt_marts_daily,
    )
    
    # Task 8: dbt test critical models
    def run_dbt_tests(**context):
        """Run dbt critical tests."""
        import subprocess
        result = subprocess.run(
            ['dbt', 'test', '--select', 'tag:critical'],
            cwd=str(dbt_project_dir),
            env={**os.environ, 'DBT_PROFILES_DIR': str(dbt_project_dir)},
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"dbt test failed: {result.stderr}")
        return result.stdout
    
    dbt_test_critical = PythonOperator(
        task_id='dbt_test_critical',
        python_callable=run_dbt_tests,
    )
    
    # Task 9: Log completion status
    log_completion = PythonOperator(
        task_id='log_completion_status',
        python_callable=log_completion_task,
    )
    
    # Define task dependencies
    # Flow: raw -> staging -> mart (measurement 5min) -> facts (calculation 5min) -> mart (daily) -> tests -> log
    # Ensure both ingestion tasks run in parallel after prepare_window
    prepare_window >> ingest_fusionsolar
    prepare_window >> ingest_isolarcloud
    
    # Both ingestion tasks must complete before dbt staging
    [ingest_fusionsolar, ingest_isolarcloud] >> dbt_run_staging
    
    # Continue with the rest of the pipeline
    dbt_run_staging >> dbt_run_marts_measurement_5min
    dbt_run_marts_measurement_5min >> dbt_run_facts_calculation_5min
    dbt_run_facts_calculation_5min >> dbt_run_marts_daily
    dbt_run_marts_daily >> dbt_test_critical
    dbt_test_critical >> log_completion

