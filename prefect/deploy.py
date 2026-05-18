"""
Prefect deployment script for MMSR daily pipeline.

This script creates a Prefect deployment that runs daily at 01:00 WIB.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Remove local prefect folder from path to avoid import conflict with prefect package
_prefect_folder = Path(__file__).parent
_prefect_folder_str = str(_prefect_folder)
if _prefect_folder_str in sys.path:
    sys.path.remove(_prefect_folder_str)

from prefect.schedules import Cron

# Import from local flows folder (not from prefect package)
flows_path = Path(__file__).parent / "flows"
sys.path.insert(0, str(flows_path))
from mmsr_daily_pipeline import mmsr_daily_pipeline

if __name__ == "__main__":
    # Create deployment with cron schedule
    # Schedule: Daily at 01:00 WIB (Asia/Jakarta timezone)
    mmsr_daily_pipeline.serve(
        name="mmsr-daily-pipeline-production",
        description="MMSR daily data pipeline - production deployment",
        tags=["mmsr", "daily", "production"],
        parameters={},
        schedules=[
            Cron(
            cron="0 1 * * *",  # 01:00 daily
            timezone="Asia/Jakarta"
            )
        ],
        work_queue_name="mmsr-queue"
    )

