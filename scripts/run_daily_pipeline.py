"""
MMSR Daily Pipeline - Simple Script for Windows Task Scheduler

This script runs the daily data pipeline:
1. Get yesterday's date (or explicit --date)
2. Ingest data from FusionSolar and iSolarCloud (parallel)
3. Run dbt transformations

Usage:
    python run_daily_pipeline.py
    python run_daily_pipeline.py --date 2026-05-09 --date 2026-05-14
    python run_daily_pipeline.py --ingest-only --date 2026-05-09
    python run_daily_pipeline.py --dbt-only --date 2026-05-14

Can be scheduled with Windows Task Scheduler.
"""

import argparse
import json
import sys
import os
import subprocess
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

# Setup logging
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"daily_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Add project paths
# Use repository root (parent of scripts/) so imports like `fusionsolar.tasks`
# resolve correctly when launched from Task Scheduler or batch file.
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "fusionsolar"))
sys.path.insert(0, str(project_root / "isolarcloud"))

# Import ingestion functions
from fusionsolar.tasks import run_fusionsolar_ingest
from isolarcloud.tasks import run_isolarcloud_ingest


def get_yesterday_date() -> str:
    """
    Get yesterday's date in YYYY-MM-DD format.
    Uses local timezone (Windows system time).
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


def run_fusionsolar_ingestion(start_date: str, end_date: str) -> Dict:
    """
    Run FusionSolar ingestion.
    """
    logger.info(f"Starting FusionSolar ingestion for {start_date} to {end_date}")
    try:
        result = run_fusionsolar_ingest(start_date=start_date, end_date=end_date)
        logger.info(f"FusionSolar ingestion completed: {result}")
        return result
    except Exception as e:
        logger.error(f"FusionSolar ingestion failed: {str(e)}", exc_info=True)
        raise


def run_isolarcloud_ingestion(start_date: str, end_date: str) -> Dict:
    """
    Run iSolarCloud ingestion.
    """
    logger.info(f"Starting iSolarCloud ingestion for {start_date} to {end_date}")
    try:
        result = run_isolarcloud_ingest(start_date=start_date, end_date=end_date)
        logger.info(f"iSolarCloud ingestion completed: {result}")
        return result
    except Exception as e:
        logger.error(f"iSolarCloud ingestion failed: {str(e)}", exc_info=True)
        raise


def run_ingestions_parallel(start_date: str, end_date: str) -> Tuple[Dict, Dict]:
    """
    Run both ingestions in parallel using ThreadPoolExecutor.
    
    Returns:
        Tuple of (fusionsolar_result, isolarcloud_result)
    """
    logger.info("Starting parallel ingestion for FusionSolar and iSolarCloud")
    
    fusionsolar_result = None
    isolarcloud_result = None
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both tasks and store futures with labels
        future_fusionsolar = executor.submit(run_fusionsolar_ingestion, start_date, end_date)
        future_isolarcloud = executor.submit(run_isolarcloud_ingestion, start_date, end_date)
        
        # Wait for both to complete and collect results
        try:
            fusionsolar_result = future_fusionsolar.result()
        except Exception as e:
            logger.error(f"FusionSolar ingestion failed: {str(e)}", exc_info=True)
            fusionsolar_result = {'status': 'failed', 'error': str(e)}
        
        try:
            isolarcloud_result = future_isolarcloud.result()
        except Exception as e:
            logger.error(f"iSolarCloud ingestion failed: {str(e)}", exc_info=True)
            isolarcloud_result = {'status': 'failed', 'error': str(e)}
    
    return fusionsolar_result, isolarcloud_result


def _dbt_executable() -> str:
    candidates = [
        project_root / ".venv" / "Scripts" / "dbt.exe",
        project_root / ".venv" / "bin" / "dbt",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return "dbt"


def run_dbt(
    dbt_command: str = "run",
    process_date: Optional[str] = None,
    extra_args: Optional[List[str]] = None,
) -> bool:
    """
    Run dbt command.

    When process_date is set, passes reingest_start_date / reingest_end_date vars
    so incremental models refresh that calendar day.
    """
    dbt_project_dir = project_root / "dbt"
    cmd = [_dbt_executable(), dbt_command]
    if process_date:
        vars_json = json.dumps(
            {"reingest_start_date": process_date, "reingest_end_date": process_date}
        )
        cmd.extend(["--vars", vars_json])
    if extra_args:
        cmd.extend(extra_args)

    logger.info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(dbt_project_dir),
            env={**os.environ, "DBT_PROFILES_DIR": str(dbt_project_dir)},
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info(f"dbt {dbt_command} completed successfully")
        if result.stdout:
            for line in result.stdout.splitlines()[-30:]:
                logger.info("dbt | %s", line)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt {dbt_command} failed (exit {e.returncode})")
        if e.stderr:
            logger.error("dbt stderr (tail):\n%s", e.stderr[-8000:])
        if e.stdout:
            logger.error("dbt stdout (tail):\n%s", e.stdout[-8000:])
        return False
    except FileNotFoundError:
        logger.error("dbt executable not found at %s", _dbt_executable())
        return False


def run_pipeline_for_date(
    process_date: str,
    *,
    ingest: bool = True,
    dbt_run: bool = True,
) -> int:
    """Ingest + dbt for one calendar date (all sites, both platforms)."""
    logger.info("=" * 80)
    logger.info("Processing date: %s", process_date)
    logger.info("=" * 80)

    fusionsolar_result = isolarcloud_result = None
    if ingest:
        logger.info("Step 1: Parallel ingestion (FusionSolar + iSolarCloud)...")
        fusionsolar_result, isolarcloud_result = run_ingestions_parallel(
            start_date=process_date,
            end_date=process_date,
        )
        fs_ok = fusionsolar_result.get("status") == "success" if fusionsolar_result else False
        iso_ok = isolarcloud_result.get("status") == "success" if isolarcloud_result else False
        if not fs_ok:
            logger.error("FusionSolar ingestion failed for %s", process_date)
        if not iso_ok:
            logger.error("iSolarCloud ingestion failed for %s", process_date)
        if not fs_ok and not iso_ok:
            logger.error("Both ingestions failed for %s — skipping dbt", process_date)
            return 1
    else:
        fs_ok = iso_ok = True

    if dbt_run:
        logger.info("Step 2: dbt run (reingest window %s) ...", process_date)
        if not run_dbt("run", process_date=process_date):
            logger.error("dbt run failed for %s", process_date)
            return 1

    logger.info("Date %s completed OK | FusionSolar: %s | iSolarCloud: %s", process_date, fusionsolar_result, isolarcloud_result)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MMSR daily pipeline (ingest + dbt)")
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        metavar="YYYY-MM-DD",
        help="Process specific date(s). Repeat for multiple days.",
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Only run API ingestion, skip dbt",
    )
    parser.add_argument(
        "--dbt-only",
        action="store_true",
        help="Only run dbt, skip ingestion",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dates: List[str] = args.dates if args.dates else [get_yesterday_date()]
    ingest = not args.dbt_only
    dbt_run = not args.ingest_only

    pipeline_start_time = time.time()
    logger.info("=" * 80)
    logger.info("Starting MMSR Daily Pipeline")
    logger.info("Dates: %s | ingest=%s | dbt=%s", ", ".join(dates), ingest, dbt_run)
    logger.info("=" * 80)

    failed: List[str] = []
    try:
        for process_date in dates:
            code = run_pipeline_for_date(process_date, ingest=ingest, dbt_run=dbt_run)
            if code != 0:
                failed.append(process_date)

        elapsed = time.time() - pipeline_start_time
        if failed:
            logger.error("=" * 80)
            logger.error("Pipeline finished with failures: %s", ", ".join(failed))
            logger.error("Elapsed: %.1f s", elapsed)
            logger.error("=" * 80)
            return 1

        logger.info("=" * 80)
        logger.info("MMSR Daily Pipeline completed successfully for all dates!")
        logger.info("Dates: %s", ", ".join(dates))
        logger.info("Total execution time: %.1f seconds", elapsed)
        logger.info("=" * 80)
        return 0
    except Exception as e:
        logger.error("=" * 80)
        logger.error("MMSR Daily Pipeline failed: %s", e, exc_info=True)
        logger.error("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())

