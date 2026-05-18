"""
Hidden Valley hourly pilot: FusionSolar ingest (2 plants) + dbt intraday marts.

Scheduled via Windows Task Scheduler as "MMSR HV Hourly Pilot".
Skips 01:00–04:59 WIB (maintenance window, jam 1–4). Exit code 3 = skipped (success for batch).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

WIB = timezone(timedelta(hours=7))
HV_PLANT_CODES = "NE=60951882,NE=78317340"
HV_DEVICE_TYPES = "inverter,meter,battery,smart_assistant"

project_root = Path(__file__).resolve().parent.parent
log_dir = project_root / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"hv_pilot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def _now_wib() -> datetime:
    return datetime.now(WIB)


def _in_skip_window(now: datetime) -> bool:
    # Skip runs at 01:xx–04:xx WIB (jam 1–4); resume at 05:00.
    return 1 <= now.hour <= 4


def _today_wib() -> str:
    return _now_wib().strftime("%Y-%m-%d")


def _dbt_executable() -> str:
    for candidate in (
        project_root / ".venv" / "Scripts" / "dbt.exe",
        project_root / ".venv" / "bin" / "dbt",
    ):
        if candidate.exists():
            return str(candidate)
    return "dbt"


def _python_executable() -> str:
    for candidate in (
        project_root / ".venv" / "Scripts" / "python.exe",
        project_root / ".venv" / "bin" / "python",
    ):
        if candidate.exists():
            return str(candidate)
    return sys.executable


def run_harvest(today: str) -> int:
    harvester = project_root / "fusionsolar" / "fusionsolar_data_harvester.py"
    cmd = [
        _python_executable(),
        str(harvester),
        "--fetch-historical",
        today,
        today,
        "--plant-codes",
        HV_PLANT_CODES,
        "--device-types",
        HV_DEVICE_TYPES,
    ]
    logger.info("Harvest: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=str(project_root / "fusionsolar"),
        capture_output=True,
        text=True,
    )
    if result.stdout:
        for line in result.stdout.splitlines()[-40:]:
            logger.info("harvest | %s", line)
    if result.returncode != 0:
        logger.error("Harvest failed (exit %s)", result.returncode)
        if result.stderr:
            logger.error("harvest stderr:\n%s", result.stderr[-8000:])
        return result.returncode
    return 0


def run_dbt_intraday(today: str) -> int:
    dbt_dir = project_root / "dbt"
    vars_file = dbt_dir / "vars" / "hv_pilot_today.yml"
    vars_file.parent.mkdir(parents=True, exist_ok=True)
    vars_file.write_text(
        f'reingest_start_date: "{today}"\n'
        f'reingest_end_date: "{today}"\n'
        f'reingest_plant_codes: "{HV_PLANT_CODES}"\n',
        encoding="utf-8",
    )
    cmd = [
        _dbt_executable(),
        "run",
        "--select",
        "stg_fusionsolar__perf_unpivoted",
        "stg_fusionsolar__perf_battery_unpivoted",
        "tag:intraday",
        "mart_meter_daily_hidden_valley",
        "--vars",
        vars_file.read_text(encoding="utf-8"),
    ]
    logger.info("dbt: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=str(dbt_dir),
        env={**os.environ, "DBT_PROFILES_DIR": str(dbt_dir)},
        capture_output=True,
        text=True,
    )
    if result.stdout:
        for line in result.stdout.splitlines()[-50:]:
            logger.info("dbt | %s", line)
    if result.returncode != 0:
        logger.error("dbt run failed (exit %s)", result.returncode)
        if result.stderr:
            logger.error("dbt stderr:\n%s", result.stderr[-8000:])
        return result.returncode
    return 0


def main() -> int:
    now = _now_wib()
    logger.info("HV pilot start (WIB %s)", now.isoformat())
    if _in_skip_window(now):
        logger.info("Skip window 01:00–04:59 WIB (jam 1–4) — exiting with code 3")
        return 3

    today = _today_wib()
    logger.info("Processing date (WIB): %s | plants: %s", today, HV_PLANT_CODES)

    if run_harvest(today) != 0:
        return 1
    if run_dbt_intraday(today) != 0:
        return 1

    logger.info("HV pilot completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
