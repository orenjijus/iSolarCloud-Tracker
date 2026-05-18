"""
dbt subprocess executor with real-time log streaming.
"""
from __future__ import annotations

import re
import shlex
import subprocess
from pathlib import Path
from typing import Generator


def get_dbt_executable() -> str:
    """Path to the dbt CLI executable (for subprocess)."""
    return _dbt_executable()


def _dbt_executable() -> str:
    workspace = Path(__file__).resolve().parent.parent.parent
    candidates = [
        workspace / ".venv" / "Scripts" / "dbt.exe",
        workspace / ".venv" / "bin" / "dbt",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return "dbt"


def _build_args(command: str) -> list[str]:
    exe = get_dbt_executable()
    try:
        parts = shlex.split(command, posix=True)
    except ValueError:
        parts = command.split()
    if not parts:
        return [exe]
    if parts[0].lower() in ("dbt", "dbt.exe"):
        parts[0] = exe
    return parts


def stream_dbt(command: str, cwd: Path) -> Generator[str, None, int]:
    args = _build_args(command)
    process = subprocess.Popen(
        args,
        shell=False,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    for line in process.stdout:
        yield line.rstrip("\n")
    process.wait()
    return process.returncode


def parse_dbt_result(log: str) -> dict:
    status = "unknown"
    pass_count = error_count = warn_count = 0
    duration_s = None
    if "Completed successfully" in log:
        status = "success"
    elif "Completed with" in log and "error" in log.lower():
        status = "error"
    elif "ERROR" in log:
        status = "error"
    summary_match = re.search(r"PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)", log)
    if summary_match:
        pass_count = int(summary_match.group(1))
        warn_count = int(summary_match.group(2))
        error_count = int(summary_match.group(3))
        if error_count > 0:
            status = "error"
        elif status == "unknown":
            status = "success"
    duration_match = re.search(
        r"in (\d+) hours? (\d+) minutes? and ([\d.]+) seconds?|in ([\d.]+)s",
        log,
    )
    if duration_match:
        if duration_match.group(4):
            duration_s = float(duration_match.group(4))
        elif duration_match.group(1) is not None:
            h = int(duration_match.group(1))
            m = int(duration_match.group(2))
            s = float(duration_match.group(3))
            duration_s = h * 3600 + m * 60 + s
    return {
        "status": status,
        "pass_count": pass_count,
        "warn_count": warn_count,
        "error_count": error_count,
        "duration_s": duration_s,
    }


def build_reingest_command(
    platform: str,
    start_date: str,
    end_date: str,
    site_ids_csv: str,
) -> str:
    plat = platform.lower().strip()
    if plat == "fusionsolar":
        select = "stg_fusionsolar__perf_unpivoted+"
        vars_str = (
            f"{{reingest_start_date: '{start_date}', "
            f"reingest_end_date: '{end_date}', "
            f"reingest_plant_codes: '{site_ids_csv}'}}"
        )
    else:
        select = "stg_isolarcloud__perf_unpivoted+"
        vars_str = (
            f"{{reingest_start_date: '{start_date}', "
            f"reingest_end_date: '{end_date}', "
            f"reingest_ps_ids: '{site_ids_csv}'}}"
        )
    return f'dbt run --select {select} --vars "{vars_str}"'
