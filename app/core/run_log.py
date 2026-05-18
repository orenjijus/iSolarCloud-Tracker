"""Persist dbt run history to JSON."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

_DEFAULT_LOG_FILE = Path(__file__).resolve().parent.parent / "data" / "run_history.json"


def _load(log_file: Path) -> list[dict]:
    if not log_file.exists():
        return []
    try:
        return json.loads(log_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save(records: list[dict], log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text(
        json.dumps(records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def append_run(
    command: str,
    status: str,
    log: str,
    duration_s: Optional[float] = None,
    pass_count: int = 0,
    warn_count: int = 0,
    error_count: int = 0,
    source_page: str = "",
    log_file: Path = _DEFAULT_LOG_FILE,
) -> dict:
    record = {
        "id": datetime.now().strftime("%Y%m%d_%H%M%S_%f"),
        "timestamp": datetime.now().isoformat(),
        "command": command,
        "status": status,
        "duration_s": duration_s,
        "pass_count": pass_count,
        "warn_count": warn_count,
        "error_count": error_count,
        "source_page": source_page,
        "log": log,
    }
    records = _load(log_file)
    records.insert(0, record)
    _save(records, log_file)
    return record


def get_history(limit: int = 100, log_file: Path = _DEFAULT_LOG_FILE) -> list[dict]:
    return _load(log_file)[:limit]


def get_run(run_id: str, log_file: Path = _DEFAULT_LOG_FILE) -> Optional[dict]:
    for r in _load(log_file):
        if r.get("id") == run_id:
            return r
    return None
