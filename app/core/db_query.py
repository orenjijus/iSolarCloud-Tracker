"""Optional PostgreSQL queries for Streamlit (POSTGRES_* / .env)."""
from __future__ import annotations

import os
import urllib.parse
from pathlib import Path

import pandas as pd


def _load_env(workspace_root: Path) -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(workspace_root / ".env")
    load_dotenv(workspace_root / "tools" / ".env")


def postgres_available(workspace_root: Path) -> bool:
    _load_env(workspace_root)
    return bool(
        os.getenv("POSTGRES_HOST") or os.getenv("PGHOST")
    ) and bool(os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD"))


def run_sql(workspace_root: Path, sql: str, timeout_s: int = 90) -> pd.DataFrame:
    try:
        from sqlalchemy import create_engine, text
    except ImportError as exc:
        raise RuntimeError(
            "Butuh `sqlalchemy` untuk query DB. Install: pip install sqlalchemy psycopg2-binary"
        ) from exc

    _load_env(workspace_root)
    user = os.getenv("POSTGRES_USER") or os.getenv("PGUSER", "juice")
    pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD", "")
    host = os.getenv("POSTGRES_HOST") or os.getenv("PGHOST", "10.101.4.88")
    port = os.getenv("POSTGRES_PORT") or os.getenv("PGPORT", "5432")
    db = os.getenv("POSTGRES_DB") or os.getenv("PGDATABASE", "MMSR")

    url = "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
        user=user,
        pwd=urllib.parse.quote_plus(pwd),
        host=host,
        port=port,
        db=db,
    )
    engine = create_engine(url, connect_args={"connect_timeout": timeout_s})
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)
