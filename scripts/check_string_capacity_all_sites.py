import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    load_dotenv(root / ".env")
    load_dotenv(root / "tools" / ".env")

    host = os.getenv("POSTGRES_HOST", "10.101.4.88")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "MMSR")
    user = os.getenv("POSTGRES_USER", "juice")
    pwd = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", ""))

    engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

    sql_summary = """
    SELECT
        site_id,
        MAX(site_name) AS site_name,
        COUNT(*) AS total_rows,
        COUNT(string_dc_capacity_kw_stc) AS capacity_filled_rows,
        ROUND(100.0 * COUNT(string_dc_capacity_kw_stc)::numeric / NULLIF(COUNT(*), 0), 2) AS capacity_fill_pct,
        COUNT(*) FILTER (WHERE string_dc_capacity_kw_stc IS NULL) AS capacity_null_rows,
        MIN(date_key) AS min_date,
        MAX(date_key) AS max_date
    FROM mart.mart_string_performance_daily
    GROUP BY site_id
    ORDER BY capacity_fill_pct ASC, site_id;
    """

    sql_global = """
    SELECT
        COUNT(*) AS total_rows,
        COUNT(string_dc_capacity_kw_stc) AS capacity_filled_rows,
        ROUND(100.0 * COUNT(string_dc_capacity_kw_stc)::numeric / NULLIF(COUNT(*), 0), 2) AS capacity_fill_pct
    FROM mart.mart_string_performance_daily;
    """

    sql_missing_sites = """
    SELECT
        site_id,
        MAX(site_name) AS site_name,
        COUNT(*) AS total_rows
    FROM mart.mart_string_performance_daily
    WHERE string_dc_capacity_kw_stc IS NULL
    GROUP BY site_id
    ORDER BY total_rows DESC, site_id;
    """

    with engine.connect() as conn:
        global_row = conn.execute(text(sql_global)).fetchone()
        summary_rows = conn.execute(text(sql_summary)).fetchall()
        missing_rows = conn.execute(text(sql_missing_sites)).fetchall()

    print("=== GLOBAL COVERAGE ===")
    print(dict(global_row._mapping))
    print("\n=== COVERAGE PER SITE ===")
    for row in summary_rows:
        print(dict(row._mapping))
    print("\n=== SITES WITH NULL CAPACITY ===")
    for row in missing_rows:
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
