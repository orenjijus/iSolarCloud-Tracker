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

    sql = """
    SELECT
        site_id,
        COUNT(*) AS total_rows,
        COUNT(daily_poa_kwh_m2) AS poa_filled_rows,
        ROUND(100.0 * COUNT(daily_poa_kwh_m2)::numeric / NULLIF(COUNT(*), 0), 2) AS poa_fill_pct,
        COUNT(pr_poa_string) AS pr_poa_filled_rows,
        ROUND(100.0 * COUNT(pr_poa_string)::numeric / NULLIF(COUNT(*), 0), 2) AS pr_poa_fill_pct
    FROM mart.mart_string_performance_daily
    WHERE site_id IN ('NE=51758766', 'NE=58630782')
      AND date_key >= CURRENT_DATE - INTERVAL '30 day'
    GROUP BY site_id
    ORDER BY site_id;
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    for row in rows:
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
