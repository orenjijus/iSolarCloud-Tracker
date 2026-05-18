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
    WITH site_base AS (
      SELECT DISTINCT site_id, site_name
      FROM mart.mart_string_performance_daily
    ),
    site_canonical AS (
      SELECT
        b.site_id AS source_site_id,
        b.site_name,
        COALESCE(da.asset_id, b.site_id) AS canonical_site_id
      FROM site_base b
      LEFT JOIN dimensions.dim_assets da
        ON da.asset_level = 'Site'
       AND UPPER(TRIM(da.site_name)) = UPPER(TRIM(b.site_name))
    ),
    layout_cnt AS (
      SELECT site_id, COUNT(*) AS layout_rows
      FROM dimensions.dim_inverter_string_layout
      WHERE COALESCE(is_active, TRUE) = TRUE
      GROUP BY site_id
    )
    SELECT
      s.source_site_id AS site_id,
      s.site_name,
      s.canonical_site_id,
      COALESCE(ls.layout_rows, 0) AS layout_rows_source_key,
      COALESCE(lc.layout_rows, 0) AS layout_rows_canonical_key
    FROM site_canonical s
    LEFT JOIN layout_cnt ls
      ON ls.site_id = s.source_site_id
    LEFT JOIN layout_cnt lc
      ON lc.site_id = s.canonical_site_id
    ORDER BY site_id;
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    for row in rows:
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
