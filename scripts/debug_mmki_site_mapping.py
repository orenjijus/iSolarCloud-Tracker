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

    q1 = """
    SELECT DISTINCT site_id, site_name
    FROM mart.mart_string_performance_daily
    WHERE site_name ILIKE '%MMKI%'
    ORDER BY site_name, site_id;
    """

    q2 = """
    SELECT asset_id, site_name, asset_level
    FROM dimensions.dim_assets
    WHERE site_name ILIKE '%MMKI%'
      AND asset_level = 'Site'
    ORDER BY site_name, asset_id;
    """

    q3 = """
    SELECT DISTINCT site_id
    FROM staging.seed_poa_sensor_config
    WHERE site_id LIKE '%MMKI%'
       OR site_id IN ('NE=50488260','NE=51758766','NE=58630782')
    ORDER BY site_id;
    """

    with engine.connect() as conn:
        print("=== mart_string_performance_daily MMKI sites ===")
        for row in conn.execute(text(q1)).fetchall():
            print(dict(row._mapping))
        print("=== dim_assets MMKI site keys ===")
        for row in conn.execute(text(q2)).fetchall():
            print(dict(row._mapping))
        print("=== seed_poa_sensor_config relevant site_id ===")
        for row in conn.execute(text(q3)).fetchall():
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
