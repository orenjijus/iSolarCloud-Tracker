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

    sql_backup = """
    CREATE TABLE IF NOT EXISTS staging.seed_poa_sensor_config_backup_20260505 AS
    SELECT * FROM staging.seed_poa_sensor_config;
    """

    sql_insert = """
    INSERT INTO staging.seed_poa_sensor_config
    (source, site_id, orient_code, poa_sensor_id, poa_sensor_name, tilt_deg, azimuth_deg, is_reference, is_active, valid_from, valid_to, notes)
    SELECT
      'mmki_cluster_bridge',
      x.target_site_id,
      s.orient_code,
      s.poa_sensor_id,
      s.poa_sensor_name,
      s.tilt_deg,
      s.azimuth_deg,
      s.is_reference,
      TRUE,
      s.valid_from,
      s.valid_to,
      NULL
    FROM staging.seed_poa_sensor_config s
    CROSS JOIN (VALUES ('NE=51758766'), ('NE=58630782')) AS x(target_site_id)
    WHERE s.site_id = 'NE=50488260'
      AND COALESCE(s.is_active, TRUE) = TRUE
      AND NOT EXISTS (
        SELECT 1
        FROM staging.seed_poa_sensor_config t
        WHERE t.site_id = x.target_site_id
          AND COALESCE(t.orient_code, -999) = COALESCE(s.orient_code, -999)
          AND t.poa_sensor_id = s.poa_sensor_id
      );
    """

    sql_check = """
    SELECT site_id, COUNT(*) AS active_rows
    FROM staging.seed_poa_sensor_config
    WHERE site_id IN ('NE=50488260','NE=51758766','NE=58630782')
      AND COALESCE(is_active, TRUE) = TRUE
    GROUP BY site_id
    ORDER BY site_id;
    """

    with engine.begin() as conn:
        conn.execute(text(sql_backup))
        result = conn.execute(text(sql_insert))
        rows = conn.execute(text(sql_check)).fetchall()

    print("inserted_rows:", result.rowcount)
    for row in rows:
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
