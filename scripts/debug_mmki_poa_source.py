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

    q = """
    WITH poa_by_site AS (
        SELECT
            psc.site_id,
            sd.date_key,
            AVG(sd.daily_irradiance_kwh_m2) AS daily_poa_site_kwh_m2
        FROM staging.seed_poa_sensor_config psc
        JOIN mart.mart_sensor_daily sd
          ON sd.asset_id = psc.poa_sensor_id
         AND sd.sensor_type = 'POA'
        WHERE COALESCE(psc.is_active, TRUE) = TRUE
          AND sd.daily_irradiance_kwh_m2 IS NOT NULL
          AND sd.date_key >= CURRENT_DATE - INTERVAL '30 day'
          AND psc.site_id IN ('NE=50488260', 'NE=51758766', 'NE=58630782')
        GROUP BY 1, 2
    )
    SELECT
        site_id,
        COUNT(*) AS poa_days
    FROM poa_by_site
    GROUP BY site_id
    ORDER BY site_id;
    """

    q_sensor = """
    SELECT
        psc.site_id,
        psc.poa_sensor_id,
        COUNT(sd.*) AS sensor_daily_rows
    FROM staging.seed_poa_sensor_config psc
    LEFT JOIN mart.mart_sensor_daily sd
      ON sd.asset_id = psc.poa_sensor_id
     AND sd.sensor_type = 'POA'
     AND sd.date_key >= CURRENT_DATE - INTERVAL '30 day'
    WHERE psc.site_id IN ('NE=50488260', 'NE=51758766', 'NE=58630782')
      AND COALESCE(psc.is_active, TRUE) = TRUE
    GROUP BY 1, 2
    ORDER BY 1, 2;
    """

    q_poa_assets = """
    SELECT
        sd.asset_id,
        COUNT(*) AS day_rows
    FROM mart.mart_sensor_daily sd
    WHERE sd.sensor_type = 'POA'
      AND sd.date_key >= CURRENT_DATE - INTERVAL '30 day'
    GROUP BY sd.asset_id
    HAVING sd.asset_id LIKE '%%46729%%'
    ORDER BY sd.asset_id;
    """

    with engine.connect() as conn:
        print("=== poa_by_site rows in last 30 days ===")
        for row in conn.execute(text(q)).fetchall():
            print(dict(row._mapping))
        print("=== sensor match counts ===")
        for row in conn.execute(text(q_sensor)).fetchall():
            print(dict(row._mapping))
        print("=== POA asset_id samples with 46729 pattern ===")
        for row in conn.execute(text(q_poa_assets)).fetchall():
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
