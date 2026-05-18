import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


SITES = ["NE=54435794", "NE=53771627", "1445767", "NE=50488260"]


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    load_dotenv(root / ".env")
    load_dotenv(root / "tools" / ".env")
    engine = create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "juice"),
            pwd=urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "")),
            host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "MMSR"),
        )
    )
    q = text(
        """
        WITH m AS (
          SELECT DISTINCT
            site_id,
            REGEXP_REPLACE(inverter_id, '^(FS_|ISO_)', '') AS inverter_key,
            string_number
          FROM mart.mart_string_performance_daily
          WHERE date_key >= CURRENT_DATE - INTERVAL '45 day'
            AND site_id = :sid
        ),
        d AS (
          SELECT DISTINCT
            site_id,
            inverter_id,
            string_no AS string_number
          FROM dimensions.dim_inverter_string_layout
          WHERE COALESCE(is_active, TRUE)=TRUE
            AND site_id = :sid
        )
        SELECT
          :sid AS site_id,
          (SELECT COUNT(*) FROM m) AS mart_keys,
          (SELECT COUNT(*) FROM d) AS dim_keys,
          (SELECT COUNT(*) FROM m JOIN d ON d.inverter_id = m.inverter_key AND d.string_number = m.string_number) AS matched_keys;
        """
    )
    q_mart_sample = text(
        """
        SELECT DISTINCT REGEXP_REPLACE(inverter_id, '^(FS_|ISO_)', '') AS inverter_key
        FROM mart.mart_string_performance_daily
        WHERE date_key >= CURRENT_DATE - INTERVAL '45 day'
          AND site_id = :sid
        ORDER BY 1
        LIMIT 10
        """
    )
    q_dim_sample = text(
        """
        SELECT DISTINCT inverter_id
        FROM dimensions.dim_inverter_string_layout
        WHERE COALESCE(is_active, TRUE)=TRUE
          AND site_id = :sid
        ORDER BY 1
        LIMIT 10
        """
    )
    with engine.connect() as conn:
        for sid in SITES:
            print(dict(conn.execute(q, {"sid": sid}).fetchone()._mapping))
            print(" mart sample:", [r[0] for r in conn.execute(q_mart_sample, {"sid": sid}).fetchall()])
            print(" dim sample :", [r[0] for r in conn.execute(q_dim_sample, {"sid": sid}).fetchall()])


if __name__ == "__main__":
    main()
