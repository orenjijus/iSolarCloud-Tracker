"""Count distinct inverters in dimensions.dim_inverter_string_layout (global + sample sites)."""

import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

SAMPLE_SITES = [
    "NE=54435794",
    "NE=53771627",
    "NE=50488260",
    "NE=51758766",
    "NE=58630782",
    "NE=62806354",
    "1445767",
    "1479456",
    "1614122",
]


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

    q_global = text(
        """
        SELECT
            COUNT(DISTINCT site_id) AS site_count,
            COUNT(DISTINCT (site_id, inverter_no)) FILTER (
                WHERE COALESCE(is_active, TRUE) = TRUE AND inverter_no IS NOT NULL
            ) AS distinct_site_inverter_no,
            COUNT(DISTINCT NULLIF(TRIM(inverter_id::text), '')) FILTER (
                WHERE COALESCE(is_active, TRUE) = TRUE
            ) AS distinct_inverter_id_values
        FROM dimensions.dim_inverter_string_layout
        """
    )

    q_per_site = text(
        """
        SELECT
            site_id,
            MAX(site_name) AS site_name,
            COUNT(DISTINCT inverter_no) FILTER (
                WHERE COALESCE(is_active, TRUE) = TRUE AND inverter_no IS NOT NULL
            ) AS inverter_count_by_no,
            COUNT(DISTINCT NULLIF(TRIM(inverter_id::text), '')) FILTER (
                WHERE COALESCE(is_active, TRUE) = TRUE
            ) AS distinct_inverter_id,
            COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE) = TRUE) AS layout_rows
        FROM dimensions.dim_inverter_string_layout
        WHERE site_id = ANY(:sids)
        GROUP BY site_id
        ORDER BY site_id
        """
    )

    with engine.connect() as conn:
        g = dict(conn.execute(q_global).fetchone()._mapping)
        print("GLOBAL (dim_inverter_string_layout, active rows):")
        print(g)
        print("\nPER SITE (sample):")
        rows = conn.execute(q_per_site, {"sids": SAMPLE_SITES}).fetchall()
        for r in rows:
            print(dict(r._mapping))


if __name__ == "__main__":
    main()
