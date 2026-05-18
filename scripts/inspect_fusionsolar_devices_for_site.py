"""Sample FusionSolar device rows for a plant to map dev_id vs plant ordering."""

import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

SITES = [
    ("NE=54435794", "Pusan"),
    ("NE=53771627", "Mall"),
    ("NE=50488260", "MMKI1"),
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

    q_cols = text(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = 'stg_fusionsolar__devices'
        ORDER BY ordinal_position
        """
    )

    q_dev = text(
        """
        SELECT dev_id, dev_name, plant_code, dev_type_id, device_category
        FROM staging.stg_fusionsolar__devices
        WHERE plant_code = :pc
          AND device_category = 'Inverter'
        ORDER BY dev_id
        """
    )

    with engine.connect() as conn:
        print("stg_fusionsolar__devices columns:")
        for r in conn.execute(q_cols).fetchall():
            print(f"  {r[0]} {r[1]}")

    for plant_code, label in SITES:
        with engine.connect() as conn:
            rows = conn.execute(q_dev, {"pc": plant_code}).fetchall()
        print(f"\n=== {label} plant_code={plant_code} inverters={len(rows)} ===")
        for r in rows[:25]:
            print(dict(r._mapping))
        if len(rows) > 25:
            print(f"  ... +{len(rows)-25} more")


if __name__ == "__main__":
    main()
