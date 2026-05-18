import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


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

    q_update = text(
        """
        WITH src AS (
          SELECT
            site_id::text AS site_id,
            inverter_no::integer AS inverter_no,
            string_no::integer AS string_no,
            NULLIF(TRIM(inverter_id::text), '') AS inverter_id_true
          FROM staging.seed_inverter_string_layout
          WHERE COALESCE(is_active, TRUE)=TRUE
            AND NULLIF(TRIM(inverter_id::text), '') IS NOT NULL
            AND inverter_no IS NOT NULL
            AND string_no IS NOT NULL
        )
        UPDATE dimensions.dim_inverter_string_layout d
        SET
          inverter_id = s.inverter_id_true,
          inverter_mapping_status = 'seed_string_layout_fix'
        FROM src s
        WHERE d.site_id = s.site_id
          AND d.inverter_no = s.inverter_no
          AND d.string_no = s.string_no
          AND COALESCE(d.is_active, TRUE)=TRUE
          AND (d.inverter_id IS NULL OR d.inverter_id ~ '^[0-9]+$')
        """
    )

    with engine.begin() as conn:
        updated = conn.execute(q_update).rowcount

    print({"updated_rows": updated})


if __name__ == "__main__":
    main()
