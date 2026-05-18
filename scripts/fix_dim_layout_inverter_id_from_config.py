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

    q_before = text(
        """
        SELECT
          COUNT(*) AS total_rows,
          COUNT(*) FILTER (WHERE inverter_id ~ '^[0-9]+$') AS numeric_inverter_id_rows,
          COUNT(*) FILTER (WHERE inverter_id !~ '^[0-9]+$') AS non_numeric_inverter_id_rows
        FROM dimensions.dim_inverter_string_layout
        WHERE COALESCE(is_active, TRUE)=TRUE
        """
    )

    q_update = text(
        """
        WITH map_cfg AS (
          SELECT
            site_id::text AS site_id,
            inverter_no::integer AS inverter_no,
            NULLIF(TRIM(inverter_id::text), '') AS inverter_id_true,
            NULLIF(TRIM(inverter_name::text), '') AS inverter_name_true
          FROM staging.seed_inverter_config
          WHERE inverter_no IS NOT NULL
            AND NULLIF(TRIM(inverter_id::text), '') IS NOT NULL
        )
        UPDATE dimensions.dim_inverter_string_layout d
        SET
          inverter_id = m.inverter_id_true,
          inverter_name = COALESCE(m.inverter_name_true, d.inverter_name),
          inverter_mapping_status = 'seed_inverter_config_fix'
        FROM map_cfg m
        WHERE d.site_id = m.site_id
          AND d.inverter_no = m.inverter_no
          AND COALESCE(d.is_active, TRUE)=TRUE
          AND (d.inverter_id IS NULL OR d.inverter_id ~ '^[0-9]+$')
        """
    )

    q_after = text(
        """
        SELECT
          COUNT(*) AS total_rows,
          COUNT(*) FILTER (WHERE inverter_id ~ '^[0-9]+$') AS numeric_inverter_id_rows,
          COUNT(*) FILTER (WHERE inverter_id !~ '^[0-9]+$') AS non_numeric_inverter_id_rows
        FROM dimensions.dim_inverter_string_layout
        WHERE COALESCE(is_active, TRUE)=TRUE
        """
    )

    with engine.begin() as conn:
        before = conn.execute(q_before).fetchone()._mapping
        updated = conn.execute(q_update).rowcount
        after = conn.execute(q_after).fetchone()._mapping

    print({"before": dict(before), "updated_rows": updated, "after": dict(after)})


if __name__ == "__main__":
    main()
