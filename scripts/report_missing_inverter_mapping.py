"""
Report inverter mapping gaps for string capacity joins:
- dim_inverter_string_layout rows where inverter_id is missing or numeric-only
- seed_inverter_config coverage for site_id + inverter_no
- mart_string_performance_daily inverter keys (45d) not in dim as non-numeric inverter_id
"""

import csv
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    out_csv = root / "docs" / "audit-tables" / "22_missing_inverter_mapping.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

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

    sql = text(
        """
        WITH cfg AS (
          SELECT
            site_id::text AS site_id,
            inverter_no::integer AS inverter_no,
            NULLIF(TRIM(inverter_id::text), '') AS cfg_inverter_id,
            NULLIF(TRIM(inverter_sn::text), '') AS cfg_inverter_sn
          FROM staging.seed_inverter_config
          WHERE inverter_no IS NOT NULL
        ),
        dim_inv AS (
          SELECT DISTINCT
            site_id::text AS site_id,
            inverter_no::integer AS inverter_no,
            MAX(inverter_id::text) AS dim_inverter_id,
            MAX(site_name::text) AS site_name,
            COUNT(*) AS layout_string_rows
          FROM dimensions.dim_inverter_string_layout
          WHERE COALESCE(is_active, TRUE) = TRUE
            AND inverter_no IS NOT NULL
          GROUP BY 1, 2
        ),
        dim_flagged AS (
          SELECT
            d.*,
            CASE
              WHEN d.dim_inverter_id IS NULL THEN 'dim_inverter_id_null'
              WHEN d.dim_inverter_id ~ '^[0-9]+$' THEN 'dim_inverter_id_numeric_only'
              ELSE 'dim_inverter_id_ok'
            END AS dim_id_status,
            c.cfg_inverter_id,
            c.cfg_inverter_sn,
            CASE
              WHEN c.cfg_inverter_id IS NULL THEN 'no_seed_inverter_config_row'
              WHEN c.cfg_inverter_id ~ '^[0-9]+$' THEN 'seed_inverter_id_still_numeric'
              ELSE 'seed_inverter_id_ok'
            END AS cfg_status
          FROM dim_inv d
          LEFT JOIN cfg c
            ON c.site_id = d.site_id
           AND c.inverter_no = d.inverter_no
        ),
        mart_inv AS (
          SELECT DISTINCT
            site_id::text AS site_id,
            REGEXP_REPLACE(inverter_id::text, '^(FS_|ISO_)', '') AS mart_inverter_key
          FROM mart.mart_string_performance_daily
          WHERE date_key >= CURRENT_DATE - INTERVAL '45 day'
        ),
        dim_ids AS (
          SELECT DISTINCT site_id::text AS site_id, inverter_id::text AS inverter_id
          FROM dimensions.dim_inverter_string_layout
          WHERE COALESCE(is_active, TRUE) = TRUE
            AND NULLIF(TRIM(inverter_id::text), '') IS NOT NULL
            AND inverter_id::text !~ '^[0-9]+$'
        ),
        mart_missing AS (
          SELECT
            m.site_id,
            m.mart_inverter_key,
            CASE
              WHEN EXISTS (
                SELECT 1 FROM dim_ids di
                WHERE di.site_id = m.site_id AND di.inverter_id = m.mart_inverter_key
              ) THEN FALSE
              ELSE TRUE
            END AS missing_in_dim_non_numeric_ids
          FROM mart_inv m
        )
        SELECT
          f.site_id,
          f.site_name,
          f.inverter_no,
          f.layout_string_rows,
          f.dim_inverter_id,
          f.dim_id_status,
          f.cfg_inverter_id,
          f.cfg_inverter_sn,
          f.cfg_status,
          (
            SELECT COUNT(*) FROM mart_missing mm
            WHERE mm.site_id = f.site_id AND mm.missing_in_dim_non_numeric_ids
          ) AS mart_inv_keys_missing_dim_45d
        FROM dim_flagged f
        WHERE f.dim_id_status <> 'dim_inverter_id_ok'
           OR f.cfg_status <> 'seed_inverter_id_ok'
        ORDER BY f.site_id, f.inverter_no
        """
    )

    sql_summary = text(
        """
        SELECT site_id, COUNT(*) AS bad_inverter_rows
        FROM (
          SELECT DISTINCT
            site_id::text AS site_id,
            inverter_no::integer AS inverter_no
          FROM dimensions.dim_inverter_string_layout
          WHERE COALESCE(is_active, TRUE) = TRUE
            AND inverter_no IS NOT NULL
            AND (
              inverter_id IS NULL
              OR inverter_id::text ~ '^[0-9]+$'
            )
        ) x
        GROUP BY site_id
        ORDER BY bad_inverter_rows DESC, site_id
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
        summary = conn.execute(sql_summary).fetchall()

    if rows:
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]._mapping.keys()))
            w.writeheader()
            for r in rows:
                w.writerow(dict(r._mapping))

    print(f"Wrote: {out_csv} detail_rows={len(rows)}")
    print("Summary sites with numeric/null dim inverter_id (distinct inverter_no):")
    for r in summary:
        print(dict(r._mapping))


if __name__ == "__main__":
    main()
