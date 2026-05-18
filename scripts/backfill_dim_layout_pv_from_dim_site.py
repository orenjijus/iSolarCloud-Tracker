"""
Fill dimensions.dim_inverter_string_layout PV fields from dimensions.dim_site when
site-level module specs exist but per-string layout rows lack pv_module_p_nom_wp_master.

Optional match to staging.seed_pv_module_model_master on (manufacturer, model, year);
when matched, uses master p_nom (should match site; if not, master wins for consistency).

Does not clear existing non-null module Wp on layout rows.
"""

from __future__ import annotations

import argparse
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--site-id",
        action="append",
        dest="site_ids",
        help="limit to site_id (repeatable). Default: all sites where dim_site has Wp and layout missing Wp",
    )
    p.add_argument("--dry-run", action="store_true", help="print counts only, no UPDATE")
    args = p.parse_args()

    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / "tools" / ".env")
    engine = create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "juice"),
            pwd=urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "")),
            host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "MMSR"),
        )
    )

    site_filter = ""
    params: dict = {}
    if args.site_ids:
        site_filter = "AND d.site_id::text = ANY(:sids)"
        params["sids"] = args.site_ids

    preview = text(
        f"""
        SELECT d.site_id::text,
               COUNT(*) AS layout_rows_to_fill,
               MAX(s.site_pv_module_model) AS site_module,
               MAX(s.site_pv_module_p_nom_wp_master) AS site_wp
        FROM dimensions.dim_inverter_string_layout d
        INNER JOIN dimensions.dim_site s
          ON s.site_id::text = d.site_id::text
        LEFT JOIN staging.seed_pv_module_model_master m
          ON lower(btrim(m.manufacturer::text)) = lower(btrim(s.site_pv_module_manufacturer::text))
         AND lower(btrim(m.model::text)) = lower(btrim(s.site_pv_module_model::text))
         AND m.year IS NOT DISTINCT FROM s.site_pv_module_model_year
        WHERE COALESCE(d.is_active, TRUE)
          AND s.site_pv_module_p_nom_wp_master IS NOT NULL
          AND s.site_pv_module_p_nom_wp_master > 0
          AND (d.pv_module_p_nom_wp_master IS NULL OR d.pv_module_p_nom_wp_master <= 0)
          {site_filter}
        GROUP BY d.site_id
        ORDER BY d.site_id
        """
    )

    update_sql = text(
        f"""
        WITH src AS (
            SELECT DISTINCT ON (d.ctid)
                d.ctid AS layout_ctid,
                COALESCE(m.p_nom, s.site_pv_module_p_nom_wp_master) AS wp,
                COALESCE(NULLIF(btrim(d.pv_module_model::text), ''), s.site_pv_module_model::text) AS pvmod,
                COALESCE(
                    NULLIF(btrim(d.pv_module_manufacturer::text), ''),
                    s.site_pv_module_manufacturer::text
                ) AS pvman,
                COALESCE(d.pv_module_model_year, s.site_pv_module_model_year) AS pvyear
            FROM dimensions.dim_inverter_string_layout d
            INNER JOIN dimensions.dim_site s
              ON s.site_id::text = d.site_id::text
            LEFT JOIN staging.seed_pv_module_model_master m
              ON lower(btrim(m.manufacturer::text)) = lower(btrim(s.site_pv_module_manufacturer::text))
             AND lower(btrim(m.model::text)) = lower(btrim(s.site_pv_module_model::text))
             AND m.year IS NOT DISTINCT FROM s.site_pv_module_model_year
            WHERE COALESCE(d.is_active, TRUE)
              AND s.site_pv_module_p_nom_wp_master IS NOT NULL
              AND s.site_pv_module_p_nom_wp_master > 0
              AND (d.pv_module_p_nom_wp_master IS NULL OR d.pv_module_p_nom_wp_master <= 0)
              {site_filter}
            ORDER BY d.ctid, m.p_nom DESC NULLS LAST
        )
        UPDATE dimensions.dim_inverter_string_layout d
        SET
            pv_module_p_nom_wp_master = src.wp,
            pv_module_model = src.pvmod,
            pv_module_manufacturer = src.pvman,
            pv_module_model_year = src.pvyear
        FROM src
        WHERE d.ctid = src.layout_ctid
        """
    )

    with engine.connect() as conn:
        prev = conn.execute(preview, params).fetchall()
        print("sites / rows needing fill:", len(prev))
        for r in prev:
            print(dict(r._mapping))

    if args.dry_run:
        print("dry-run: no UPDATE executed")
        return

    with engine.begin() as conn:
        r = conn.execute(update_sql, params)
        print("rows_updated:", r.rowcount)


if __name__ == "__main__":
    main()
